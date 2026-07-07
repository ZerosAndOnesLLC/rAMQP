//! Message-body paging for deep quorum queues (broker.md §8's #1 risk).
//!
//! A deep backlog must not live in RAM (§3.1: bounded, configurable RSS).
//! Once a queue's resident bodies exceed its cap, `apply(enqueue)` spills
//! the body to an append-only **segment file** and the replicated state
//! keeps only a [`SpillRef`] — the index stays resident, the bytes do not.
//! Dispatch reads spilled bodies back with a positioned read; settled
//! messages decrement their segment's live count, and a segment whose live
//! count reaches zero is deleted (space is reclaimed segment-wise, matching
//! FIFO consumption).
//!
//! Concurrency: every replica manages its *own* spill directory (bodies are
//! replicated through the Raft log, not through these files). Writes happen
//! inside the state-machine apply path (that is the point: flow-to-disk
//! *is* the memory relief); reads take a shared handle. Snapshot builders
//! [`pin`](Spill::pin) the current segments so a concurrent settle cannot
//! delete a segment mid-read; deletions are deferred until unpin.

use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// Roll to a new segment file once the current one exceeds this.
const SEGMENT_MAX_BYTES: u64 = 64 * 1024 * 1024;

/// Where one spilled body lives.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SpillRef {
    /// The segment file id.
    pub segment: u64,
    /// Byte offset of the body within the segment.
    pub offset: u64,
    /// Body length.
    pub len: u32,
}

struct Segment {
    file: File,
    /// Bodies written and not yet released.
    live: usize,
    /// Bytes appended so far.
    size: u64,
}

struct Inner {
    dir: PathBuf,
    next_segment: u64,
    current: Option<u64>,
    segments: HashMap<u64, Segment>,
    /// Snapshot builders currently reading; segment deletion defers while
    /// non-zero.
    pins: usize,
    /// Segments that reached zero live entries while pinned.
    deferred_delete: Vec<u64>,
}

/// One queue's spill store (cheap to clone; internally synchronized).
#[derive(Clone)]
pub struct Spill {
    inner: Arc<Mutex<Inner>>,
}

impl std::fmt::Debug for Spill {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Spill").finish_non_exhaustive()
    }
}

impl Spill {
    /// Open a spill store under `dir`. Any leftover files from a previous
    /// process are removed — without a persisted Raft log, spilled bodies
    /// only make sense next to the in-memory state that referenced them.
    pub fn open(dir: PathBuf) -> Result<Spill, String> {
        if dir.exists() {
            std::fs::remove_dir_all(&dir).map_err(|e| e.to_string())?;
        }
        Self::open_preserving(dir)
    }

    /// Open a spill store, KEEPING existing segment files — the persisted
    /// (recovered) Raft state still references them. Recovered segments load
    /// with a zero live count; call [`set_live`](Spill::set_live) with the
    /// restored state's per-segment reference counts before any release can
    /// reach them. New appends always roll a fresh segment.
    pub fn open_preserving(dir: PathBuf) -> Result<Spill, String> {
        std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
        let mut segments = HashMap::new();
        let mut next_segment = 0u64;
        for entry in std::fs::read_dir(&dir).map_err(|e| e.to_string())? {
            let entry = entry.map_err(|e| e.to_string())?;
            let name = entry.file_name();
            let Some(id) = name
                .to_str()
                .and_then(|n| n.strip_suffix(".seg"))
                .and_then(|n| n.parse::<u64>().ok())
            else {
                continue;
            };
            let file = OpenOptions::new()
                .read(true)
                .write(true)
                .open(entry.path())
                .map_err(|e| e.to_string())?;
            let size = file.metadata().map_err(|e| e.to_string())?.len();
            segments.insert(
                id,
                Segment {
                    file,
                    live: 0,
                    size,
                },
            );
            next_segment = next_segment.max(id + 1);
        }
        Ok(Spill {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                next_segment,
                current: None,
                segments,
                pins: 0,
                deferred_delete: Vec::new(),
            })),
        })
    }

    /// Set recovered segments' live reference counts (from the restored
    /// state); segments present on disk but no longer referenced are
    /// reclaimed here.
    pub fn set_live(&self, counts: &HashMap<u64, usize>) {
        let mut inner = self.inner.lock().expect("spill lock");
        let ids: Vec<u64> = inner.segments.keys().copied().collect();
        for id in ids {
            let live = counts.get(&id).copied().unwrap_or(0);
            if let Some(segment) = inner.segments.get_mut(&id) {
                segment.live = live;
            }
            if live == 0 && inner.current != Some(id) {
                delete_segment(&mut inner, id);
            }
        }
    }

    /// Append one body, returning where it landed.
    pub fn append(&self, body: &[u8]) -> Result<SpillRef, String> {
        let mut inner = self.inner.lock().expect("spill lock");
        // Roll the segment if needed.
        let roll = match inner.current {
            Some(id) => inner.segments[&id].size >= SEGMENT_MAX_BYTES,
            None => true,
        };
        if roll {
            let id = inner.next_segment;
            inner.next_segment += 1;
            let path = inner.dir.join(format!("{id:016}.seg"));
            let file = OpenOptions::new()
                .create(true)
                .truncate(true)
                .read(true)
                .write(true)
                .open(&path)
                .map_err(|e| e.to_string())?;
            inner.segments.insert(
                id,
                Segment {
                    file,
                    live: 0,
                    size: 0,
                },
            );
            inner.current = Some(id);
        }
        let id = inner.current.expect("current segment");
        let segment = inner.segments.get_mut(&id).expect("segment");
        let offset = segment.size;
        segment
            .file
            .seek(SeekFrom::Start(offset))
            .map_err(|e| e.to_string())?;
        segment.file.write_all(body).map_err(|e| e.to_string())?;
        segment.size += body.len() as u64;
        segment.live += 1;
        Ok(SpillRef {
            segment: id,
            offset,
            len: body.len() as u32,
        })
    }

    /// Read one spilled body back.
    pub fn read(&self, r: &SpillRef) -> Result<Bytes, String> {
        let mut inner = self.inner.lock().expect("spill lock");
        let segment = inner
            .segments
            .get_mut(&r.segment)
            .ok_or("spill segment gone")?;
        let mut buf = vec![0u8; r.len as usize];
        segment
            .file
            .seek(SeekFrom::Start(r.offset))
            .map_err(|e| e.to_string())?;
        segment
            .file
            .read_exact(&mut buf)
            .map_err(|e| e.to_string())?;
        Ok(Bytes::from(buf))
    }

    /// A spilled body was settled: reclaim segment space once nothing in
    /// the segment is live.
    pub fn release(&self, r: &SpillRef) {
        let mut inner = self.inner.lock().expect("spill lock");
        let dead = match inner.segments.get_mut(&r.segment) {
            Some(segment) => {
                segment.live = segment.live.saturating_sub(1);
                segment.live == 0 && inner.current != Some(r.segment)
            }
            None => false,
        };
        if dead {
            if inner.pins > 0 {
                inner.deferred_delete.push(r.segment);
            } else {
                delete_segment(&mut inner, r.segment);
            }
        }
    }

    /// Hold segment deletions (a snapshot build is reading). Balanced by
    /// [`unpin`](Spill::unpin).
    pub fn pin(&self) {
        self.inner.lock().expect("spill lock").pins += 1;
    }

    /// Release a [`pin`](Spill::pin); deferred deletions run when the last
    /// pin drops.
    pub fn unpin(&self) {
        let mut inner = self.inner.lock().expect("spill lock");
        inner.pins = inner.pins.saturating_sub(1);
        if inner.pins == 0 {
            for id in std::mem::take(&mut inner.deferred_delete) {
                if inner.segments.get(&id).is_some_and(|s| s.live == 0) {
                    delete_segment(&mut inner, id);
                }
            }
        }
    }

    /// Diagnostics: (segment count, total on-disk bytes).
    pub fn stats(&self) -> (usize, u64) {
        let inner = self.inner.lock().expect("spill lock");
        (
            inner.segments.len(),
            inner.segments.values().map(|s| s.size).sum(),
        )
    }
}

fn delete_segment(inner: &mut Inner, id: u64) {
    inner.segments.remove(&id);
    let path = inner.dir.join(format!("{id:016}.seg"));
    if let Err(e) = std::fs::remove_file(&path) {
        tracing::warn!(path = %path.display(), error = %e, "spill segment delete failed");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn append_read_release_reclaims_segments() {
        let dir = std::env::temp_dir().join(format!("ramqp-spill-test-{}", std::process::id()));
        let spill = Spill::open(dir.clone()).expect("open");

        let a = spill.append(b"alpha").expect("append");
        let b = spill.append(b"bravo").expect("append");
        assert_eq!(&spill.read(&a).expect("read a")[..], b"alpha");
        assert_eq!(&spill.read(&b).expect("read b")[..], b"bravo");

        // Same (current) segment: releases do not delete it.
        spill.release(&a);
        spill.release(&b);
        assert_eq!(spill.stats().0, 1, "current segment is never deleted");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pinned_segments_survive_release_until_unpin() {
        let dir = std::env::temp_dir().join(format!("ramqp-spill-pin-{}", std::process::id()));
        let spill = Spill::open(dir.clone()).expect("open");
        let r = spill.append(b"pinned").expect("append");

        // Force a segment roll so `r`'s segment is no longer current.
        {
            let mut inner = spill.inner.lock().expect("lock");
            inner.current = None;
        }
        spill.append(b"next segment").expect("append");

        spill.pin();
        spill.release(&r);
        // Still readable under the pin.
        assert_eq!(&spill.read(&r).expect("read")[..], b"pinned");
        spill.unpin();
        assert!(spill.read(&r).is_err(), "deleted after the pin dropped");

        std::fs::remove_dir_all(&dir).ok();
    }
}
