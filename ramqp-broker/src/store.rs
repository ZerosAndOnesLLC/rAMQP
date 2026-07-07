//! The durable-local message store (broker.md Phase 7): a redb-backed
//! substrate for `/durable/<name>` queues.
//!
//! One database file serves every durable queue on the node. All writes ride
//! a single **group-commit writer task**: queue actors submit operations on
//! a channel, the writer drains a burst, applies it in one write
//! transaction, and fsyncs once (`Durability::Immediate`) — the batching
//! lever from broker.md §3.2 (one fsync amortizes across every publish in
//! flight, across all durable queues). A publish is confirmed to the
//! producer only after its batch commits, so the accepted disposition is a
//! real on-disk durability confirm.
//!
//! Reads (recovery scans, dispatch-time body fetches) use redb's MVCC read
//! transactions, which never block the writer.

use std::path::Path;
use std::sync::Arc;

use bytes::Bytes;
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use tokio::sync::{mpsc, oneshot};

/// Messages: `(queue id, message id)` → `(failed-delivery count, enqueue
/// time ms, body)`.
const MESSAGES: TableDefinition<(u64, u64), (u32, u64, &[u8])> = TableDefinition::new("messages");
/// Queue registry: name → queue id.
const QUEUES: TableDefinition<&str, u64> = TableDefinition::new("queues");
/// Single-row metadata: key → counter (`"next_queue_id"`).
const META: TableDefinition<&str, u64> = TableDefinition::new("meta");
/// Raft groups: group name → group id.
const RAFT_GROUPS: TableDefinition<&str, u64> = TableDefinition::new("raft_groups");
/// Raft log: `(group id, index)` → encoded entry.
const RAFT_LOG: TableDefinition<(u64, u64), &[u8]> = TableDefinition::new("raft_log");
/// Raft hard state: `(group id, key)` → encoded value (`"vote"`, `"purged"`,
/// `"snap_meta"`, and the snapshot blob as either `"snap_blob"` — the bytes
/// inline, atomic with the pointer — or `"snap_path"` — a file path for
/// large paged-queue blobs).
const RAFT_META: TableDefinition<(u64, &str), &[u8]> = TableDefinition::new("raft_meta");

/// How many operations one commit may absorb.
const COMMIT_BATCH_MAX: usize = 1024;

/// One durable mutation, applied by the writer task.
pub(crate) enum StoreOp {
    /// Store a message; `done` fires (true) once it is on disk.
    Insert {
        queue: u64,
        msg_id: u64,
        enqueued_ms: u64,
        body: Bytes,
        done: oneshot::Sender<bool>,
    },
    /// Remove a settled message (ack/drop).
    Remove { queue: u64, msg_id: u64 },
    /// Count a failed delivery attempt (requeue with penalty).
    Fail { queue: u64, msg_id: u64 },
    /// Durably append Raft log entries; `done` fires once fsynced.
    RaftAppend {
        group: u64,
        entries: Vec<(u64, Vec<u8>)>,
        done: oneshot::Sender<bool>,
    },
    /// Durably record a Raft vote.
    RaftVote {
        group: u64,
        vote: Vec<u8>,
        done: oneshot::Sender<bool>,
    },
    /// Remove Raft entries with `index >= since`.
    RaftTruncate {
        group: u64,
        since: u64,
        done: oneshot::Sender<bool>,
    },
    /// Remove Raft entries with `index <= upto`; record the purge marker.
    RaftPurge {
        group: u64,
        upto: u64,
        marker: Vec<u8>,
        done: oneshot::Sender<bool>,
    },
    /// Record the current snapshot (pointer or inline blob).
    RaftSnapshot {
        group: u64,
        meta: Vec<u8>,
        blob: crate::cluster::store::SnapshotPersist,
        done: oneshot::Sender<bool>,
    },
}

impl StoreOp {
    fn take_done(self) -> Option<oneshot::Sender<bool>> {
        match self {
            StoreOp::Insert { done, .. }
            | StoreOp::RaftAppend { done, .. }
            | StoreOp::RaftVote { done, .. }
            | StoreOp::RaftTruncate { done, .. }
            | StoreOp::RaftPurge { done, .. }
            | StoreOp::RaftSnapshot { done, .. } => Some(done),
            StoreOp::Remove { .. } | StoreOp::Fail { .. } => None,
        }
    }
}

/// A handle to the node's durable store.
#[derive(Clone)]
pub(crate) struct Store {
    db: Arc<Database>,
    writer: mpsc::Sender<StoreOp>,
}

impl std::fmt::Debug for Store {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Store").finish_non_exhaustive()
    }
}

impl Store {
    /// Open (or create) the store at `<data_dir>/ramqp-broker.redb` and
    /// start its writer task.
    pub fn open(data_dir: &Path) -> Result<Store, String> {
        std::fs::create_dir_all(data_dir).map_err(|e| e.to_string())?;
        let path = data_dir.join("ramqp-broker.redb");
        let db = Arc::new(Database::create(&path).map_err(|e| e.to_string())?);
        // Make sure the tables exist so first reads don't error.
        {
            let txn = db.begin_write().map_err(|e| e.to_string())?;
            txn.open_table(MESSAGES).map_err(|e| e.to_string())?;
            txn.open_table(QUEUES).map_err(|e| e.to_string())?;
            txn.open_table(META).map_err(|e| e.to_string())?;
            txn.open_table(RAFT_GROUPS).map_err(|e| e.to_string())?;
            txn.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
            txn.open_table(RAFT_META).map_err(|e| e.to_string())?;
            txn.commit().map_err(|e| e.to_string())?;
        }
        let (writer, rx) = mpsc::channel(4096);
        let writer_db = db.clone();
        // The writer is blocking (redb commits fsync): give it its own thread
        // via spawn_blocking-style dedicated task.
        std::thread::Builder::new()
            .name("ramqp-store-writer".into())
            .spawn(move || writer_loop(writer_db, rx))
            .map_err(|e| e.to_string())?;
        Ok(Store { db, writer })
    }

    /// Submit a mutation (awaits channel capacity — the ingest backpressure).
    pub async fn submit(&self, op: StoreOp) -> Result<(), String> {
        self.writer
            .send(op)
            .await
            .map_err(|_| "store writer stopped".to_owned())
    }

    /// The stable id for a queue name, allocating one on first use.
    pub fn queue_id(&self, name: &str) -> Result<u64, String> {
        // Fast path: already registered.
        {
            let txn = self.db.begin_read().map_err(|e| e.to_string())?;
            let table = txn.open_table(QUEUES).map_err(|e| e.to_string())?;
            if let Some(id) = table.get(name).map_err(|e| e.to_string())? {
                return Ok(id.value());
            }
        }
        // Allocate through a write transaction (serialized by redb; the
        // registry only calls this once per queue declaration).
        let txn = self.db.begin_write().map_err(|e| e.to_string())?;
        let id = {
            let mut queues = txn.open_table(QUEUES).map_err(|e| e.to_string())?;
            if let Some(id) = queues.get(name).map_err(|e| e.to_string())? {
                id.value()
            } else {
                let mut meta = txn.open_table(META).map_err(|e| e.to_string())?;
                let next = meta
                    .get("next_queue_id")
                    .map_err(|e| e.to_string())?
                    .map(|v| v.value())
                    .unwrap_or(1);
                meta.insert("next_queue_id", next + 1)
                    .map_err(|e| e.to_string())?;
                queues.insert(name, next).map_err(|e| e.to_string())?;
                next
            }
        };
        txn.commit().map_err(|e| e.to_string())?;
        Ok(id)
    }

    /// Recovery scan: every stored `(msg_id, failures, enqueued_ms)` for a
    /// queue.
    pub fn scan(&self, queue: u64) -> Result<Vec<(u64, u32, u64)>, String> {
        let txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let table = txn.open_table(MESSAGES).map_err(|e| e.to_string())?;
        let mut out = Vec::new();
        for entry in table
            .range((queue, 0)..=(queue, u64::MAX))
            .map_err(|e| e.to_string())?
        {
            let (key, value) = entry.map_err(|e| e.to_string())?;
            let (failures, enqueued_ms, _) = value.value();
            out.push((key.value().1, failures, enqueued_ms));
        }
        Ok(out)
    }

    /// Fetch one message body (dispatch path; a page-cached B-tree read).
    pub fn body(&self, queue: u64, msg_id: u64) -> Option<Bytes> {
        let txn = self.db.begin_read().ok()?;
        let table = txn.open_table(MESSAGES).ok()?;
        let value = table.get((queue, msg_id)).ok()??;
        Some(Bytes::copy_from_slice(value.value().2))
    }

    /// The stable id for a Raft group name, allocating one on first use.
    fn raft_group_id(&self, name: &str) -> Result<u64, String> {
        {
            let txn = self.db.begin_read().map_err(|e| e.to_string())?;
            let table = txn.open_table(RAFT_GROUPS).map_err(|e| e.to_string())?;
            if let Some(id) = table.get(name).map_err(|e| e.to_string())? {
                return Ok(id.value());
            }
        }
        let txn = self.db.begin_write().map_err(|e| e.to_string())?;
        let id = {
            let mut groups = txn.open_table(RAFT_GROUPS).map_err(|e| e.to_string())?;
            if let Some(id) = groups.get(name).map_err(|e| e.to_string())? {
                id.value()
            } else {
                let mut meta = txn.open_table(META).map_err(|e| e.to_string())?;
                let next = meta
                    .get("next_raft_group")
                    .map_err(|e| e.to_string())?
                    .map(|v| v.value())
                    .unwrap_or(1);
                meta.insert("next_raft_group", next + 1)
                    .map_err(|e| e.to_string())?;
                groups.insert(name, next).map_err(|e| e.to_string())?;
                next
            }
        };
        txn.commit().map_err(|e| e.to_string())?;
        Ok(id)
    }

    /// Load a group's persisted hard state.
    fn recover_raft(&self, group: u64) -> Result<crate::cluster::store::RaftLogRecovery, String> {
        let txn = self.db.begin_read().map_err(|e| e.to_string())?;
        let meta = txn.open_table(RAFT_META).map_err(|e| e.to_string())?;
        let get = |key: &str| -> Result<Option<Vec<u8>>, String> {
            Ok(meta
                .get((group, key))
                .map_err(|e| e.to_string())?
                .map(|v| v.value().to_vec()))
        };
        use crate::cluster::store::SnapshotPersist;
        let vote = get("vote")?;
        let purged = get("purged")?;
        let snap_meta = get("snap_meta")?;
        let snap_blob = get("snap_blob")?;
        let snap_path = get("snap_path")?;
        let snapshot = match (snap_meta, snap_blob, snap_path) {
            (Some(m), Some(b), _) => Some((m, SnapshotPersist::Inline(b))),
            (Some(m), None, Some(p)) => {
                let path = std::path::PathBuf::from(String::from_utf8_lossy(&p).into_owned());
                path.exists().then_some((m, SnapshotPersist::File(path)))
            }
            _ => None,
        };
        let log = txn.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let mut entries = Vec::new();
        for entry in log
            .range((group, 0)..=(group, u64::MAX))
            .map_err(|e| e.to_string())?
        {
            let (key, value) = entry.map_err(|e| e.to_string())?;
            entries.push((key.value().1, value.value().to_vec()));
        }
        Ok(crate::cluster::store::RaftLogRecovery {
            vote,
            purged,
            entries,
            snapshot,
        })
    }
}

/// One group's view of the store as a [`RaftLogSink`]: each call submits an
/// op to the group-commit writer and resolves when its batch fsyncs.
#[derive(Debug)]
struct GroupSink {
    store: Store,
    group: u64,
}

type SinkFuture<'a> = std::pin::Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

impl GroupSink {
    fn run(&self, op_for: impl FnOnce(oneshot::Sender<bool>) -> StoreOp + Send) -> SinkFuture<'_> {
        let (done_tx, done_rx) = oneshot::channel();
        let op = op_for(done_tx);
        Box::pin(async move {
            self.store.submit(op).await?;
            match done_rx.await {
                Ok(true) => Ok(()),
                Ok(false) => Err("raft persistence commit failed".to_owned()),
                Err(_) => Err("store writer stopped".to_owned()),
            }
        })
    }
}

impl crate::cluster::store::RaftLogSink for GroupSink {
    fn append(&self, entries: Vec<(u64, Vec<u8>)>) -> SinkFuture<'_> {
        let group = self.group;
        self.run(move |done| StoreOp::RaftAppend {
            group,
            entries,
            done,
        })
    }

    fn save_vote(&self, vote: Vec<u8>) -> SinkFuture<'_> {
        let group = self.group;
        self.run(move |done| StoreOp::RaftVote { group, vote, done })
    }

    fn truncate_since(&self, since: u64) -> SinkFuture<'_> {
        let group = self.group;
        self.run(move |done| StoreOp::RaftTruncate { group, since, done })
    }

    fn purge_upto(&self, upto: u64, marker: Vec<u8>) -> SinkFuture<'_> {
        let group = self.group;
        self.run(move |done| StoreOp::RaftPurge {
            group,
            upto,
            marker,
            done,
        })
    }

    fn save_snapshot(
        &self,
        meta: Vec<u8>,
        blob: crate::cluster::store::SnapshotPersist,
    ) -> SinkFuture<'_> {
        let group = self.group;
        self.run(move |done| StoreOp::RaftSnapshot {
            group,
            meta,
            blob,
            done,
        })
    }
}

impl crate::cluster::store::RaftPersistFactory for Store {
    fn open_group(
        &self,
        group: &str,
    ) -> Result<
        (
            std::sync::Arc<dyn crate::cluster::store::RaftLogSink>,
            crate::cluster::store::RaftLogRecovery,
        ),
        String,
    > {
        let id = self.raft_group_id(group)?;
        let recovery = self.recover_raft(id)?;
        Ok((
            std::sync::Arc::new(GroupSink {
                store: self.clone(),
                group: id,
            }),
            recovery,
        ))
    }
}

/// The group-commit loop: drain a burst, one transaction, one fsync,
/// then notify.
fn writer_loop(db: Arc<Database>, mut rx: mpsc::Receiver<StoreOp>) {
    while let Some(first) = rx.blocking_recv() {
        let mut batch = vec![first];
        while batch.len() < COMMIT_BATCH_MAX {
            match rx.try_recv() {
                Ok(op) => batch.push(op),
                Err(_) => break,
            }
        }
        let committed = apply_batch(&db, &batch).is_ok();
        for op in batch {
            if let Some(done) = op.take_done() {
                let _ = done.send(committed);
            }
        }
    }
    tracing::debug!("store writer stopped");
}

fn apply_batch(db: &Database, batch: &[StoreOp]) -> Result<(), String> {
    let txn = db.begin_write().map_err(|e| e.to_string())?;
    {
        let mut raft_log = txn.open_table(RAFT_LOG).map_err(|e| e.to_string())?;
        let mut raft_meta = txn.open_table(RAFT_META).map_err(|e| e.to_string())?;
        let mut table = txn.open_table(MESSAGES).map_err(|e| e.to_string())?;
        for op in batch {
            match op {
                StoreOp::RaftAppend { group, entries, .. } => {
                    for (index, bytes) in entries {
                        raft_log
                            .insert((*group, *index), bytes.as_slice())
                            .map_err(|e| e.to_string())?;
                    }
                }
                StoreOp::RaftVote { group, vote, .. } => {
                    raft_meta
                        .insert((*group, "vote"), vote.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                StoreOp::RaftTruncate { group, since, .. } => {
                    raft_log
                        .retain_in((*group, *since)..=(*group, u64::MAX), |_, _| false)
                        .map_err(|e| e.to_string())?;
                }
                StoreOp::RaftPurge {
                    group,
                    upto,
                    marker,
                    ..
                } => {
                    raft_log
                        .retain_in((*group, 0)..=(*group, *upto), |_, _| false)
                        .map_err(|e| e.to_string())?;
                    raft_meta
                        .insert((*group, "purged"), marker.as_slice())
                        .map_err(|e| e.to_string())?;
                }
                StoreOp::RaftSnapshot {
                    group, meta, blob, ..
                } => {
                    use crate::cluster::store::SnapshotPersist;
                    raft_meta
                        .insert((*group, "snap_meta"), meta.as_slice())
                        .map_err(|e| e.to_string())?;
                    // Exactly one of blob/path survives, atomically with the
                    // pointer (a stale sibling would resurrect an old
                    // snapshot on recovery).
                    match blob {
                        SnapshotPersist::Inline(bytes) => {
                            raft_meta
                                .insert((*group, "snap_blob"), bytes.as_slice())
                                .map_err(|e| e.to_string())?;
                            raft_meta
                                .remove((*group, "snap_path"))
                                .map_err(|e| e.to_string())?;
                        }
                        SnapshotPersist::File(path) => {
                            raft_meta
                                .insert(
                                    (*group, "snap_path"),
                                    path.to_string_lossy().as_bytes(),
                                )
                                .map_err(|e| e.to_string())?;
                            raft_meta
                                .remove((*group, "snap_blob"))
                                .map_err(|e| e.to_string())?;
                        }
                    }
                }
                StoreOp::Insert {
                    queue,
                    msg_id,
                    enqueued_ms,
                    body,
                    ..
                } => {
                    table
                        .insert((*queue, *msg_id), (0u32, *enqueued_ms, body.as_ref()))
                        .map_err(|e| e.to_string())?;
                }
                StoreOp::Remove { queue, msg_id } => {
                    table.remove((*queue, *msg_id)).map_err(|e| e.to_string())?;
                }
                StoreOp::Fail { queue, msg_id } => {
                    let updated = table
                        .get((*queue, *msg_id))
                        .map_err(|e| e.to_string())?
                        .map(|v| {
                            let (failures, enqueued_ms, body) = v.value();
                            (failures + 1, enqueued_ms, body.to_vec())
                        });
                    if let Some((failures, enqueued_ms, body)) = updated {
                        table
                            .insert((*queue, *msg_id), (failures, enqueued_ms, body.as_slice()))
                            .map_err(|e| e.to_string())?;
                    }
                }
            }
        }
    }
    txn.commit().map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn insert_survives_reopen_and_scan_recovers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let store = Store::open(dir.path()).expect("open");
        let q = store.queue_id("orders").expect("queue id");

        for i in 1..=3u64 {
            let (tx, rx) = oneshot::channel();
            store
                .submit(StoreOp::Insert {
                    queue: q,
                    msg_id: i,
                    enqueued_ms: 1_000 + i,
                    body: Bytes::from(vec![i as u8; 8]),
                    done: tx,
                })
                .await
                .expect("submit");
            assert!(rx.await.expect("commit notify"), "insert committed");
        }
        // Ack one, fail another.
        store
            .submit(StoreOp::Remove {
                queue: q,
                msg_id: 1,
            })
            .await
            .expect("remove");
        store
            .submit(StoreOp::Fail {
                queue: q,
                msg_id: 2,
            })
            .await
            .expect("fail");
        // Barrier: a further committed insert proves the batch landed.
        let (tx, rx) = oneshot::channel();
        store
            .submit(StoreOp::Insert {
                queue: q,
                msg_id: 4,
                enqueued_ms: 1_004,
                body: Bytes::from_static(b"x"),
                done: tx,
            })
            .await
            .expect("submit");
        assert!(rx.await.expect("notify"));
        drop(store);

        // Reopen: state survives the process boundary. (In-process, the
        // writer thread releases the database lock asynchronously after the
        // handle drops — retry briefly; a real restart is a process
        // boundary.)
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let store = loop {
            match Store::open(dir.path()) {
                Ok(store) => break store,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "database lock never released: {e}"
                    );
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
            }
        };
        let q2 = store.queue_id("orders").expect("same queue");
        assert_eq!(q2, q, "queue id is stable across restarts");
        let mut recovered = store.scan(q).expect("scan");
        recovered.sort_unstable();
        assert_eq!(recovered, vec![(2, 1, 1_002), (3, 0, 1_003), (4, 0, 1_004)]);
        assert_eq!(&store.body(q, 3).expect("body")[..], &[3u8; 8]);
        assert!(store.body(q, 1).is_none(), "acked message is gone");
    }
}
