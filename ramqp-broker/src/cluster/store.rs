//! In-memory Raft storage for the metadata group.
//!
//! Log, vote, snapshot, and the applied state machine live in memory —
//! sufficient for cluster formation and tests. The durable (on-disk) log is
//! Phase 7 work; the storage trait boundary is exactly where it slots in.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};
use serde::{Deserialize, Serialize};

use super::meta::{self, MetaResponse, QueueSpec};
use super::{MetaTypeConfig, NodeId};

/// The serialized form of a state-machine snapshot.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    catalog: BTreeMap<String, QueueSpec>,
}

#[derive(Debug, Default)]
struct Inner {
    /// The Raft log.
    log: BTreeMap<u64, Entry<MetaTypeConfig>>,
    /// The last purged (compacted-away) log id.
    last_purged: Option<LogId<NodeId>>,
    /// The persisted vote.
    vote: Option<Vote<NodeId>>,
    /// Applied state: the queue catalog.
    catalog: BTreeMap<String, QueueSpec>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// The current snapshot, if one was built/installed.
    snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
    snapshot_idx: u64,
}

/// Shared in-memory storage for one metadata-group node.
#[derive(Debug, Default)]
pub struct MetaStore {
    inner: Mutex<Inner>,
}

impl MetaStore {
    fn lock(&self) -> MutexGuard<'_, Inner> {
        self.inner.lock().expect("meta store lock")
    }

    /// A point-in-time copy of the applied queue catalog.
    pub fn catalog(&self) -> BTreeMap<String, QueueSpec> {
        self.lock().catalog.clone()
    }
}

impl RaftLogReader<MetaTypeConfig> for Arc<MetaStore> {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<MetaTypeConfig>>, StorageError<NodeId>> {
        Ok(self
            .lock()
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl RaftSnapshotBuilder<MetaTypeConfig> for Arc<MetaStore> {
    async fn build_snapshot(&mut self) -> Result<Snapshot<MetaTypeConfig>, StorageError<NodeId>> {
        let mut inner = self.lock();
        let payload = SnapshotPayload {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            catalog: inner.catalog.clone(),
        };
        let data = serde_json::to_vec(&payload)
            .map_err(|e| openraft::StorageIOError::write_snapshot(None, &e))?;
        inner.snapshot_idx += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id: format!(
                "{}-{}",
                inner
                    .last_applied
                    .map(|l| l.index.to_string())
                    .unwrap_or_else(|| "none".to_owned()),
                inner.snapshot_idx
            ),
        };
        inner.snapshot = Some((meta.clone(), data.clone()));
        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        })
    }
}

impl RaftStorage<MetaTypeConfig> for Arc<MetaStore> {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.lock().vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<MetaTypeConfig>, StorageError<NodeId>> {
        let inner = self.lock();
        let last = inner
            .log
            .iter()
            .next_back()
            .map(|(_, e)| *e.get_log_id())
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id: last,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I>(&mut self, entries: I) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<MetaTypeConfig>> + Send,
    {
        let mut inner = self.lock();
        for entry in entries {
            inner.log.insert(entry.get_log_id().index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<NodeId>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.lock();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.lock();
        inner.last_purged = Some(log_id);
        let keys: Vec<u64> = inner.log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<MetaTypeConfig>],
    ) -> Result<Vec<MetaResponse>, StorageError<NodeId>> {
        let mut inner = self.lock();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.last_applied = Some(*entry.get_log_id());
            match &entry.payload {
                EntryPayload::Blank => responses.push(MetaResponse::NotFound),
                EntryPayload::Normal(cmd) => {
                    let resp = meta::apply(&mut inner.catalog, cmd);
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    inner.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), m.clone());
                    responses.push(MetaResponse::NotFound);
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<NodeId>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<NodeId>> {
        let data = snapshot.into_inner();
        let payload: SnapshotPayload = serde_json::from_slice(&data)
            .map_err(|e| openraft::StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.lock();
        inner.last_applied = payload.last_applied;
        inner.last_membership = payload.last_membership;
        inner.catalog = payload.catalog;
        inner.snapshot = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<MetaTypeConfig>>, StorageError<NodeId>> {
        Ok(self.lock().snapshot.clone().map(|(meta, data)| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}
