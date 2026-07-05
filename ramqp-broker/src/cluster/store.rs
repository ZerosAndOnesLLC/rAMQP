//! In-memory Raft storage, generic over the replicated state machine.
//!
//! One storage implementation serves every Raft group in the broker: the
//! metadata group ([`MetaState`]) and each per-queue group (Phase 6). Log,
//! vote, snapshot, and applied state live in memory — sufficient for cluster
//! formation and tests; the durable (on-disk) log is Phase 7 work, and the
//! storage trait boundary is exactly where it slots in.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftTypeConfig, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::NodeId;
use super::meta::{self, MetaCommand, MetaResponse, QueueSpec};

/// A deterministic replicated state machine: the only broker-specific part
/// of a Raft group.
pub trait ReplicatedState:
    Default + Debug + Clone + Serialize + DeserializeOwned + Send + Sync + 'static
{
    /// The command type committed through the log.
    type Command;
    /// The response returned to the proposer.
    type Response;

    /// Apply one committed command. Must be deterministic.
    fn apply(&mut self, command: &Self::Command) -> Self::Response;

    /// The response used for non-app entries (blank/membership), which still
    /// consume a slot in openraft's response vector.
    fn void_response() -> Self::Response;
}

/// The metadata group's state: the replicated queue catalog.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct MetaState {
    /// Queue name → replicated description.
    pub catalog: BTreeMap<String, QueueSpec>,
}

impl ReplicatedState for MetaState {
    type Command = MetaCommand;
    type Response = MetaResponse;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        meta::apply(&mut self.catalog, command)
    }

    fn void_response() -> Self::Response {
        MetaResponse::NotFound
    }
}

/// The serialized form of a state-machine snapshot.
#[derive(Serialize, serde::Deserialize)]
#[serde(bound = "S: Serialize + DeserializeOwned")]
struct SnapshotPayload<S> {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    state: S,
}

#[derive(Debug)]
struct Inner<C: RaftTypeConfig, S> {
    /// The Raft log.
    log: BTreeMap<u64, C::Entry>,
    /// The last purged (compacted-away) log id.
    last_purged: Option<LogId<NodeId>>,
    /// The persisted vote.
    vote: Option<Vote<NodeId>>,
    /// The applied state machine.
    state: S,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// The current snapshot, if one was built/installed.
    snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, Vec<u8>)>,
    snapshot_idx: u64,
}

impl<C: RaftTypeConfig, S: Default> Default for Inner<C, S> {
    fn default() -> Self {
        Inner {
            log: BTreeMap::new(),
            last_purged: None,
            vote: None,
            state: S::default(),
            last_applied: None,
            last_membership: StoredMembership::default(),
            snapshot: None,
            snapshot_idx: 0,
        }
    }
}

/// Shared in-memory storage for one Raft-group member. Internally
/// reference-counted — clones share the same log/state. A local wrapper (not
/// a bare `Arc`) so the openraft storage traits can be implemented
/// generically without tripping the orphan rules.
#[derive(Debug)]
pub struct SharedStore<C: RaftTypeConfig, S>(Arc<MemStore<C, S>>);

impl<C: RaftTypeConfig, S> Clone for SharedStore<C, S> {
    fn clone(&self) -> Self {
        SharedStore(self.0.clone())
    }
}

impl<C: RaftTypeConfig, S: Default> Default for SharedStore<C, S> {
    fn default() -> Self {
        SharedStore(Arc::new(MemStore {
            inner: Mutex::new(Inner::default()),
        }))
    }
}

#[derive(Debug)]
struct MemStore<C: RaftTypeConfig, S> {
    inner: Mutex<Inner<C, S>>,
}

impl<C: RaftTypeConfig, S> SharedStore<C, S> {
    fn lock(&self) -> MutexGuard<'_, Inner<C, S>> {
        self.0.inner.lock().expect("raft store lock")
    }

    /// Read the applied state through `f` (point-in-time, under the lock).
    pub fn with_state<T>(&self, f: impl FnOnce(&S) -> T) -> T {
        f(&self.lock().state)
    }

    /// Diagnostics: `(log entries held, last purged index)` — compaction
    /// visibility for tests and (later) the management surface.
    pub fn log_stats(&self) -> (usize, Option<u64>) {
        let inner = self.lock();
        (inner.log.len(), inner.last_purged.map(|l| l.index))
    }
}

/// The metadata group's storage.
pub type MetaStore = SharedStore<super::MetaTypeConfig, MetaState>;

impl MetaStore {
    /// A point-in-time copy of the applied queue catalog.
    pub fn catalog(&self) -> BTreeMap<String, QueueSpec> {
        self.with_state(|s| s.catalog.clone())
    }
}

impl<C, S> RaftLogReader<C> for SharedStore<C, S>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode, Entry = Entry<C>>,
    C::D: Clone + Debug,
    S: ReplicatedState<Command = C::D, Response = C::R>,
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<NodeId>> {
        Ok(self
            .lock()
            .log
            .range(range)
            .map(|(_, e)| e.clone())
            .collect())
    }
}

impl<C, S> RaftSnapshotBuilder<C> for SharedStore<C, S>
where
    C: RaftTypeConfig<
            NodeId = NodeId,
            Node = BasicNode,
            Entry = Entry<C>,
            SnapshotData = Cursor<Vec<u8>>,
        >,
    C::D: Clone + Debug,
    S: ReplicatedState<Command = C::D, Response = C::R>,
{
    async fn build_snapshot(&mut self) -> Result<Snapshot<C>, StorageError<NodeId>> {
        let mut inner = self.lock();
        let payload = SnapshotPayload {
            last_applied: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            state: inner.state.clone(),
        };
        // Compact binary encoding: snapshots of binary-heavy queue state must
        // not inflate (JSON turns 256-byte bodies into ~1 KB integer arrays).
        let data = bincode::serialize(&payload)
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

impl<C, S> RaftStorage<C> for SharedStore<C, S>
where
    C: RaftTypeConfig<
            NodeId = NodeId,
            Node = BasicNode,
            Entry = Entry<C>,
            SnapshotData = Cursor<Vec<u8>>,
        >,
    C::D: Clone + Debug,
    S: ReplicatedState<Command = C::D, Response = C::R>,
{
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.lock().vote)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<NodeId>> {
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
        I: IntoIterator<Item = C::Entry> + Send,
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
        entries: &[C::Entry],
    ) -> Result<Vec<C::R>, StorageError<NodeId>> {
        let mut inner = self.lock();
        let mut responses = Vec::with_capacity(entries.len());
        for entry in entries {
            inner.last_applied = Some(*entry.get_log_id());
            match &entry.payload {
                EntryPayload::Blank => responses.push(S::void_response()),
                EntryPayload::Normal(cmd) => {
                    let resp = inner.state.apply(cmd);
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    inner.last_membership =
                        StoredMembership::new(Some(*entry.get_log_id()), m.clone());
                    responses.push(S::void_response());
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
        let payload: SnapshotPayload<S> = bincode::deserialize(&data)
            .map_err(|e| openraft::StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let mut inner = self.lock();
        inner.last_applied = payload.last_applied;
        inner.last_membership = payload.last_membership;
        inner.state = payload.state;
        inner.snapshot = Some((meta.clone(), data));
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<NodeId>> {
        Ok(self.lock().snapshot.clone().map(|(meta, data)| Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data)),
        }))
    }
}
