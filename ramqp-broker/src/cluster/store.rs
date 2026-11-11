//! In-memory Raft storage, generic over the replicated state machine.
//!
//! One storage implementation serves every Raft group in the broker: the
//! metadata group ([`MetaState`]) and each per-queue group (Phase 6). Log,
//! vote, and applied state live in memory — sufficient for cluster
//! formation and tests; the durable (on-disk) log is a later Phase 7 slice,
//! and the storage trait boundary is exactly where it slots in. Snapshot
//! *blobs* optionally live on disk (a deep paged queue's snapshot must not
//! double its RSS — §3.1).

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage, Snapshot};
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, RaftLogId, RaftTypeConfig, SnapshotMeta, StorageError,
    StoredMembership, Vote,
};
use serde::Serialize;

use super::NodeId;
use super::meta::{self, MetaCommand, MetaResponse, QueueSpec};

/// A deterministic replicated state machine: the only broker-specific part
/// of a Raft group.
pub trait ReplicatedState: Default + Debug + Clone + Send + Sync + 'static {
    /// The command type committed through the log.
    type Command;
    /// The response returned to the proposer.
    type Response;

    /// Apply one committed command. Must be deterministic.
    fn apply(&mut self, command: &Self::Command) -> Self::Response;

    /// The response used for non-app entries (blank/membership), which still
    /// consume a slot in openraft's response vector.
    fn void_response() -> Self::Response;

    /// Called under the store lock immediately before the state is cloned
    /// for a snapshot build — pin any external resources (spill segments)
    /// the off-lock serialization will read. Balanced by
    /// [`snapshot_bytes`](ReplicatedState::snapshot_bytes), which must
    /// unpin on every path.
    fn prepare_snapshot(&self) {}

    /// Serialize the state for a snapshot (may read resources pinned by
    /// [`prepare_snapshot`](ReplicatedState::prepare_snapshot)). Balanced by
    /// [`finish_snapshot`](ReplicatedState::finish_snapshot), which the
    /// builder ALWAYS calls afterward — including when serialization never
    /// ran (task cancellation at runtime shutdown), so a pin can never leak.
    fn snapshot_bytes(&self) -> Result<Vec<u8>, String>;

    /// Release whatever [`prepare_snapshot`](ReplicatedState::prepare_snapshot)
    /// pinned. Called exactly once per build by the snapshot builder, on
    /// every path.
    fn finish_snapshot(&self) {}

    /// Restore from [`snapshot_bytes`](ReplicatedState::snapshot_bytes).
    fn restore_snapshot(&mut self, bytes: &[u8]) -> Result<(), String>;
}

/// The metadata group's state: the replicated queue catalog and the
/// exchange topology.
#[derive(Debug, Clone, Default, Serialize, serde::Deserialize)]
pub struct MetaState {
    /// Queue name → replicated description.
    pub catalog: BTreeMap<String, QueueSpec>,
    /// Exchange definitions (with their bindings).
    pub exchanges: crate::exchange::Defs,
    /// Bumped on every applied exchange change, so nodes can tell whether
    /// their local routing view is current.
    pub exchanges_epoch: u64,
    /// Per quorum-queue name: how many times it has been deleted. The
    /// queue's Raft group id (fabric address and on-disk storage) is derived
    /// from `(name, incarnation)` — see [`super::group_id`] — so a queue
    /// re-declared after a delete never shares state with the old one, and
    /// every node can discard any group older than the current incarnation.
    pub incarnations: BTreeMap<String, u64>,
    /// Runtime definitions (per-queue policies, users, permissions), by key.
    pub defs: BTreeMap<String, Vec<u8>>,
    /// Bumped on every applied definition change.
    pub defs_epoch: u64,
}

/// The 0.11.0 snapshot encoding (no runtime definitions).
#[derive(serde::Deserialize)]
struct MetaStateV3 {
    catalog: BTreeMap<String, QueueSpec>,
    exchanges: crate::exchange::Defs,
    exchanges_epoch: u64,
    incarnations: BTreeMap<String, u64>,
}

/// The pre-exchange (0.9.x) snapshot encoding, still readable so a node
/// upgraded in place restores its catalog.
#[derive(serde::Deserialize)]
struct LegacyMetaState {
    catalog: BTreeMap<String, QueueSpec>,
}

/// The 0.10.0 snapshot encoding (no incarnations).
#[derive(serde::Deserialize)]
struct MetaStateV2 {
    catalog: BTreeMap<String, QueueSpec>,
    exchanges: crate::exchange::Defs,
    exchanges_epoch: u64,
}

impl ReplicatedState for MetaState {
    type Command = MetaCommand;
    type Response = MetaResponse;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        if let MetaCommand::Exchange(op) = command {
            let result = crate::exchange::apply_op(&mut self.exchanges, op).map(|changed| {
                if changed {
                    self.exchanges_epoch += 1;
                }
                (changed, self.exchanges_epoch)
            });
            return MetaResponse::Exchange(result);
        }
        if let MetaCommand::Def { key, value } = command {
            let changed = match value {
                Some(v) => {
                    self.defs.get(key) != Some(v) && {
                        self.defs.insert(key.clone(), v.clone());
                        true
                    }
                }
                None => self.defs.remove(key).is_some(),
            };
            if changed {
                self.defs_epoch += 1;
            }
            return MetaResponse::Def(changed, self.defs_epoch);
        }
        if let MetaCommand::DeleteQueue { name } = command {
            // Every node applies this, so every node moves the name to its
            // next incarnation in the same log position.
            return if self.catalog.remove(name).is_some() {
                *self.incarnations.entry(name.clone()).or_default() += 1;
                MetaResponse::Deleted
            } else {
                MetaResponse::NotFound
            };
        }
        meta::apply(&mut self.catalog, command)
    }

    fn void_response() -> Self::Response {
        MetaResponse::NotFound
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        // Use the oldest encoding that holds the state, so a node not yet
        // upgraded (rolling upgrade, or a rollback) can still install it:
        // 0.9.x until exchanges are used, 0.10.0 until a queue is deleted,
        // 0.11.0 until a runtime definition is written.
        if self.defs.is_empty() && self.defs_epoch == 0 && !self.incarnations.is_empty() {
            #[derive(Serialize)]
            struct V3<'a> {
                catalog: &'a BTreeMap<String, QueueSpec>,
                exchanges: &'a crate::exchange::Defs,
                exchanges_epoch: u64,
                incarnations: &'a BTreeMap<String, u64>,
            }
            return crate::serde_bin::to_vec(&V3 {
                catalog: &self.catalog,
                exchanges: &self.exchanges,
                exchanges_epoch: self.exchanges_epoch,
                incarnations: &self.incarnations,
            })
            .map_err(|e| e.to_string());
        }
        let no_defs = self.defs.is_empty() && self.defs_epoch == 0;
        if no_defs
            && self.incarnations.is_empty()
            && !(self.exchanges.is_empty() && self.exchanges_epoch == 0)
        {
            #[derive(Serialize)]
            struct V2<'a> {
                catalog: &'a BTreeMap<String, QueueSpec>,
                exchanges: &'a crate::exchange::Defs,
                exchanges_epoch: u64,
            }
            return crate::serde_bin::to_vec(&V2 {
                catalog: &self.catalog,
                exchanges: &self.exchanges,
                exchanges_epoch: self.exchanges_epoch,
            })
            .map_err(|e| e.to_string());
        }
        if no_defs
            && self.incarnations.is_empty()
            && self.exchanges.is_empty()
            && self.exchanges_epoch == 0
        {
            #[derive(Serialize)]
            struct Legacy<'a> {
                catalog: &'a BTreeMap<String, QueueSpec>,
            }
            return crate::serde_bin::to_vec(&Legacy {
                catalog: &self.catalog,
            })
            .map_err(|e| e.to_string());
        }
        crate::serde_bin::to_vec(self).map_err(|e| e.to_string())
    }

    fn restore_snapshot(&mut self, bytes: &[u8]) -> Result<(), String> {
        // Newest encoding first; each older one is a strict prefix-shape, and
        // decoding is exact (trailing bytes fail), so the first match wins.
        *self = match crate::serde_bin::from_slice(bytes) {
            Ok(state) => state,
            Err(e) => match crate::serde_bin::from_slice::<MetaStateV3>(bytes)
                .map(|v3| MetaState {
                    catalog: v3.catalog,
                    exchanges: v3.exchanges,
                    exchanges_epoch: v3.exchanges_epoch,
                    incarnations: v3.incarnations,
                    ..Default::default()
                })
                .or_else(|_| {
                    crate::serde_bin::from_slice::<MetaStateV2>(bytes).map(|v2| MetaState {
                        catalog: v2.catalog,
                        exchanges: v2.exchanges,
                        exchanges_epoch: v2.exchanges_epoch,
                        ..Default::default()
                    })
                }) {
                Ok(state) => state,
                Err(_) => {
                    let legacy: LegacyMetaState =
                        crate::serde_bin::from_slice(bytes).map_err(|_| e.to_string())?;
                    MetaState {
                        catalog: legacy.catalog,
                        ..Default::default()
                    }
                }
            },
        };
        Ok(())
    }
}

/// The serialized form of a state-machine snapshot: raft positions plus the
/// state's own [`ReplicatedState::snapshot_bytes`] encoding.
#[derive(Serialize, serde::Deserialize)]
struct SnapshotPayload {
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    state_bytes: Vec<u8>,
}

/// Where a built/installed snapshot blob lives.
#[derive(Debug, Clone)]
enum SnapshotBlob {
    /// In memory (small states: the metadata catalog, shallow queues).
    Memory(Vec<u8>),
    /// On disk (paged queues: the blob must not double the queue's RSS).
    File(PathBuf),
}

/// Where a snapshot blob lives for persistence purposes: small blobs (the
/// metadata catalog, unpaged queues) ride **inline** through the sink and are
/// stored in its database — durable and atomic with the snapshot pointer;
/// large paged-queue blobs stay in their **file** and only the path is
/// recorded. Without the inline form, a group whose blobs live in memory
/// would durably purge its log (openraft persists the purge) while its
/// snapshot evaporated on restart — silent total state loss.
#[derive(Debug, Clone)]
pub enum SnapshotPersist {
    /// The blob bytes themselves; the sink stores them.
    Inline(Vec<u8>),
    /// The blob lives in this file; the sink records the path.
    File(PathBuf),
}

/// Write-through persistence for one Raft group's hard state (log entries,
/// vote, purge marker, snapshot pointer). The in-memory maps stay the
/// working copy; every mutation that Raft requires to be durable goes
/// through here **before** the storage call returns. Restart recovery loads
/// the same data back via [`RaftLogRecovery`].
///
/// Entries and votes are pre-encoded (serde_bin) by the caller so the sink is
/// type-erased and one implementation serves every group.
pub trait RaftLogSink: Send + Sync + std::fmt::Debug {
    /// Durably append `(index, encoded entry)` pairs; returns once fsynced.
    fn append(
        &self,
        entries: Vec<(u64, Vec<u8>)>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Durably record the vote.
    fn save_vote(
        &self,
        vote: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Remove entries with `index >= since` (leader-change conflict).
    fn truncate_since(
        &self,
        since: u64,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Remove entries with `index <= upto` and record the purge marker.
    fn purge_upto(
        &self,
        upto: u64,
        marker: Vec<u8>,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;

    /// Record the current snapshot (encoded meta + the blob, inline or by
    /// path — see [`SnapshotPersist`]).
    fn save_snapshot(
        &self,
        meta: Vec<u8>,
        blob: SnapshotPersist,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + '_>>;
}

/// Opens per-group persistence: implemented by the on-disk store
/// (`store-redb`), consumed wherever a Raft group member is created.
pub trait RaftPersistFactory: Send + Sync + std::fmt::Debug {
    /// The sink + whatever was recovered for `group`.
    fn open_group(&self, group: &str) -> Result<(Arc<dyn RaftLogSink>, RaftLogRecovery), String>;
    /// Every group with persisted state.
    fn groups(&self) -> Result<Vec<String>, String>;
    /// Durably remove a group's persisted state (log, vote, snapshot
    /// pointer, and its id). The group must no longer be running.
    fn purge_group(&self, group: &str) -> Result<(), String>;
}

/// What a [`RaftLogSink`] recovered from disk at startup.
#[derive(Debug, Default)]
pub struct RaftLogRecovery {
    /// Encoded vote, when one was saved.
    pub vote: Option<Vec<u8>>,
    /// Encoded purge marker, when one was saved.
    pub purged: Option<Vec<u8>>,
    /// Encoded `(index, entry)` pairs still in the log, ascending.
    pub entries: Vec<(u64, Vec<u8>)>,
    /// Encoded snapshot meta + its blob (inline bytes or the blob file),
    /// when a snapshot was saved.
    pub snapshot: Option<(Vec<u8>, SnapshotPersist)>,
}

use std::pin::Pin;

impl SnapshotBlob {
    fn read(&self) -> std::io::Result<Vec<u8>> {
        match self {
            SnapshotBlob::Memory(bytes) => Ok(bytes.clone()),
            SnapshotBlob::File(path) => std::fs::read(path),
        }
    }
}

/// Write a snapshot blob so it survives power loss: create the directory,
/// write the bytes, fsync the file, then fsync the directory (the entry
/// itself must be durable — without it a crash can leave the durable
/// snapshot pointer naming a file that never made it to disk, which recovery
/// reads as silent total state loss below the purge marker).
fn write_blob_durably(
    dir: &std::path::Path,
    path: &std::path::Path,
    data: &[u8],
) -> std::io::Result<()> {
    std::fs::create_dir_all(dir)?;
    let mut file = std::fs::File::create(path)?;
    std::io::Write::write_all(&mut file, data)?;
    file.sync_all()?;
    std::fs::File::open(dir)?.sync_all()?;
    Ok(())
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
    snapshot: Option<(SnapshotMeta<NodeId, BasicNode>, SnapshotBlob)>,
    snapshot_idx: u64,
    /// When set, snapshot blobs are written here instead of held in memory.
    snapshot_dir: Option<PathBuf>,
    /// Write-through hard-state persistence (`None` = in-memory only).
    persist: Option<Arc<dyn RaftLogSink>>,
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
            snapshot_dir: None,
            persist: None,
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

    /// A store with a given initial state and (optionally) a directory for
    /// on-disk snapshot blobs (paged queues).
    pub fn new_with(state: S, snapshot_dir: Option<PathBuf>) -> Self
    where
        S: Default,
    {
        let store = SharedStore(Arc::new(MemStore {
            inner: Mutex::new(Inner::default()),
        }));
        {
            let mut inner = store.lock();
            inner.state = state;
            inner.snapshot_dir = snapshot_dir;
        }
        store
    }

    /// The sink, when persistence is wired.
    fn persist(&self) -> Option<Arc<dyn RaftLogSink>> {
        self.lock().persist.clone()
    }

    /// A persistent store: hard state recovered from `recovery`, every
    /// later mutation written through `sink` before the Raft call returns.
    pub fn new_persistent(
        state: S,
        snapshot_dir: Option<PathBuf>,
        sink: Arc<dyn RaftLogSink>,
        recovery: RaftLogRecovery,
    ) -> Result<Self, String>
    where
        S: ReplicatedState,
        C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode, Entry = Entry<C>>,
        C::Entry: serde::de::DeserializeOwned,
    {
        let store = Self::new_with(state, snapshot_dir);
        {
            let mut inner = store.lock();
            if let Some(vote) = &recovery.vote {
                inner.vote = Some(
                    crate::serde_bin::from_slice(vote).map_err(|e| format!("vote decode: {e}"))?,
                );
            }
            if let Some(purged) = &recovery.purged {
                inner.last_purged = Some(
                    crate::serde_bin::from_slice(purged)
                        .map_err(|e| format!("purge decode: {e}"))?,
                );
            }
            for (index, bytes) in &recovery.entries {
                let entry: C::Entry = crate::serde_bin::from_slice(bytes)
                    .map_err(|e| format!("log entry {index} decode: {e}"))?;
                inner.log.insert(*index, entry);
            }
            if let Some((meta_bytes, recovered_blob)) = &recovery.snapshot {
                let meta: SnapshotMeta<NodeId, BasicNode> =
                    crate::serde_bin::from_slice(meta_bytes)
                        .map_err(|e| format!("snapshot meta decode: {e}"))?;
                let (data, blob) = match recovered_blob {
                    SnapshotPersist::Inline(bytes) => {
                        (bytes.clone(), SnapshotBlob::Memory(bytes.clone()))
                    }
                    SnapshotPersist::File(path) => {
                        let data =
                            std::fs::read(path).map_err(|e| format!("snapshot blob read: {e}"))?;
                        (data, SnapshotBlob::File(path.clone()))
                    }
                };
                let payload: SnapshotPayload = crate::serde_bin::from_slice(&data)
                    .map_err(|e| format!("snapshot payload decode: {e}"))?;
                inner
                    .state
                    .restore_snapshot(&payload.state_bytes)
                    .map_err(|e| format!("snapshot state restore: {e}"))?;
                inner.last_applied = payload.last_applied;
                inner.last_membership = payload.last_membership;
                // Continue the snapshot-id counter past the recovered
                // snapshot: a fresh counter would rebuild at the same applied
                // index under the SAME file name, truncating the only copy of
                // the current snapshot in place (torn blob on crash).
                if let Some(idx) = meta
                    .snapshot_id
                    .rsplit('-')
                    .next()
                    .and_then(|s| s.parse::<u64>().ok())
                {
                    inner.snapshot_idx = idx;
                }
                inner.snapshot = Some((meta, blob));
            }
            inner.persist = Some(sink);
        }
        Ok(store)
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

    /// A quorum queue's catalog entry and current incarnation, read together
    /// (one consistent applied state).
    pub fn queue(&self, name: &str) -> Option<(QueueSpec, u64)> {
        self.with_state(|s| {
            s.catalog
                .get(name)
                .map(|spec| (spec.clone(), s.incarnations.get(name).copied().unwrap_or(0)))
        })
    }

    /// The runtime definitions and their epoch (one consistent read).
    pub fn defs(&self) -> (u64, BTreeMap<String, Vec<u8>>) {
        self.with_state(|s| (s.defs_epoch, s.defs.clone()))
    }

    /// The applied runtime-definitions epoch.
    pub fn defs_epoch(&self) -> u64 {
        self.with_state(|s| s.defs_epoch)
    }

    /// A queue name's current incarnation (0 until it is first deleted).
    pub fn incarnation(&self, name: &str) -> u64 {
        self.with_state(|s| s.incarnations.get(name).copied().unwrap_or(0))
    }

    /// Every name's incarnation (names never deleted are absent).
    pub fn incarnations(&self) -> BTreeMap<String, u64> {
        self.with_state(|s| s.incarnations.clone())
    }

    /// The applied exchange-definitions epoch.
    pub fn exchanges_epoch(&self) -> u64 {
        self.with_state(|s| s.exchanges_epoch)
    }

    /// A point-in-time copy of the exchange definitions and their epoch.
    pub fn exchanges(&self) -> (u64, crate::exchange::Defs) {
        self.with_state(|s| (s.exchanges_epoch, s.exchanges.clone()))
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
        // Capture a cheap point-in-time copy under the lock, then RELEASE it —
        // the same mutex serializes apply/append and every catalog/dispatch
        // read, so serializing a deep queue's state (potentially multi-second)
        // while holding it would stall the whole group and time out followers.
        // `prepare_snapshot` (still under the lock) pins any external
        // resources the off-lock serialization reads.
        let (state, last_applied, last_membership, meta, snapshot_dir) = {
            let mut inner = self.lock();
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
            inner.state.prepare_snapshot();
            (
                inner.state.clone(),
                inner.last_applied,
                inner.last_membership.clone(),
                meta,
                inner.snapshot_dir.clone(),
            )
        };
        // Serialize off the async worker (CPU-heavy for large state) and
        // off-lock; `snapshot_bytes` unpins whatever `prepare_snapshot`
        // pinned. Compact binary encoding throughout.
        let idx = meta.snapshot_id.clone();
        let built =
            tokio::task::spawn_blocking(move || -> Result<(Vec<u8>, SnapshotBlob), String> {
                let state_bytes = state.snapshot_bytes()?;
                let payload = SnapshotPayload {
                    last_applied,
                    last_membership,
                    state_bytes,
                };
                let data = crate::serde_bin::to_vec(&payload).map_err(|e| e.to_string())?;
                // Deep paged states: park the blob on disk so the snapshot does
                // not double the queue's RSS. Written durably — this blob may
                // become the ONLY copy of the state once the log purges.
                let blob = match &snapshot_dir {
                    Some(dir) => {
                        let path = dir.join(format!("snapshot-{idx}.bin"));
                        write_blob_durably(dir, &path, &data).map_err(|e| e.to_string())?;
                        SnapshotBlob::File(path)
                    }
                    None => SnapshotBlob::Memory(data.clone()),
                };
                Ok((data, blob))
            })
            .await;
        // ALWAYS unpin, even if the blocking task was cancelled before it
        // ran (runtime shutdown) — the clone shares the spill Arc with the
        // live state, so unpinning through the live state balances the pin
        // no matter what happened to the closure (LOW-16).
        self.lock().state.finish_snapshot();
        let built = built
            .map_err(|e| openraft::StorageIOError::write_snapshot(None, &e))?
            .map_err(|e| {
                openraft::StorageIOError::write_snapshot(None, &std::io::Error::other(e))
            })?;
        let (data, blob) = built;
        // Record the snapshot durably before the old blob goes away —
        // memory-held blobs ride inline (a durably purged log with no
        // durable snapshot would be silent total state loss on restart),
        // file blobs by path (recovery must never point at a deleted file).
        if let Some(sink) = self.persist() {
            let encoded = crate::serde_bin::to_vec(&meta)
                .map_err(|e| openraft::StorageIOError::write_snapshot(None, &e))?;
            let persist_blob = match &blob {
                SnapshotBlob::Memory(bytes) => SnapshotPersist::Inline(bytes.clone()),
                SnapshotBlob::File(path) => SnapshotPersist::File(path.clone()),
            };
            sink.save_snapshot(encoded, persist_blob)
                .await
                .map_err(|e| {
                    openraft::StorageIOError::write_snapshot(None, &std::io::Error::other(e))
                })?;
        }
        // Briefly re-lock only to publish the finished snapshot (and drop
        // the previous on-disk blob).
        {
            let mut inner = self.lock();
            if let Some((_, SnapshotBlob::File(old))) = &inner.snapshot
                && !matches!(&blob, SnapshotBlob::File(new) if new == old)
            {
                let _ = std::fs::remove_file(old);
            }
            inner.snapshot = Some((meta.clone(), blob));
        }
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
        if let Some(sink) = self.persist() {
            let encoded = crate::serde_bin::to_vec(vote)
                .map_err(|e| openraft::StorageIOError::write_vote(&e))?;
            sink.save_vote(encoded)
                .await
                .map_err(|e| openraft::StorageIOError::write_vote(&std::io::Error::other(e)))?;
        }
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
        let entries: Vec<C::Entry> = entries.into_iter().collect();
        if let Some(sink) = self.persist() {
            // Raft safety: an entry acknowledged to the leader must be
            // durable BEFORE this returns. The sink group-commits, so
            // concurrent groups share one fsync.
            let mut encoded = Vec::with_capacity(entries.len());
            for entry in &entries {
                encoded.push((
                    entry.get_log_id().index,
                    crate::serde_bin::to_vec(entry)
                        .map_err(|e| openraft::StorageIOError::write_logs(&e))?,
                ));
            }
            sink.append(encoded)
                .await
                .map_err(|e| openraft::StorageIOError::write_logs(&std::io::Error::other(e)))?;
        }
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
        if let Some(sink) = self.persist() {
            sink.truncate_since(log_id.index)
                .await
                .map_err(|e| openraft::StorageIOError::write_logs(&std::io::Error::other(e)))?;
        }
        let mut inner = self.lock();
        let keys: Vec<u64> = inner.log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in keys {
            inner.log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        if let Some(sink) = self.persist() {
            let marker = crate::serde_bin::to_vec(&log_id)
                .map_err(|e| openraft::StorageIOError::write_logs(&e))?;
            sink.purge_upto(log_id.index, marker)
                .await
                .map_err(|e| openraft::StorageIOError::write_logs(&std::io::Error::other(e)))?;
        }
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
        let payload: SnapshotPayload = crate::serde_bin::from_slice(&data)
            .map_err(|e| openraft::StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let (blob, persist, old_blob) = {
            let mut inner = self.lock();
            // Restore INTO the existing state, never into `S::default()`:
            // the snapshot does not carry node-local configuration (a paged
            // queue's spill store), so a default-rebuilt state would silently
            // drop paging — unbounded RSS from then on — and leak the old
            // state's spill segments without releasing them. The in-place
            // restore releases old bodies as it replaces them. On failure the
            // member goes Fatal (openraft) with a partially-cleared state;
            // restart recovers from disk.
            inner
                .state
                .restore_snapshot(&payload.state_bytes)
                .map_err(|e| {
                    openraft::StorageIOError::read_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e),
                    )
                })?;
            inner.last_applied = payload.last_applied;
            inner.last_membership = payload.last_membership;
            let blob = match &inner.snapshot_dir {
                Some(dir) => {
                    let path = dir.join(format!("snapshot-{}.bin", meta.snapshot_id));
                    match write_blob_durably(dir, &path, &data) {
                        Ok(()) => SnapshotBlob::File(path),
                        Err(e) => {
                            // Degraded but safe: the blob rides inline through
                            // the sink instead (and the old file survives
                            // until the new pointer is durable).
                            tracing::warn!(error = %e, "snapshot blob write failed; keeping blob in memory");
                            SnapshotBlob::Memory(data)
                        }
                    }
                }
                None => SnapshotBlob::Memory(data),
            };
            // The old blob file is NOT deleted here: a crash between deleting
            // it and durably recording the new pointer would leave recovery
            // pointing at nothing — an empty state below a durable purge
            // marker. Deletion waits until the new pointer is durable.
            let old_blob = match &inner.snapshot {
                Some((_, SnapshotBlob::File(old))) if !matches!(&blob, SnapshotBlob::File(new) if new == old) => {
                    Some(old.clone())
                }
                _ => None,
            };
            inner.snapshot = Some((meta.clone(), blob.clone()));
            (blob, inner.persist.clone(), old_blob)
        };
        // Record the installed snapshot durably (recovery restarts from it).
        if let Some(sink) = persist {
            let encoded = crate::serde_bin::to_vec(meta)
                .map_err(|e| openraft::StorageIOError::write_snapshot(None, &e))?;
            let persist_blob = match &blob {
                SnapshotBlob::Memory(bytes) => SnapshotPersist::Inline(bytes.clone()),
                SnapshotBlob::File(path) => SnapshotPersist::File(path.clone()),
            };
            sink.save_snapshot(encoded, persist_blob)
                .await
                .map_err(|e| {
                    openraft::StorageIOError::write_snapshot(None, &std::io::Error::other(e))
                })?;
        }
        // Only now — with the new snapshot durably recorded — may the
        // previous blob go away.
        if let Some(old) = old_blob {
            let _ = std::fs::remove_file(&old);
        }
        Ok(())
    }

    async fn get_current_snapshot(&mut self) -> Result<Option<Snapshot<C>>, StorageError<NodeId>> {
        let snapshot = self.lock().snapshot.clone();
        match snapshot {
            Some((meta, blob)) => {
                let data = blob.read().map_err(|e| {
                    openraft::StorageIOError::read_snapshot(Some(meta.signature()), &e)
                })?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(Cursor::new(data)),
                }))
            }
            None => Ok(None),
        }
    }
}

#[cfg(all(test, feature = "store-redb"))]
mod tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use openraft::Config;

    use super::super::MetaRaft;
    use super::super::meta::{MetaCommand, QueueSpec, QueueType};
    use super::super::network::UnreachableNetwork;
    use super::*;

    /// A metadata snapshot written before exchanges existed (0.9.x: the
    /// catalog alone) still restores after an in-place upgrade; a current
    /// snapshot round-trips with its exchange state.
    #[test]
    fn meta_snapshot_restores_legacy_and_current_formats() {
        #[derive(Serialize)]
        struct Legacy {
            catalog: BTreeMap<String, QueueSpec>,
        }
        let spec = QueueSpec {
            queue_type: QueueType::Quorum,
            replicas: 3,
            placement: vec![1, 2, 3],
        };
        let legacy = crate::serde_bin::to_vec(&Legacy {
            catalog: BTreeMap::from([("orders".to_owned(), spec.clone())]),
        })
        .unwrap();
        // Until exchanges are used, a snapshot keeps the legacy encoding so
        // an older node can still install it (review M2).
        let unused = MetaState {
            catalog: BTreeMap::from([("orders".to_owned(), spec.clone())]),
            ..Default::default()
        };
        assert_eq!(unused.snapshot_bytes().unwrap(), legacy);
        let mut state = MetaState::default();
        state.restore_snapshot(&legacy).expect("legacy restores");
        assert_eq!(state.catalog.get("orders"), Some(&spec));
        assert!(state.exchanges.is_empty());

        let op = crate::exchange::ExchangeOp::Declare {
            vhost: String::new(),
            name: "events".into(),
            kind: crate::exchange::ExchangeKind::Topic,
            alternate: None,
        };
        assert_eq!(
            state.apply(&MetaCommand::Exchange(op)),
            MetaResponse::Exchange(Ok((true, 1)))
        );
        let bytes = state.snapshot_bytes().unwrap();
        let mut back = MetaState::default();
        back.restore_snapshot(&bytes).unwrap();
        assert_eq!(back.exchanges_epoch, 1);
        assert!(back.exchanges.contains_key("/events"));
        assert_eq!(back.catalog.get("orders"), Some(&spec));

        // Exchanges but no deletes yet: the 0.10.0 encoding (no
        // incarnations field), still readable by a 0.10.0 node.
        #[derive(serde::Deserialize)]
        struct V2 {
            catalog: BTreeMap<String, QueueSpec>,
            exchanges: crate::exchange::Defs,
            exchanges_epoch: u64,
        }
        let v2: V2 = crate::serde_bin::from_slice(&bytes).expect("0.10.0 shape");
        assert_eq!(
            (v2.catalog.len(), v2.exchanges.len(), v2.exchanges_epoch),
            (1, 1, 1)
        );

        // A delete moves the name to its next incarnation; the snapshot
        // then carries the incarnations and round-trips.
        assert_eq!(
            state.apply(&MetaCommand::DeleteQueue {
                name: "orders".into()
            }),
            MetaResponse::Deleted
        );
        assert_eq!(state.incarnations.get("orders"), Some(&1));
        assert_eq!(
            state.apply(&MetaCommand::DeleteQueue {
                name: "orders".into()
            }),
            MetaResponse::NotFound,
            "deleting again changes nothing"
        );
        assert_eq!(state.incarnations.get("orders"), Some(&1));
        let bytes = state.snapshot_bytes().unwrap();
        let mut back = MetaState::default();
        back.restore_snapshot(&bytes).unwrap();
        assert_eq!(back.incarnations.get("orders"), Some(&1));
        assert!(back.catalog.is_empty());
        assert!(back.exchanges.contains_key("/events"));

        // No runtime definitions yet: the 0.11.0 encoding (a 0.11.0 node
        // can install it).
        #[derive(serde::Deserialize)]
        struct V3 {
            catalog: BTreeMap<String, QueueSpec>,
            exchanges: crate::exchange::Defs,
            exchanges_epoch: u64,
            incarnations: BTreeMap<String, u64>,
        }
        let v3: V3 = crate::serde_bin::from_slice(&bytes).expect("0.11.0 shape");
        assert_eq!(
            (
                v3.catalog.len(),
                v3.exchanges.len(),
                v3.exchanges_epoch,
                v3.incarnations.len()
            ),
            (0, 1, 1, 1)
        );

        // Runtime definitions: set, unchanged re-set, remove; then the
        // snapshot carries them and round-trips.
        let def = |key: &str, value: Option<&[u8]>| MetaCommand::Def {
            key: key.into(),
            value: value.map(<[u8]>::to_vec),
        };
        assert_eq!(
            state.apply(&def("user:a", Some(b"1"))),
            MetaResponse::Def(true, 1)
        );
        assert_eq!(
            state.apply(&def("user:a", Some(b"1"))),
            MetaResponse::Def(false, 1)
        );
        assert_eq!(
            state.apply(&def("user:b", Some(b"2"))),
            MetaResponse::Def(true, 2)
        );
        assert_eq!(
            state.apply(&def("user:b", None)),
            MetaResponse::Def(true, 3)
        );
        assert_eq!(
            state.apply(&def("user:b", None)),
            MetaResponse::Def(false, 3)
        );
        let bytes = state.snapshot_bytes().unwrap();
        assert!(
            crate::serde_bin::from_slice::<V3>(&bytes).is_err(),
            "newer encoding"
        );
        let mut back = MetaState::default();
        back.restore_snapshot(&bytes).unwrap();
        assert_eq!(back.defs.get("user:a").map(Vec::as_slice), Some(&b"1"[..]));
        assert_eq!((back.defs.len(), back.defs_epoch), (1, 3));
        assert_eq!(back.incarnations.get("orders"), Some(&1));
    }

    /// Reopen the redb store, waiting out the writer thread's lock release
    /// (a real restart is a process boundary).
    fn reopen_store(path: &std::path::Path) -> crate::store::Store {
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            match crate::store::Store::open(path) {
                Ok(store) => return store,
                Err(e) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "database lock never released: {e}"
                    );
                    std::thread::sleep(Duration::from_millis(20));
                }
            }
        }
    }

    fn meta_config() -> std::sync::Arc<Config> {
        std::sync::Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                // Aggressive compaction so the test crosses the
                // snapshot-then-purge boundary quickly.
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(50),
                max_in_snapshot_log_to_keep: 10,
                purge_batch_size: 50,
                ..Default::default()
            }
            .validate()
            .expect("valid config"),
        )
    }

    /// The CRIT-3 regression (issue #19): once openraft durably purges the
    /// log behind a snapshot, that snapshot is the ONLY copy of the state —
    /// for the metadata group its blob lived purely in memory, so a restart
    /// recovered an empty catalog with a purged log (silent loss of every
    /// queue definition). Inline snapshot persistence closes this.
    #[tokio::test]
    async fn meta_snapshot_survives_restart_after_log_purge() {
        const WRITES: usize = 200;
        let dir = tempfile::tempdir().expect("tempdir");

        // First life: enough catalog writes to trigger snapshot + purge.
        {
            let store = crate::store::Store::open(dir.path()).expect("open");
            let (sink, recovery) =
                crate::cluster::store::RaftPersistFactory::open_group(&store, "meta")
                    .expect("open group");
            let meta_store = MetaStore::new_persistent(MetaState::default(), None, sink, recovery)
                .expect("fresh persistent store");
            let (log_store, state_machine) = openraft::storage::Adaptor::new(meta_store.clone());
            let raft = MetaRaft::new(
                1,
                meta_config(),
                UnreachableNetwork,
                log_store,
                state_machine,
            )
            .await
            .expect("raft");
            raft.initialize(BTreeMap::from([(1u64, BasicNode::new("local"))]))
                .await
                .expect("initialize");
            raft.wait(Some(Duration::from_secs(5)))
                .current_leader(1, "self-elect")
                .await
                .expect("leader");

            for i in 0..WRITES {
                raft.client_write(MetaCommand::CreateQueue {
                    name: format!("q{i:03}"),
                    spec: QueueSpec {
                        queue_type: QueueType::Quorum,
                        replicas: 1,
                        placement: vec![1],
                    },
                })
                .await
                .expect("catalog write");
            }
            // Compaction runs asynchronously; wait for the purge to land.
            let deadline = std::time::Instant::now() + Duration::from_secs(10);
            while meta_store.log_stats().1.is_none() {
                assert!(
                    std::time::Instant::now() < deadline,
                    "log never purged; snapshot policy did not fire"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            raft.shutdown().await.expect("shutdown");
        }

        // Second life: recover from disk. The purged prefix exists ONLY in
        // the snapshot now — without inline blob persistence the catalog
        // came back empty.
        let store = reopen_store(dir.path());
        let (sink, recovery) =
            crate::cluster::store::RaftPersistFactory::open_group(&store, "meta")
                .expect("reopen group");
        assert!(
            recovery.snapshot.is_some(),
            "the snapshot must have been persisted before the log purge"
        );
        let meta_store = MetaStore::new_persistent(MetaState::default(), None, sink, recovery)
            .expect("recovered persistent store");
        let snapshot_catalog = meta_store.catalog();
        assert!(
            snapshot_catalog.contains_key("q000"),
            "the purged prefix must come back from the snapshot"
        );

        // Restart the raft on the recovered store (no initialize — vote and
        // membership recover from disk); replaying the log tail on top of
        // the snapshot must yield the complete catalog.
        let (log_store, state_machine) = openraft::storage::Adaptor::new(meta_store.clone());
        let raft = MetaRaft::new(
            1,
            meta_config(),
            UnreachableNetwork,
            log_store,
            state_machine,
        )
        .await
        .expect("raft restart");
        raft.wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| {
                    m.current_leader == Some(1)
                        && m.last_applied.map(|l| l.index) == m.last_log_index
                },
                "re-elected and caught up",
            )
            .await
            .expect("recovery replay");
        let catalog = meta_store.catalog();
        assert_eq!(
            catalog.len(),
            WRITES,
            "every catalog entry survives the restart"
        );
        raft.shutdown().await.expect("shutdown");
    }
}
