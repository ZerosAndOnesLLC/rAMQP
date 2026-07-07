//! The per-queue Raft group: a quorum queue's replicated state machine.
//!
//! Exactly the two state transitions the broker.md §7 design calls for ride
//! the log — **enqueue** and **settle** — so every replica converges on the
//! same message store. Dispatch bookkeeping (which consumer holds which
//! unacked message) is deliberately *not* replicated in this slice: it is
//! leader-local, and a leader failover redelivers whatever was in flight —
//! at-least-once, the same contract the transient queue gives on consumer
//! death. Replicated consumer state (exactly-once-closer semantics) is a
//! later refinement.
//!
//! Encoding note: command bodies are `serde` values (JSON on the wire/in
//! snapshots today). That is control-plane-grade, not data-plane-grade — the
//! binary log/RPC codec lands with the multi-raft manager, before quorum
//! queues are benchmarked (broker.md §3).

use std::collections::BTreeMap;
use std::io::Cursor;

use bytes::Bytes;
use openraft::BasicNode;
use serde::{Deserialize, Serialize};

use super::NodeId;
use super::paging::{Spill, SpillRef};
use super::store::{ReplicatedState, SharedStore};

openraft::declare_raft_types!(
    /// Raft type configuration for one queue group.
    pub QueueTypeConfig:
        D = QueueCommand,
        R = QueueResponse,
        NodeId = NodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// A queue group's Raft handle.
pub type QueueRaft = openraft::Raft<QueueTypeConfig>;

/// A queue group's storage.
pub type QueueStore = SharedStore<QueueTypeConfig, QueueState>;

/// A state transition proposed to a queue group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueCommand {
    /// Store a message.
    Enqueue {
        /// The raw message bytes as received (all sections). `Bytes`, so the
        /// single-replica path never copies the body; the wire codec for
        /// multi-node replication handles its own framing.
        body: Bytes,
    },
    /// Resolve a previously enqueued message.
    Settle {
        /// The id assigned at enqueue.
        msg_id: u64,
        /// `false` → remove (acked/dropped); `true` → keep and count a
        /// failed delivery attempt (requeue).
        requeue: bool,
    },
}

/// The applied result of a [`QueueCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueResponse {
    /// The message was stored under this id.
    Enqueued {
        /// The queue-assigned message id (monotonic per queue).
        msg_id: u64,
    },
    /// The settle was applied.
    Settled,
    /// No such message (already settled, or never existed).
    NotFound,
    /// Non-app log entry (blank/membership).
    Void,
}

/// Where one stored message's bytes live right now.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StoredBody {
    /// In memory.
    Resident(Bytes),
    /// Paged out to this replica's spill store (broker.md §8: deep queues
    /// keep indices resident, not bytes).
    Spilled(SpillRef),
}

impl StoredBody {
    /// The bytes when resident (tests/diagnostics; dispatch goes through
    /// [`QueueState::body_of`]).
    pub fn resident(&self) -> Option<&Bytes> {
        match self {
            StoredBody::Resident(bytes) => Some(bytes),
            StoredBody::Spilled(_) => None,
        }
    }
}

/// One replicated message.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReplicatedMessage {
    /// The message bytes (resident or spilled).
    pub body: StoredBody,
    /// Failed delivery attempts (incremented by requeue settles).
    pub failures: u32,
}

/// Paging knobs for one queue's state machine.
#[derive(Debug, Clone)]
pub struct QueuePaging {
    /// This replica's spill store.
    pub spill: Spill,
    /// Bodies stay resident until this many bytes are held; beyond it,
    /// `apply(enqueue)` spills (FIFO-friendly: the head stays hot).
    pub resident_max_bytes: usize,
}

/// The queue group's replicated state: the ordered message store.
///
/// The *replicated* part is `(next_msg_id, id → (failures, bytes))`; whether
/// a given body is resident or spilled is a local decision each replica
/// makes independently (bodies travel via the log, not via spill files).
#[derive(Debug, Clone, Default)]
pub struct QueueState {
    /// The next message id to assign.
    pub next_msg_id: u64,
    /// Messages by id (BTreeMap keeps FIFO order by assignment).
    pub messages: BTreeMap<u64, ReplicatedMessage>,
    /// Bytes currently held resident.
    resident_bytes: usize,
    /// Paging configuration (`None` → never spill).
    paging: Option<QueuePaging>,
}

/// How a body read resolves: immediately, or via a spill fetch performed
/// outside the store lock.
#[derive(Debug)]
pub enum BodyFetch {
    /// The bytes, refcount-cloned.
    Ready(Bytes),
    /// Read `1` from `0` after releasing the store lock.
    Spilled(Spill, SpillRef),
}

impl QueueState {
    /// A state machine that spills bodies beyond `resident_max_bytes`.
    pub fn paged(spill: Spill, resident_max_bytes: usize) -> Self {
        QueueState {
            paging: Some(QueuePaging {
                spill,
                resident_max_bytes,
            }),
            ..Default::default()
        }
    }

    /// How to read one message's body (resolve [`BodyFetch::Spilled`]
    /// *outside* the store lock).
    pub fn body_of(&self, msg_id: u64) -> Option<BodyFetch> {
        let message = self.messages.get(&msg_id)?;
        Some(match &message.body {
            StoredBody::Resident(bytes) => BodyFetch::Ready(bytes.clone()),
            StoredBody::Spilled(r) => BodyFetch::Spilled(
                self.paging
                    .as_ref()
                    .expect("spilled body implies paging")
                    .spill
                    .clone(),
                *r,
            ),
        })
    }

    /// Diagnostics: bytes currently resident.
    pub fn resident_bytes(&self) -> usize {
        self.resident_bytes
    }

    fn store_body(&mut self, body: &Bytes) -> StoredBody {
        if let Some(paging) = &self.paging
            && self.resident_bytes + body.len() > paging.resident_max_bytes
        {
            match paging.spill.append(body) {
                Ok(r) => return StoredBody::Spilled(r),
                Err(e) => {
                    // Spill failure: fall back to resident (correctness over
                    // the memory bound) and say so loudly.
                    tracing::error!(error = %e, "spill append failed; keeping body resident");
                }
            }
        }
        self.resident_bytes += body.len();
        StoredBody::Resident(body.clone())
    }

    fn drop_body(&mut self, body: &StoredBody) {
        match body {
            StoredBody::Resident(bytes) => {
                self.resident_bytes = self.resident_bytes.saturating_sub(bytes.len());
            }
            StoredBody::Spilled(r) => {
                if let Some(paging) = &self.paging {
                    paging.spill.release(r);
                }
            }
        }
    }
}

/// The portable (snapshot) form of one body.
#[derive(Serialize, Deserialize)]
enum PortableBody {
    /// The bytes travel in the snapshot.
    Inline(Vec<u8>),
    /// The bytes live in this replica's spill store. Only meaningful on the
    /// node that built the snapshot (compaction/restart); installing such a
    /// snapshot on a *different* node fails loudly.
    External(SpillRef),
}

/// The snapshot payload for [`QueueState`].
#[derive(Serialize, Deserialize)]
struct PortableState {
    next_msg_id: u64,
    messages: Vec<(u64, u32, PortableBody)>,
}

impl ReplicatedState for QueueState {
    type Command = QueueCommand;
    type Response = QueueResponse;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        match command {
            QueueCommand::Enqueue { body } => {
                self.next_msg_id += 1;
                let msg_id = self.next_msg_id;
                let stored = self.store_body(body);
                self.messages.insert(
                    msg_id,
                    ReplicatedMessage {
                        body: stored,
                        failures: 0,
                    },
                );
                QueueResponse::Enqueued { msg_id }
            }
            QueueCommand::Settle { msg_id, requeue } => {
                if *requeue {
                    match self.messages.get_mut(msg_id) {
                        Some(m) => {
                            m.failures += 1;
                            QueueResponse::Settled
                        }
                        None => QueueResponse::NotFound,
                    }
                } else if let Some(removed) = self.messages.remove(msg_id) {
                    self.drop_body(&removed.body);
                    QueueResponse::Settled
                } else {
                    QueueResponse::NotFound
                }
            }
        }
    }

    fn void_response() -> Self::Response {
        QueueResponse::Void
    }

    fn prepare_snapshot(&self) {
        // Hold spill-segment deletions while `snapshot_bytes` reads them.
        if let Some(paging) = &self.paging {
            paging.spill.pin();
        }
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>, String> {
        // Balance `prepare_snapshot` on every path.
        struct Unpin<'a>(Option<&'a Spill>);
        impl Drop for Unpin<'_> {
            fn drop(&mut self) {
                if let Some(spill) = self.0 {
                    spill.unpin();
                }
            }
        }
        let _unpin = Unpin(self.paging.as_ref().map(|p| &p.spill));

        let mut messages = Vec::with_capacity(self.messages.len());
        for (id, m) in &self.messages {
            let body = match &m.body {
                StoredBody::Resident(bytes) => PortableBody::Inline(bytes.to_vec()),
                // Keep spilled bodies external: a deep queue's snapshot must
                // not materialize gigabytes (§3.1). Node-local by design —
                // see PortableBody::External.
                StoredBody::Spilled(r) => PortableBody::External(*r),
            };
            messages.push((*id, m.failures, body));
        }
        bincode::serialize(&PortableState {
            next_msg_id: self.next_msg_id,
            messages,
        })
        .map_err(|e| e.to_string())
    }

    fn restore_snapshot(&mut self, bytes: &[u8]) -> Result<(), String> {
        let portable: PortableState = bincode::deserialize(bytes).map_err(|e| e.to_string())?;
        // Keep this state's paging config; rebuild contents.
        for (_, m) in std::mem::take(&mut self.messages) {
            self.drop_body(&m.body);
        }
        self.resident_bytes = 0;
        self.next_msg_id = portable.next_msg_id;
        for (id, failures, body) in portable.messages {
            let stored =
                match body {
                    PortableBody::Inline(bytes) => self.store_body(&Bytes::from(bytes)),
                    PortableBody::External(r) => {
                        // Valid only where the spill lives. Verify readability so
                        // a cross-node install fails here, loudly, instead of at
                        // dispatch time.
                        let paging = self.paging.as_ref().ok_or(
                            "snapshot references spilled bodies but this state has no spill",
                        )?;
                        paging.spill.read(&r).map_err(|e| {
                            format!("snapshot references unreadable spill data: {e}")
                        })?;
                        StoredBody::Spilled(r)
                    }
                };
            self.messages.insert(
                id,
                ReplicatedMessage {
                    body: stored,
                    failures,
                },
            );
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
    use openraft::{BasicNode, Config};

    use super::super::network::UnreachableNetwork;
    use super::*;

    async fn single_node_group() -> (QueueRaft, QueueStore) {
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                ..Default::default()
            }
            .validate()
            .expect("valid config"),
        );
        let store = QueueStore::default();
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let raft = QueueRaft::new(7, config, UnreachableNetwork, log_store, state_machine)
            .await
            .expect("raft");
        raft.initialize(BTreeMap::from([(7u64, BasicNode::new("local"))]))
            .await
            .expect("initialize");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(7, "self-elect")
            .await
            .expect("leader");
        (raft, store)
    }

    /// The paged state machine (broker.md §8's #1 risk): bodies beyond the
    /// resident budget spill to disk, dispatch reads them back, settles
    /// reclaim segment space, and snapshots round-trip locally (spilled
    /// bodies stay external, pinned against concurrent reclamation).
    #[test]
    fn paged_state_bounds_resident_bytes_and_round_trips() {
        let dir = std::env::temp_dir().join(format!("ramqp-paged-test-{}", std::process::id()));
        std::fs::remove_dir_all(&dir).ok();
        let spill = Spill::open(dir.clone()).expect("spill open");
        // Budget: two 8-byte bodies stay resident; the rest spill.
        let mut state = QueueState::paged(spill.clone(), 16);

        for i in 0..10u8 {
            state.apply(&QueueCommand::Enqueue {
                body: Bytes::from(vec![i; 8]),
            });
        }
        assert!(
            state.resident_bytes() <= 16,
            "resident bytes bounded by the budget, got {}",
            state.resident_bytes()
        );
        assert_eq!(state.messages.len(), 10, "index stays fully resident");

        // Reads resolve both ways.
        match state.body_of(1).expect("head") {
            BodyFetch::Ready(bytes) => assert_eq!(&bytes[..], &[0u8; 8]),
            other => panic!("head should be resident, got {other:?}"),
        }
        match state.body_of(10).expect("tail") {
            BodyFetch::Spilled(spill, r) => {
                assert_eq!(&spill.read(&r).expect("spill read")[..], &[9u8; 8]);
            }
            other => panic!("tail should be spilled, got {other:?}"),
        }

        // Snapshot with spilled bodies external; restore locally.
        state.prepare_snapshot();
        let bytes = state.snapshot_bytes().expect("snapshot");
        let mut restored = QueueState::paged(spill.clone(), 16);
        restored.restore_snapshot(&bytes).expect("restore");
        assert_eq!(restored.messages.len(), 10);
        match restored.body_of(10).expect("tail after restore") {
            BodyFetch::Spilled(spill, r) => {
                assert_eq!(&spill.read(&r).expect("read")[..], &[9u8; 8]);
            }
            other => panic!("tail should still be spilled, got {other:?}"),
        }

        // Settling everything releases every spilled body.
        for id in 1..=10u64 {
            state.apply(&QueueCommand::Settle {
                msg_id: id,
                requeue: false,
            });
        }
        assert_eq!(state.resident_bytes(), 0);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[tokio::test]
    async fn enqueue_and_settle_through_the_log() {
        let (raft, store) = single_node_group().await;

        let mut ids = Vec::new();
        for body in [b"m1".as_slice(), b"m2", b"m3"] {
            let resp = raft
                .client_write(QueueCommand::Enqueue {
                    body: Bytes::copy_from_slice(body),
                })
                .await
                .expect("enqueue");
            match resp.data {
                QueueResponse::Enqueued { msg_id } => ids.push(msg_id),
                other => panic!("expected enqueued, got {other:?}"),
            }
        }
        assert_eq!(ids, vec![1, 2, 3]);
        store.with_state(|s| {
            assert_eq!(s.messages.len(), 3);
            assert_eq!(
                &s.messages[&1].body.resident().expect("resident")[..],
                b"m1"
            );
        });

        // Ack removes; requeue counts a failure and keeps the message.
        let resp = raft
            .client_write(QueueCommand::Settle {
                msg_id: 1,
                requeue: false,
            })
            .await
            .expect("settle");
        assert_eq!(resp.data, QueueResponse::Settled);

        let resp = raft
            .client_write(QueueCommand::Settle {
                msg_id: 2,
                requeue: true,
            })
            .await
            .expect("requeue");
        assert_eq!(resp.data, QueueResponse::Settled);

        // Settling an unknown id is a no-op response, not an error.
        let resp = raft
            .client_write(QueueCommand::Settle {
                msg_id: 1,
                requeue: false,
            })
            .await
            .expect("double settle");
        assert_eq!(resp.data, QueueResponse::NotFound);

        store.with_state(|s| {
            assert_eq!(s.messages.len(), 2);
            assert_eq!(s.messages[&2].failures, 1);
            assert!(!s.messages.contains_key(&1));
        });
    }

    /// The Phase 6 headline property at the state-machine level: messages
    /// committed to a 3-replica queue group survive the leader dying
    /// mid-stream — the new leader's store holds every acknowledged enqueue.
    #[tokio::test]
    async fn leader_death_loses_no_committed_message() {
        use super::super::network::Router;

        let router: Router<QueueTypeConfig> = Router::default();
        let mut rafts = BTreeMap::new();
        let mut stores = BTreeMap::new();
        for id in [1u64, 2, 3] {
            let config = Arc::new(
                Config {
                    heartbeat_interval: 50,
                    election_timeout_min: 150,
                    election_timeout_max: 300,
                    ..Default::default()
                }
                .validate()
                .expect("valid config"),
            );
            let store = QueueStore::default();
            let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
            let raft = QueueRaft::new(id, config, router.clone(), log_store, state_machine)
                .await
                .expect("raft");
            router.register(id, raft.clone());
            rafts.insert(id, raft);
            stores.insert(id, store);
        }
        rafts[&1]
            .initialize(
                [1u64, 2, 3]
                    .map(|id| (id, BasicNode::new(format!("n{id}"))))
                    .into_iter()
                    .collect::<BTreeMap<_, _>>(),
            )
            .await
            .expect("initialize");
        let leader = rafts[&1]
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "leader")
            .await
            .expect("election")
            .current_leader
            .expect("leader id");

        // Commit 50 messages through the leader; every one is acknowledged.
        let mut committed = Vec::new();
        for i in 0..50u32 {
            let resp = rafts[&leader]
                .client_write(QueueCommand::Enqueue {
                    body: Bytes::copy_from_slice(&i.to_be_bytes()),
                })
                .await
                .expect("committed enqueue");
            match resp.data {
                QueueResponse::Enqueued { msg_id } => committed.push(msg_id),
                other => panic!("expected enqueued, got {other:?}"),
            }
        }

        // Kill the leader mid-stream.
        rafts[&leader].shutdown().await.expect("leader shutdown");
        router.deregister(leader);

        // A survivor takes over...
        let survivor = *[1u64, 2, 3].iter().find(|&&id| id != leader).unwrap();
        let new_leader = rafts[&survivor]
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.current_leader.is_some_and(|l| l != leader),
                "re-election",
            )
            .await
            .expect("re-election")
            .current_leader
            .expect("new leader");

        // ...the group stays writable...
        rafts[&new_leader]
            .client_write(QueueCommand::Enqueue {
                body: Bytes::from_static(b"after failover"),
            })
            .await
            .expect("post-failover enqueue");

        // ...and ZERO committed messages were lost: the new leader's applied
        // store contains every acknowledged id.
        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        loop {
            let all_present = stores[&new_leader]
                .with_state(|s| committed.iter().all(|id| s.messages.contains_key(id)));
            if all_present {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "committed messages missing on the new leader"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        stores[&new_leader].with_state(|s| {
            assert_eq!(s.messages.len(), 51, "50 committed + 1 post-failover");
            // Content intact, order preserved by id.
            assert_eq!(
                &s.messages[&1].body.resident().expect("resident")[..],
                &0u32.to_be_bytes()
            );
            assert_eq!(
                &s.messages[&50].body.resident().expect("resident")[..],
                &49u32.to_be_bytes()
            );
        });
    }

    /// With a snapshot policy configured, the in-memory log is compacted as
    /// entries apply — log memory tracks queue depth, not total messages
    /// ever enqueued (broker.md §3.2).
    #[tokio::test]
    async fn snapshot_policy_purges_the_log() {
        let config = Arc::new(
            Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1_000),
                max_in_snapshot_log_to_keep: 100,
                purge_batch_size: 500,
                ..Default::default()
            }
            .validate()
            .expect("valid config"),
        );
        let store = QueueStore::default();
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let raft = QueueRaft::new(9, config, UnreachableNetwork, log_store, state_machine)
            .await
            .expect("raft");
        raft.initialize(BTreeMap::from([(9u64, BasicNode::new("local"))]))
            .await
            .expect("initialize");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(9, "self-elect")
            .await
            .expect("leader");

        // Enqueue + immediately settle 12k messages (24k log entries), far
        // past the 1k snapshot threshold.
        for i in 0..12_000u32 {
            let resp = raft
                .client_write(QueueCommand::Enqueue {
                    body: Bytes::copy_from_slice(&i.to_be_bytes()),
                })
                .await
                .expect("enqueue");
            let QueueResponse::Enqueued { msg_id } = resp.data else {
                panic!("expected enqueued");
            };
            raft.client_write(QueueCommand::Settle {
                msg_id,
                requeue: false,
            })
            .await
            .expect("settle");
        }

        // Compaction runs asynchronously; wait for the purge to land.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        loop {
            let (log_len, last_purged) = store.log_stats();
            if last_purged.is_some() && log_len < 5_000 {
                break;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "log never compacted: {log_len} entries held, purged={last_purged:?}"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        // The applied state is empty (everything settled) regardless of
        // how much log was kept.
        store.with_state(|s| assert!(s.messages.is_empty()));
    }

    #[tokio::test]
    async fn snapshot_round_trips_the_message_store() {
        let (raft, store) = single_node_group().await;
        for i in 0..5u8 {
            raft.client_write(QueueCommand::Enqueue {
                body: Bytes::copy_from_slice(&[i]),
            })
            .await
            .expect("enqueue");
        }

        // Build a snapshot from the live store, install it into a fresh one.
        let mut source = store.clone();
        let snapshot = source.build_snapshot().await.expect("snapshot");
        let mut fresh = QueueStore::default();
        fresh
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .expect("install");
        fresh.with_state(|s| {
            assert_eq!(s.messages.len(), 5);
            assert_eq!(s.next_msg_id, 5);
            assert_eq!(&s.messages[&3].body.resident().expect("resident")[..], &[2]);
        });
    }
}
