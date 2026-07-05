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

/// One replicated message.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicatedMessage {
    /// The raw message bytes.
    pub body: Bytes,
    /// Failed delivery attempts (incremented by requeue settles).
    pub failures: u32,
}

/// The queue group's replicated state: the ordered message store.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct QueueState {
    /// The next message id to assign.
    pub next_msg_id: u64,
    /// Messages by id (BTreeMap keeps FIFO order by assignment).
    pub messages: BTreeMap<u64, ReplicatedMessage>,
}

impl ReplicatedState for QueueState {
    type Command = QueueCommand;
    type Response = QueueResponse;

    fn apply(&mut self, command: &Self::Command) -> Self::Response {
        match command {
            QueueCommand::Enqueue { body } => {
                self.next_msg_id += 1;
                let msg_id = self.next_msg_id;
                self.messages.insert(
                    msg_id,
                    ReplicatedMessage {
                        body: body.clone(),
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
                } else if self.messages.remove(msg_id).is_some() {
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
            assert_eq!(&s.messages[&1].body[..], b"m1");
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
            assert_eq!(&s.messages[&1].body[..], &0u32.to_be_bytes());
            assert_eq!(&s.messages[&50].body[..], &49u32.to_be_bytes());
        });
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
            assert_eq!(&s.messages[&3].body[..], &[2]);
        });
    }
}
