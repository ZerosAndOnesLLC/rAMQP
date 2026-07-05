//! Cluster foundation (broker.md Phases 5–6): openraft-based replication.
//!
//! The design (broker.md §7–8): one **metadata Raft group** spanning all
//! nodes owns the queue catalog, placement, and policies; each quorum queue
//! later becomes its own per-queue Raft group multiplexed by a multi-raft
//! manager. This module is the foundation slice — the metadata group's
//! state machine, an in-memory log store, and an in-process network router
//! (the TCP inter-node transport is the next slice; the router also serves
//! multi-node tests without sockets).

pub mod meta;
pub mod network;
pub mod store;
pub mod tcp;

use std::io::Cursor;

use openraft::BasicNode;

use meta::{MetaCommand, MetaResponse};

/// The metadata group's node id.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// Raft type configuration for the metadata group.
    pub MetaTypeConfig:
        D = MetaCommand,
        R = MetaResponse,
        NodeId = NodeId,
        Node = BasicNode,
        SnapshotData = Cursor<Vec<u8>>,
);

/// The metadata group's Raft handle.
pub type MetaRaft = openraft::Raft<MetaTypeConfig>;

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::{BasicNode, Config};

    use super::meta::{MetaCommand, MetaResponse, QueueSpec, QueueType};
    use super::network::Router;
    use super::store::MetaStore;
    use super::{MetaRaft, NodeId};

    async fn spawn_node(id: NodeId, router: &Router) -> (MetaRaft, Arc<MetaStore>) {
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
        let store = Arc::new(MetaStore::default());
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let raft = MetaRaft::new(id, config, router.clone(), log_store, state_machine)
            .await
            .expect("raft node");
        router.register(id, raft.clone());
        (raft, store)
    }

    fn members(ids: &[NodeId]) -> BTreeMap<NodeId, BasicNode> {
        ids.iter()
            .map(|&id| (id, BasicNode::new(format!("node-{id}"))))
            .collect()
    }

    #[tokio::test]
    async fn single_node_metadata_group_applies_commands() {
        let router = Router::default();
        let (raft, store) = spawn_node(1, &router).await;
        raft.initialize(members(&[1])).await.expect("initialize");
        raft.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "self-elect")
            .await
            .expect("leader");

        let resp = raft
            .client_write(MetaCommand::CreateQueue {
                name: "orders".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write");
        assert_eq!(resp.data, MetaResponse::Created);

        // Idempotence: re-creating reports AlreadyExists.
        let resp = raft
            .client_write(MetaCommand::CreateQueue {
                name: "orders".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write");
        assert_eq!(resp.data, MetaResponse::AlreadyExists);

        let catalog = store.catalog();
        assert_eq!(catalog.len(), 1);
        assert_eq!(catalog["orders"].queue_type, QueueType::Quorum);

        let resp = raft
            .client_write(MetaCommand::DeleteQueue {
                name: "orders".into(),
            })
            .await
            .expect("write");
        assert_eq!(resp.data, MetaResponse::Deleted);
        assert!(store.catalog().is_empty());
    }

    #[tokio::test]
    async fn three_node_cluster_replicates_the_catalog() {
        let router = Router::default();
        let (n1, s1) = spawn_node(1, &router).await;
        let (_n2, s2) = spawn_node(2, &router).await;
        let (_n3, s3) = spawn_node(3, &router).await;

        // Form the cluster from node 1 with all three as voters.
        n1.initialize(members(&[1, 2, 3]))
            .await
            .expect("initialize");
        let leader = n1
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("election")
            .current_leader
            .expect("leader id");

        // Write through the leader's handle.
        let leader_raft = router.get(leader).expect("leader handle");
        leader_raft
            .client_write(MetaCommand::CreateQueue {
                name: "replicated".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write on leader");

        // Every node's state machine converges on the same catalog.
        for (id, store) in [(1u64, &s1), (2, &s2), (3, &s3)] {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            loop {
                if store.catalog().contains_key("replicated") {
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "node {id} never applied the catalog entry"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    #[tokio::test]
    async fn cluster_survives_leader_failure() {
        let router = Router::default();
        let (n1, s1) = spawn_node(1, &router).await;
        let (_n2, s2) = spawn_node(2, &router).await;
        let (_n3, s3) = spawn_node(3, &router).await;
        n1.initialize(members(&[1, 2, 3]))
            .await
            .expect("initialize");
        let leader = n1
            .wait(Some(Duration::from_secs(5)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("election")
            .current_leader
            .expect("leader id");

        // Kill the leader: stop its Raft core (no more outbound heartbeats)
        // and make it unreachable (no inbound RPCs).
        let leader_raft = router.get(leader).expect("leader handle");
        leader_raft.shutdown().await.expect("leader shutdown");
        router.deregister(leader);
        let survivor = *[1u64, 2, 3]
            .iter()
            .find(|&&id| id != leader)
            .expect("survivor");
        let survivor_raft = router.get(survivor).expect("survivor handle");

        // The remaining quorum elects a new leader...
        let new_leader = survivor_raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(
                |m| m.current_leader.is_some_and(|l| l != leader),
                "re-election",
            )
            .await
            .expect("re-election")
            .current_leader
            .expect("new leader");
        assert_ne!(new_leader, leader);

        // ...and the catalog stays writable.
        router
            .get(new_leader)
            .expect("new leader handle")
            .client_write(MetaCommand::CreateQueue {
                name: "post-failover".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write after failover");

        // Every surviving node converges on the post-failover write.
        let stores = [(1u64, &s1), (2, &s2), (3, &s3)];
        for (id, store) in stores {
            if id == leader {
                continue; // the killed node cannot converge
            }
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !store.catalog().contains_key("post-failover") {
                assert!(
                    std::time::Instant::now() < deadline,
                    "survivor {id} never applied the post-failover write"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }

    #[tokio::test]
    async fn learner_can_be_added_and_promoted() {
        let router = Router::default();
        let (n1, _s1) = spawn_node(1, &router).await;
        let (_n4, s4) = spawn_node(4, &router).await;

        n1.initialize(members(&[1])).await.expect("initialize");
        n1.wait(Some(Duration::from_secs(5)))
            .current_leader(1, "self-elect")
            .await
            .expect("leader");

        // Add node 4 as a learner, then promote to voter.
        n1.add_learner(4, BasicNode::new("node-4"), true)
            .await
            .expect("add learner");
        n1.change_membership(vec![1, 4], false)
            .await
            .expect("promote");

        n1.client_write(MetaCommand::CreateQueue {
            name: "after-join".into(),
            spec: QueueSpec {
                queue_type: QueueType::Transient,
                replicas: 1,
            },
        })
        .await
        .expect("write");

        let deadline = std::time::Instant::now() + Duration::from_secs(5);
        while !s4.catalog().contains_key("after-join") {
            assert!(
                std::time::Instant::now() < deadline,
                "joined node never caught up"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
}
