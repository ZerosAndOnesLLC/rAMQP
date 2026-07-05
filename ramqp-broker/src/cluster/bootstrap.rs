//! Cluster formation from a static seed list.
//!
//! Every node starts its metadata Raft and serves the inter-node TCP
//! listener; the node with the **lowest seed id** proposes the initial
//! membership (retrying until a quorum of seeds is reachable), everyone else
//! simply waits to be contacted. Initializing an already-initialized node is
//! a no-op, so restarts and races are safe.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::InitializeError;
use openraft::{BasicNode, Config};
use serde::{Deserialize, Serialize};

use super::store::MetaStore;
use super::tcp::{TcpNetworkFactory, serve_raft};
use super::{MetaRaft, NodeId};

/// Static cluster membership configuration for one node.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    /// This node's id (must appear in `seeds`).
    pub node_id: NodeId,
    /// The inter-node Raft listen address (e.g. `0.0.0.0:7472`).
    pub raft_listen: String,
    /// All founding members: `(node id, raft address as peers reach it)`.
    pub seeds: Vec<(NodeId, String)>,
}

/// A running metadata-group member.
pub struct ClusterHandle {
    /// The local Raft handle.
    pub raft: MetaRaft,
    /// The local store (applied catalog reads).
    pub store: MetaStore,
    /// The bound inter-node listen address (useful with port `0`).
    pub raft_addr: std::net::SocketAddr,
}

impl std::fmt::Debug for ClusterHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterHandle")
            .field("raft_addr", &self.raft_addr)
            .finish_non_exhaustive()
    }
}

/// Start this node's metadata-group member and (on the designated
/// bootstrapper) form the cluster.
///
/// Returns as soon as the local Raft is serving; formation continues in the
/// background until a quorum of seeds is up. Use
/// [`ClusterHandle::await_membership`] to block until the cluster is formed.
pub async fn bootstrap(config: ClusterConfig) -> std::io::Result<ClusterHandle> {
    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .map_err(std::io::Error::other)?,
    );
    let store = MetaStore::default();
    let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
    let raft = MetaRaft::new(
        config.node_id,
        raft_config,
        TcpNetworkFactory,
        log_store,
        state_machine,
    )
    .await
    .map_err(std::io::Error::other)?;

    let listener = tokio::net::TcpListener::bind(&config.raft_listen).await?;
    let raft_addr = listener.local_addr()?;
    tokio::spawn(serve_raft(listener, raft.clone()));

    // The lowest seed id proposes the initial membership. Retry until enough
    // seeds are reachable; stop as soon as the cluster reports as formed
    // (locally or via a peer that got there first).
    let is_bootstrapper = config
        .seeds
        .iter()
        .map(|(id, _)| *id)
        .min()
        .is_some_and(|min| min == config.node_id);
    if is_bootstrapper {
        let members: BTreeMap<NodeId, BasicNode> = config
            .seeds
            .iter()
            .map(|(id, addr)| (*id, BasicNode::new(addr.clone())))
            .collect();
        let raft = raft.clone();
        tokio::spawn(async move {
            loop {
                match raft.initialize(members.clone()).await {
                    Ok(()) => {
                        tracing::info!("metadata cluster initialized");
                        return;
                    }
                    // Someone (possibly us, before a restart) already formed it.
                    Err(openraft::error::RaftError::APIError(InitializeError::NotAllowed(_))) => {
                        return;
                    }
                    Err(e) => {
                        tracing::debug!(error = %e, "cluster initialize retry");
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        });
    }

    Ok(ClusterHandle {
        raft,
        store,
        raft_addr,
    })
}

impl ClusterHandle {
    /// Wait until the cluster has a leader (formation completed).
    pub async fn await_membership(&self, timeout: Duration) -> Result<NodeId, std::io::Error> {
        let metrics = self
            .raft
            .wait(Some(timeout))
            .metrics(|m| m.current_leader.is_some(), "cluster formed")
            .await
            .map_err(std::io::Error::other)?;
        metrics
            .current_leader
            .ok_or_else(|| std::io::Error::other("no leader after wait"))
    }
}

#[cfg(test)]
mod tests {
    use super::super::meta::{MetaCommand, QueueSpec, QueueType};
    use super::*;

    /// Three nodes bootstrap concurrently from the same seed list (ports
    /// pre-bound so the addresses are known), form a cluster, and replicate.
    #[tokio::test]
    async fn seeds_form_a_cluster() {
        // Reserve three ports by binding :0 listeners, then release them for
        // the nodes to re-bind. (Small race window; fine for a test.)
        let mut addrs = Vec::new();
        for _ in 0..3 {
            let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap().to_string());
            drop(l);
        }
        let seeds: Vec<(NodeId, String)> = (1..=3u64).zip(addrs.iter().cloned()).collect();

        let mut handles = Vec::new();
        for (id, addr) in &seeds {
            let config = ClusterConfig {
                node_id: *id,
                raft_listen: addr.clone(),
                seeds: seeds.clone(),
            };
            handles.push(bootstrap(config).await.expect("bootstrap"));
        }

        // Formation completes on every node.
        let mut leader = None;
        for h in &handles {
            let l = h
                .await_membership(Duration::from_secs(10))
                .await
                .expect("membership");
            leader.get_or_insert(l);
            assert_eq!(Some(l), leader, "nodes disagree on the leader");
        }

        // A write through the leader converges everywhere.
        let leader = leader.expect("leader id");
        let leader_handle = handles
            .iter()
            .find(|h| h.raft.metrics().borrow().id == leader)
            .expect("leader handle");
        leader_handle
            .raft
            .client_write(MetaCommand::CreateQueue {
                name: "seeded".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write");

        for h in &handles {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !h.store.catalog().contains_key("seeded") {
                assert!(std::time::Instant::now() < deadline, "no convergence");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
