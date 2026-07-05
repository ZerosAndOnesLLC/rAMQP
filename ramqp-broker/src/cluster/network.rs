//! In-process Raft network: routes RPCs between nodes in the same process.
//!
//! This serves two roles: the multi-node test harness (no sockets), and the
//! seam where the TCP inter-node transport slots in next — `RaftNetwork` is
//! implemented against a router today and against a connection pool then.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use openraft::BasicNode;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::{MetaRaft, MetaTypeConfig, NodeId};

/// Routes RPCs to co-located Raft nodes by id.
#[derive(Default, Clone)]
pub struct Router {
    nodes: Arc<RwLock<HashMap<NodeId, MetaRaft>>>,
}

impl std::fmt::Debug for Router {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let ids: Vec<NodeId> = self
            .nodes
            .read()
            .expect("router lock")
            .keys()
            .copied()
            .collect();
        f.debug_struct("Router").field("nodes", &ids).finish()
    }
}

impl Router {
    /// Make a node reachable under `id`.
    pub fn register(&self, id: NodeId, raft: MetaRaft) {
        self.nodes.write().expect("router lock").insert(id, raft);
    }

    /// Remove a node (simulates a crash/partition for tests).
    pub fn deregister(&self, id: NodeId) {
        self.nodes.write().expect("router lock").remove(&id);
    }

    /// Look up a node's Raft handle.
    pub fn get(&self, id: NodeId) -> Option<MetaRaft> {
        self.nodes.read().expect("router lock").get(&id).cloned()
    }
}

impl RaftNetworkFactory<MetaTypeConfig> for Router {
    type Network = RouterConnection;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        RouterConnection {
            target,
            router: self.clone(),
        }
    }
}

/// A "connection" to one target node through the router.
pub struct RouterConnection {
    target: NodeId,
    router: Router,
}

impl std::fmt::Debug for RouterConnection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterConnection")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl RouterConnection {
    fn target_raft(&self) -> Result<MetaRaft, Unreachable> {
        self.router
            .get(self.target)
            .ok_or_else(|| Unreachable::new(&std::io::Error::other("node not registered")))
    }
}

impl RaftNetwork<MetaTypeConfig> for RouterConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft().map_err(RPCError::Unreachable)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        let raft = self.target_raft().map_err(RPCError::Unreachable)?;
        raft.install_snapshot(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft().map_err(RPCError::Unreachable)?;
        raft.vote(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }
}
