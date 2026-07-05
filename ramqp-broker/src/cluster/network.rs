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

use openraft::RaftTypeConfig;

use super::{MetaTypeConfig, NodeId};

/// A network for groups that never speak to peers (single-node groups, e.g.
/// an unreplicated queue group or tests): every RPC reports the peer as
/// unreachable. Generic over the group's type config.
#[derive(Debug, Default, Clone, Copy)]
pub struct UnreachableNetwork;

impl<C> RaftNetworkFactory<C> for UnreachableNetwork
where
    C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    type Network = UnreachableConnection;

    async fn new_client(&mut self, _target: NodeId, _node: &BasicNode) -> Self::Network {
        UnreachableConnection
    }
}

/// The connection type of [`UnreachableNetwork`].
#[derive(Debug)]
pub struct UnreachableConnection;

impl<C> RaftNetwork<C> for UnreachableConnection
where
    C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("single-node group has no peers"),
        )))
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<C>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        RPCError<NodeId, BasicNode, RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("single-node group has no peers"),
        )))
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        Err(RPCError::Unreachable(Unreachable::new(
            &std::io::Error::other("single-node group has no peers"),
        )))
    }
}

/// Routes RPCs to co-located Raft nodes by id. Generic over the group's
/// type config, so it serves the metadata group and queue groups alike.
pub struct Router<C: RaftTypeConfig = MetaTypeConfig> {
    nodes: Arc<RwLock<HashMap<NodeId, openraft::Raft<C>>>>,
}

impl<C: RaftTypeConfig> Default for Router<C> {
    fn default() -> Self {
        Router {
            nodes: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

impl<C: RaftTypeConfig> Clone for Router<C> {
    fn clone(&self) -> Self {
        Router {
            nodes: self.nodes.clone(),
        }
    }
}

impl<C: RaftTypeConfig> std::fmt::Debug for Router<C> {
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

impl<C: RaftTypeConfig> Router<C> {
    /// Make a node reachable under `id`.
    pub fn register(&self, id: NodeId, raft: openraft::Raft<C>) {
        self.nodes.write().expect("router lock").insert(id, raft);
    }

    /// Remove a node (simulates a crash/partition for tests).
    pub fn deregister(&self, id: NodeId) {
        self.nodes.write().expect("router lock").remove(&id);
    }

    /// Look up a node's Raft handle.
    pub fn get(&self, id: NodeId) -> Option<openraft::Raft<C>> {
        self.nodes.read().expect("router lock").get(&id).cloned()
    }
}

impl<C> RaftNetworkFactory<C> for Router<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    type Network = RouterConnection<C>;

    async fn new_client(&mut self, target: NodeId, _node: &BasicNode) -> Self::Network {
        RouterConnection {
            target,
            router: self.clone(),
        }
    }
}

/// A "connection" to one target node through the router.
pub struct RouterConnection<C: RaftTypeConfig = MetaTypeConfig> {
    target: NodeId,
    router: Router<C>,
}

impl<C: RaftTypeConfig> std::fmt::Debug for RouterConnection<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RouterConnection")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}

impl<C: RaftTypeConfig> RouterConnection<C> {
    fn target_raft(&self) -> Result<openraft::Raft<C>, Unreachable> {
        self.router
            .get(self.target)
            .ok_or_else(|| Unreachable::new(&std::io::Error::other("node not registered")))
    }
}

impl<C> RaftNetwork<C> for RouterConnection<C>
where
    C: RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<C>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, BasicNode, RaftError<NodeId>>> {
        let raft = self.target_raft().map_err(RPCError::Unreachable)?;
        raft.append_entries(rpc)
            .await
            .map_err(|e| RPCError::RemoteError(openraft::error::RemoteError::new(self.target, e)))
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<C>,
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
