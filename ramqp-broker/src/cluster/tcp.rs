//! TCP inter-node transport for the metadata Raft group.
//!
//! Wire format: length-prefixed (`u32` big-endian) serde_json envelopes, one
//! request/response in flight per connection — openraft drives one
//! replication stream per peer, so serial RPC per connection is the natural
//! shape. JSON keeps the control plane debuggable; the per-queue data-plane
//! groups (Phase 6 multi-raft) get the binary codec and connection sharing.
//!
//! Servers [`serve_raft`] a listener and dispatch to the local Raft; clients
//! ([`TcpNetworkFactory`]) lazily connect to the peer address carried in its
//! [`BasicNode`] and reconnect on failure — openraft's replication layer
//! handles retry/backoff on `Unreachable`.

use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use openraft::BasicNode;
use openraft::error::{RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::{MetaRaft, MetaTypeConfig, NodeId};

/// A Raft RPC request envelope.
#[derive(Debug, Serialize, Deserialize)]
enum Rpc {
    AppendEntries(AppendEntriesRequest<MetaTypeConfig>),
    Vote(VoteRequest<NodeId>),
    InstallSnapshot(InstallSnapshotRequest<MetaTypeConfig>),
}

/// A Raft RPC response envelope (already `Result`-shaped: remote Raft errors
/// travel as their serde forms).
#[derive(Debug, Serialize, Deserialize)]
enum Reply {
    AppendEntries(Result<AppendEntriesResponse<NodeId>, RaftError<NodeId>>),
    Vote(Result<VoteResponse<NodeId>, RaftError<NodeId>>),
    InstallSnapshot(
        Result<
            InstallSnapshotResponse<NodeId>,
            RaftError<NodeId, openraft::error::InstallSnapshotError>,
        >,
    ),
}

/// Frame cap for control-plane RPCs (a snapshot chunk rides inside; 64 MiB
/// is far above any sane metadata snapshot and still a hard bound).
const MAX_RPC_FRAME: u32 = 64 * 1024 * 1024;

async fn write_frame<T: Serialize>(
    stream: &mut TcpStream,
    value: &T,
) -> Result<(), std::io::Error> {
    let body = serde_json::to_vec(value).map_err(std::io::Error::other)?;
    let len = u32::try_from(body.len()).map_err(std::io::Error::other)?;
    if len > MAX_RPC_FRAME {
        return Err(std::io::Error::other("raft rpc frame too large"));
    }
    stream.write_all(&len.to_be_bytes()).await?;
    stream.write_all(&body).await?;
    stream.flush().await
}

async fn read_frame<T: for<'de> Deserialize<'de>>(
    stream: &mut TcpStream,
) -> Result<T, std::io::Error> {
    let mut len = [0u8; 4];
    stream.read_exact(&mut len).await?;
    let len = u32::from_be_bytes(len);
    if len > MAX_RPC_FRAME {
        return Err(std::io::Error::other("raft rpc frame too large"));
    }
    let mut body = vec![0u8; len as usize];
    stream.read_exact(&mut body).await?;
    serde_json::from_slice(&body).map_err(std::io::Error::other)
}

/// Accept inter-node connections and dispatch RPCs into the local Raft.
/// Runs until the listener errors or the task is dropped.
pub async fn serve_raft(listener: TcpListener, raft: MetaRaft) -> std::io::Result<()> {
    loop {
        let (mut stream, peer) = listener.accept().await?;
        let _ = stream.set_nodelay(true);
        let raft = raft.clone();
        tokio::spawn(async move {
            loop {
                let rpc: Rpc = match read_frame(&mut stream).await {
                    Ok(rpc) => rpc,
                    Err(_) => break, // peer closed / garbage: drop the conn
                };
                let reply = match rpc {
                    Rpc::AppendEntries(req) => Reply::AppendEntries(raft.append_entries(req).await),
                    Rpc::Vote(req) => Reply::Vote(raft.vote(req).await),
                    Rpc::InstallSnapshot(req) => {
                        Reply::InstallSnapshot(raft.install_snapshot(req).await)
                    }
                };
                if write_frame(&mut stream, &reply).await.is_err() {
                    break;
                }
            }
            tracing::trace!(%peer, "raft peer connection closed");
        });
    }
}

/// Creates lazily-connecting TCP clients to peers (addresses come from each
/// peer's [`BasicNode::addr`]).
#[derive(Debug, Default, Clone)]
pub struct TcpNetworkFactory;

impl RaftNetworkFactory<MetaTypeConfig> for TcpNetworkFactory {
    type Network = TcpConnection;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        TcpConnection {
            target,
            addr: node.addr.clone(),
            stream: None,
        }
    }
}

/// One lazily-(re)connected RPC connection to a peer node.
#[derive(Debug)]
pub struct TcpConnection {
    target: NodeId,
    addr: String,
    stream: Option<TcpStream>,
}

impl TcpConnection {
    async fn call(&mut self, rpc: &Rpc) -> Result<Reply, std::io::Error> {
        if self.stream.is_none() {
            let stream = TcpStream::connect(&self.addr).await?;
            let _ = stream.set_nodelay(true);
            self.stream = Some(stream);
        }
        let stream = self.stream.as_mut().expect("connected above");
        match async {
            write_frame(stream, rpc).await?;
            read_frame::<Reply>(stream).await
        }
        .await
        {
            Ok(reply) => Ok(reply),
            Err(e) => {
                // Poisoned connection: drop it so the next call reconnects.
                self.stream = None;
                Err(e)
            }
        }
    }
}

type NetError<E> = RPCError<NodeId, BasicNode, E>;

fn unreachable<E: std::error::Error>(e: std::io::Error) -> NetError<E> {
    RPCError::Unreachable(Unreachable::new(&e))
}

fn remote<E: std::error::Error>(target: NodeId, e: E) -> NetError<E> {
    RPCError::RemoteError(openraft::error::RemoteError::new(target, e))
}

impl RaftNetwork<MetaTypeConfig> for TcpConnection {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<NodeId>, NetError<RaftError<NodeId>>> {
        match self.call(&Rpc::AppendEntries(rpc)).await {
            Ok(Reply::AppendEntries(Ok(resp))) => Ok(resp),
            Ok(Reply::AppendEntries(Err(e))) => Err(remote(self.target, e)),
            Ok(_) => Err(unreachable(std::io::Error::other("mismatched rpc reply"))),
            Err(e) => Err(unreachable(e)),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<MetaTypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<NodeId>,
        NetError<RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        match self.call(&Rpc::InstallSnapshot(rpc)).await {
            Ok(Reply::InstallSnapshot(Ok(resp))) => Ok(resp),
            Ok(Reply::InstallSnapshot(Err(e))) => Err(remote(self.target, e)),
            Ok(_) => Err(unreachable(std::io::Error::other("mismatched rpc reply"))),
            Err(e) => Err(unreachable(e)),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<NodeId>,
        _option: RPCOption,
    ) -> Result<VoteResponse<NodeId>, NetError<RaftError<NodeId>>> {
        match self.call(&Rpc::Vote(rpc)).await {
            Ok(Reply::Vote(Ok(resp))) => Ok(resp),
            Ok(Reply::Vote(Err(e))) => Err(remote(self.target, e)),
            Ok(_) => Err(unreachable(std::io::Error::other("mismatched rpc reply"))),
            Err(e) => Err(unreachable(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use openraft::{BasicNode, Config};

    use super::super::meta::{MetaCommand, QueueSpec, QueueType};
    use super::super::store::MetaStore;
    use super::super::{MetaRaft, NodeId};
    use super::{TcpNetworkFactory, serve_raft};

    /// Bind a raft listener, spawn a node serving it, return (raft, store, addr).
    async fn spawn_tcp_node(id: NodeId) -> (MetaRaft, MetaStore, String) {
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
        let store = MetaStore::default();
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let raft = MetaRaft::new(id, config, TcpNetworkFactory, log_store, state_machine)
            .await
            .expect("raft node");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind raft port");
        let addr = listener.local_addr().expect("addr").to_string();
        tokio::spawn(serve_raft(listener, raft.clone()));
        (raft, store, addr)
    }

    #[tokio::test]
    async fn three_node_cluster_over_tcp() {
        let (n1, s1, a1) = spawn_tcp_node(1).await;
        let (n2, s2, a2) = spawn_tcp_node(2).await;
        let (n3, s3, a3) = spawn_tcp_node(3).await;

        let members: BTreeMap<NodeId, BasicNode> = [
            (1u64, BasicNode::new(a1)),
            (2, BasicNode::new(a2)),
            (3, BasicNode::new(a3)),
        ]
        .into();
        n1.initialize(members).await.expect("initialize over tcp");
        n1.wait(Some(Duration::from_secs(10)))
            .metrics(|m| m.current_leader.is_some(), "leader elected")
            .await
            .expect("election");

        let handles: BTreeMap<NodeId, MetaRaft> =
            [(1u64, n1.clone()), (2, n2.clone()), (3, n3.clone())].into();
        let leader = n1.metrics().borrow().current_leader.expect("leader");
        handles[&leader]
            .client_write(MetaCommand::CreateQueue {
                name: "tcp-replicated".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                },
            })
            .await
            .expect("write");

        for (id, store) in [(1u64, &s1), (2, &s2), (3, &s3)] {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !store.catalog().contains_key("tcp-replicated") {
                assert!(
                    std::time::Instant::now() < deadline,
                    "node {id} never applied over tcp"
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
    }
}
