//! The cluster node: everything one broker process contributes to the
//! cluster — its metadata-group member, its per-queue group members, the
//! leader-local queue actors, and the fabric server that lets peers reach
//! all of them (broker.md §8).
//!
//! Leader routing works in two halves:
//! - **Origin side** (any node an AMQP client connects to): the registry
//!   resolves `/quorum/<name>` to a [`crate::proxy`] actor, which finds the
//!   queue group's leader through this node and forwards publish/subscribe
//!   traffic over the fabric.
//! - **Leader side** (this file's fabric server): forwarded traffic lands on
//!   the local quorum actor exactly as a local connection's would — the queue
//!   actor cannot tell a proxied consumer from a direct one.
//!
//! Placement is rendezvous-hashed over the metadata group's voters at
//! declaration and **recorded in the catalog**, so every node agrees where a
//! queue lives without a second consensus round, and membership churn does
//! not silently migrate queues.

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use openraft::BasicNode;
use openraft::error::{ClientWriteError, RaftError};
use tokio::net::TcpListener;
use tokio::sync::{mpsc, oneshot, watch};

use super::fabric::{
    self, FabricHeader, GroupRef, OutFrame, PublishStatus, RaftKind, RequestKind, spawn_writer,
};
use super::meta::{MetaCommand, MetaResponse, MetaWriteError, QueueSpec, QueueType};
use super::queue_group::{QueueRaft, QueueStore, QueueTypeConfig};
use super::store::{MetaState, MetaStore};
use super::{MetaRaft, MetaTypeConfig, NodeId};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg};
use crate::quorum;

/// Cluster membership settings for one node (mirrors the public
/// [`crate::config::ClusterMemberConfig`], which stays free of cluster
/// internals on the crates.io surface).
#[derive(Debug, Clone)]
pub struct NodeSettings {
    /// This node's id (must appear in `seeds`).
    pub node_id: NodeId,
    /// The fabric listen address (Raft + forwarding, one port).
    pub listen: String,
    /// All founding members: `(node id, fabric address as peers reach it)`.
    pub seeds: Vec<(NodeId, String)>,
    /// Default replica count for newly declared quorum queues (capped at the
    /// current voter count).
    pub replicas: u8,
    /// Per-queue depth bound handed to leader-local actors.
    pub max_queue_depth: usize,
    /// Per-queue byte bound handed to leader-local actors (0 = disabled).
    pub max_queue_bytes: usize,
    /// When set, deep queues page message bodies to disk here and park
    /// snapshot blobs there too (broker.md §8 deep-queue mitigation).
    pub data_dir: Option<std::path::PathBuf>,
    /// Per-queue resident-body budget before paging kicks in.
    pub resident_bytes_max: usize,
    /// Queue policies (prefix-matched; leader-local enforcement). These are
    /// NODE-LOCAL configuration: keep them identical on every node, or a
    /// failover silently changes the policy a queue runs under.
    pub policies: Vec<(String, crate::config::QueuePolicy)>,
    /// The dead-letter router (None in bare-node tests).
    pub dlx: Option<crate::policy::DeadLetterSender>,
    /// On-disk Raft hard-state persistence (`store-redb` + `data_dir`):
    /// groups recover their log/vote/snapshot across restarts.
    pub persist: Option<Arc<dyn super::store::RaftPersistFactory>>,
}

/// One local member of a per-queue Raft group.
struct GroupMember {
    raft: QueueRaft,
    store: QueueStore,
}

/// A running cluster node.
pub struct ClusterNode {
    pub(crate) node_id: NodeId,
    settings: NodeSettings,
    meta: MetaRaft,
    meta_store: MetaStore,
    peers: Arc<fabric::Peers>,
    /// Local members of per-queue groups, keyed by queue name.
    groups: std::sync::Mutex<HashMap<String, Arc<GroupMember>>>,
    /// Leader-local queue actors (spawned only while this node leads).
    actors: std::sync::Mutex<HashMap<String, QueueHandle>>,
    /// The bound fabric address (useful with port `0`).
    pub fabric_addr: std::net::SocketAddr,
    shutdown: watch::Sender<bool>,
    /// Set for good by [`stop`](ClusterNode::stop): no group member may be
    /// (re)created past this point. Without it, a still-open inbound fabric
    /// connection could lazily resurrect an EMPTY member on a dying node —
    /// which then answers the leader's appends with conflicts below its
    /// matched index ("follower log reversion", an openraft panic).
    stopping: std::sync::atomic::AtomicBool,
}

impl std::fmt::Debug for ClusterNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClusterNode")
            .field("node_id", &self.node_id)
            .field("fabric_addr", &self.fabric_addr)
            .finish_non_exhaustive()
    }
}

/// Raft settings for the metadata group.
fn meta_raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        ..Default::default()
    }
}

/// Raft settings for a queue group (compaction keeps log memory tracking
/// queue depth, not total messages ever enqueued — broker.md §3.2).
fn queue_raft_config() -> openraft::Config {
    openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Compaction cadence trades snapshot cost against log memory. A
        // snapshot serializes the whole index (and clones it under the store
        // lock), so at depth its cost scales with queue size — 50k applies
        // per snapshot keeps the log tail ~15 MiB for 256 B bodies while
        // making deep-queue snapshot stalls 10x rarer than the old 5k
        // cadence. Incremental snapshots are the standing follow-up.
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(50_000),
        max_in_snapshot_log_to_keep: 2_000,
        purge_batch_size: 5_000,
        ..Default::default()
    }
}

impl ClusterNode {
    /// Start this node: bind the fabric listener, start the metadata-group
    /// member, and (on the lowest seed id) form the cluster in the
    /// background. Returns as soon as the node is serving.
    pub async fn bootstrap(settings: NodeSettings) -> std::io::Result<Arc<ClusterNode>> {
        // Raft's safety argument assumes votes and log entries survive a
        // restart. Without persistence a restarted voter rejoins EMPTY: it
        // can re-grant a vote in a term it already voted in, electing a
        // leader without entries the old quorum committed — silent loss of
        // acknowledged messages. Supported for tests/ephemeral clusters,
        // but never silently.
        if settings.persist.is_none() {
            tracing::warn!(
                "clustered node has NO on-disk Raft persistence (no data_dir / store-redb): \
                 a node restart can silently lose committed messages (a restarted voter may \
                 vote twice in the same term); configure data_dir with the store-redb feature \
                 for durable clustering"
            );
        }
        let peers = Arc::new(fabric::Peers::default());
        let raft_config = Arc::new(
            meta_raft_config()
                .validate()
                .map_err(std::io::Error::other)?,
        );
        let meta_store = match &settings.persist {
            Some(factory) => {
                let (sink, recovery) = factory.open_group("meta").map_err(std::io::Error::other)?;
                MetaStore::new_persistent(MetaState::default(), None, sink, recovery)
                    .map_err(std::io::Error::other)?
            }
            None => MetaStore::default(),
        };
        let (log_store, state_machine) = openraft::storage::Adaptor::new(meta_store.clone());
        let meta = MetaRaft::new(
            settings.node_id,
            raft_config,
            FabricNetworkFactory {
                peers: peers.clone(),
                group: GroupRef::Meta,
            },
            log_store,
            state_machine,
        )
        .await
        .map_err(std::io::Error::other)?;

        let listener = TcpListener::bind(&settings.listen).await?;
        let fabric_addr = listener.local_addr()?;
        // The fabric speaks unauthenticated, unencrypted frames: any host
        // with TCP reach can publish/consume/settle on every queue this node
        // leads, rewrite the replicated catalog, and feed forged Raft RPCs
        // into every group (a high-term Vote alone is a cluster-wide
        // liveness DoS). Until fabric auth/TLS lands it MUST be confined to
        // an isolated, trusted network — say so loudly on any other bind.
        if !fabric_addr.ip().is_loopback() {
            tracing::warn!(
                addr = %fabric_addr,
                "cluster fabric is listening on a non-loopback address with NO authentication \
                 or encryption — any host that can reach this port has full control of this \
                 node's queues, catalog, and Raft groups; run the fabric only on an isolated \
                 trusted network (firewall/VPC/WireGuard)"
            );
        }
        let (shutdown, shutdown_rx) = watch::channel(false);

        let node = Arc::new(ClusterNode {
            node_id: settings.node_id,
            meta,
            meta_store,
            peers,
            groups: std::sync::Mutex::new(HashMap::new()),
            actors: std::sync::Mutex::new(HashMap::new()),
            fabric_addr,
            shutdown,
            settings,
            stopping: std::sync::atomic::AtomicBool::new(false),
        });

        tokio::spawn(serve_fabric(listener, node.clone(), shutdown_rx));

        // The lowest seed id proposes the initial membership, retrying until
        // a quorum of seeds is reachable. Initializing an already-initialized
        // cluster is a no-op, so restarts and races are safe.
        let seeds = node.settings.seeds.clone();
        let is_bootstrapper = seeds
            .iter()
            .map(|(id, _)| *id)
            .min()
            .is_some_and(|min| min == node.node_id);
        if is_bootstrapper {
            let members: BTreeMap<NodeId, BasicNode> = seeds
                .iter()
                .map(|(id, addr)| (*id, BasicNode::new(addr.clone())))
                .collect();
            let meta = node.meta.clone();
            tokio::spawn(async move {
                loop {
                    match meta.initialize(members.clone()).await {
                        Ok(()) => {
                            tracing::info!("metadata cluster initialized");
                            return;
                        }
                        Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(
                            _,
                        ))) => return,
                        Err(e) => {
                            tracing::debug!(error = %e, "cluster initialize retry");
                            tokio::time::sleep(Duration::from_millis(250)).await;
                        }
                    }
                }
            });
        }
        Ok(node)
    }

    /// Wait until the metadata cluster has a leader (formation completed).
    pub async fn await_membership(&self, timeout: Duration) -> std::io::Result<NodeId> {
        let metrics = self
            .meta
            .wait(Some(timeout))
            .metrics(|m| m.current_leader.is_some(), "cluster formed")
            .await
            .map_err(std::io::Error::other)?;
        metrics
            .current_leader
            .ok_or_else(|| std::io::Error::other("no leader after wait"))
    }

    /// Whether [`stop`](ClusterNode::stop) has begun (proxies abort their
    /// leader-rebind loops instead of spinning against a dead node).
    pub(crate) fn is_stopping(&self) -> bool {
        self.stopping.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Stop this node: fabric listener, every Raft member, every actor.
    /// Abrupt by design (the kill-the-leader path); no draining.
    pub async fn stop(&self) {
        self.stopping
            .store(true, std::sync::atomic::Ordering::Release);
        let _ = self.shutdown.send(true);
        self.actors.lock().expect("actors lock").clear();
        let members: Vec<Arc<GroupMember>> = {
            let mut groups = self.groups.lock().expect("groups lock");
            groups.drain().map(|(_, m)| m).collect()
        };
        for member in members {
            let _ = member.raft.shutdown().await;
        }
        let _ = self.meta.shutdown().await;
    }

    /// A peer's fabric address: founding seeds first, then live membership.
    fn addr_of(&self, id: NodeId) -> Option<String> {
        if let Some((_, addr)) = self.settings.seeds.iter().find(|(sid, _)| *sid == id) {
            return Some(addr.clone());
        }
        let metrics = self.meta.metrics().borrow().clone();
        metrics
            .membership_config
            .nodes()
            .find(|(nid, _)| **nid == id)
            .map(|(_, n)| n.addr.clone())
    }

    /// Current metadata voters with addresses.
    fn voters(&self) -> Vec<(NodeId, String)> {
        let metrics = self.meta.metrics().borrow().clone();
        let membership = metrics.membership_config;
        let voter_ids: Vec<NodeId> = membership.membership().voter_ids().collect();
        voter_ids
            .into_iter()
            .filter_map(|id| {
                membership
                    .membership()
                    .get_node(&id)
                    .map(|n| (id, n.addr.clone()))
            })
            .collect()
    }

    /// Declare (or look up) a quorum queue and make sure its group is
    /// running. Returns the authoritative spec.
    pub async fn declare_quorum(self: &Arc<Self>, name: &str) -> Result<QueueSpec, String> {
        if let Some(spec) = self.meta_store.catalog().get(name).cloned() {
            self.ensure_group(name, &spec).await?;
            return Ok(spec);
        }

        // New queue: place it over the current voters and record the
        // placement in the catalog.
        let voters = self.voters();
        if voters.is_empty() {
            return Err("cluster not formed yet (no voters)".to_owned());
        }
        let want = usize::from(self.settings.replicas.max(1)).min(voters.len());
        let ids: Vec<NodeId> = voters.iter().map(|(id, _)| *id).collect();
        let placement = rendezvous_placement(name, &ids, want);
        let spec = QueueSpec {
            queue_type: QueueType::Quorum,
            replicas: self.settings.replicas,
            placement,
        };
        let response = self
            .meta_write(MetaCommand::CreateQueue {
                name: name.to_owned(),
                spec: spec.clone(),
            })
            .await?;
        let spec = match response {
            MetaResponse::Created => spec,
            // Raced another declarer: adopt the winner's placement.
            MetaResponse::AlreadyExists(existing) => existing,
            other => return Err(format!("unexpected catalog response: {other:?}")),
        };
        self.ensure_group(name, &spec).await?;
        Ok(spec)
    }

    /// Make sure the queue's Raft group is running on its placement nodes
    /// and has a leader.
    async fn ensure_group(self: &Arc<Self>, name: &str, spec: &QueueSpec) -> Result<(), String> {
        let members: Vec<(NodeId, String)> = spec
            .placement
            .iter()
            .map(|id| {
                self.addr_of(*id)
                    .map(|addr| (*id, addr))
                    .ok_or_else(|| format!("no address for placement node {id}"))
            })
            .collect::<Result<_, _>>()?;

        // Start every member (self locally, peers over the fabric). Failures
        // are tolerated as long as a quorum forms — the lazy heal on inbound
        // Raft RPCs covers stragglers.
        let mut starts = Vec::new();
        for (id, _addr) in &members {
            if *id == self.node_id {
                self.start_local_member(name, &members).await?;
            } else {
                let node = self.clone();
                let name = name.to_owned();
                let members = members.clone();
                let id = *id;
                starts.push(tokio::spawn(async move {
                    node.start_remote_member(id, &name, &members).await
                }));
            }
        }
        for start in starts {
            if let Ok(Err(e)) = start.await {
                tracing::debug!(queue = %name, error = %e, "remote member start failed (lazy heal will retry)");
            }
        }
        self.wait_group_leader(name, spec, Duration::from_secs(10))
            .await
            .map(|_| ())
    }

    async fn start_remote_member(
        &self,
        id: NodeId,
        name: &str,
        members: &[(NodeId, String)],
    ) -> Result<(), String> {
        let addr = members
            .iter()
            .find(|(mid, _)| *mid == id)
            .map(|(_, a)| a.clone())
            .ok_or("member address missing")?;
        let conn = self
            .peers
            .client(id, &addr)
            .conn()
            .await
            .map_err(|e| e.to_string())?;
        conn.call(
            RequestKind::StartGroup {
                queue: name.to_owned(),
                members: members.to_vec(),
            },
            Bytes::new(),
        )
        .await
        .map(|_| ())
    }

    /// Create (idempotently) this node's member of a queue group. The lowest
    /// member id also proposes the group's initial membership.
    pub(crate) async fn start_local_member(
        &self,
        name: &str,
        members: &[(NodeId, String)],
    ) -> Result<(), String> {
        if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
            return Err("node is stopping".to_owned());
        }
        if !members.iter().any(|(id, _)| *id == self.node_id) {
            return Err(format!("node {} not in placement for {name}", self.node_id));
        }
        if self.groups.lock().expect("groups lock").contains_key(name) {
            return Ok(());
        }
        let config = Arc::new(queue_raft_config().validate().map_err(|e| e.to_string())?);
        let store = paged_queue_store(
            self.settings.data_dir.as_deref(),
            name,
            self.settings.resident_bytes_max,
            self.settings.persist.as_ref(),
        )?;
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
        let raft = QueueRaft::new(
            self.node_id,
            config,
            FabricNetworkFactory {
                peers: self.peers.clone(),
                group: GroupRef::Queue(name.to_owned()),
            },
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| e.to_string())?;
        enum Insert {
            Inserted,
            Raced,
            Stopping,
        }
        let outcome = {
            let mut groups = self.groups.lock().expect("groups lock");
            // Re-check `stopping` under the SAME lock stop() drains under:
            // the entry check races a concurrent stop(), and inserting a
            // fresh EMPTY member after the drain resurrects it on a dying
            // node — whose conflict replies below its previously-acked
            // matched index panic the leader ("follower log reversion", the
            // exact failure the flag exists to prevent).
            if self.stopping.load(std::sync::atomic::Ordering::Acquire) {
                Insert::Stopping
            } else if groups.contains_key(name) {
                // Raced another creator: keep the first, drop ours.
                Insert::Raced
            } else {
                groups.insert(
                    name.to_owned(),
                    Arc::new(GroupMember {
                        raft: raft.clone(),
                        store,
                    }),
                );
                Insert::Inserted
            }
        };
        match outcome {
            Insert::Inserted => {}
            Insert::Raced => {
                let _ = raft.shutdown().await;
                return Ok(());
            }
            Insert::Stopping => {
                let _ = raft.shutdown().await;
                return Err("node is stopping".to_owned());
            }
        }

        let is_bootstrapper = members
            .iter()
            .map(|(id, _)| *id)
            .min()
            .is_some_and(|min| min == self.node_id);
        if is_bootstrapper {
            let membership: BTreeMap<NodeId, BasicNode> = members
                .iter()
                .map(|(id, addr)| (*id, BasicNode::new(addr.clone())))
                .collect();
            let queue = name.to_owned();
            tokio::spawn(async move {
                loop {
                    match raft.initialize(membership.clone()).await {
                        Ok(()) => {
                            tracing::debug!(queue = %queue, "queue group initialized");
                            return;
                        }
                        Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(
                            _,
                        ))) => return,
                        Err(RaftError::Fatal(_)) => return,
                        Err(e) => {
                            tracing::trace!(queue = %queue, error = %e, "group initialize retry");
                            tokio::time::sleep(Duration::from_millis(200)).await;
                        }
                    }
                }
            });
        }
        Ok(())
    }

    /// The local member of a queue group, lazily healed from the catalog
    /// (covers restarts and members that missed the StartGroup fanout).
    async fn group_member(&self, name: &str) -> Option<Arc<GroupMember>> {
        if let Some(member) = self.groups.lock().expect("groups lock").get(name) {
            return Some(member.clone());
        }
        let spec = self.meta_store.catalog().get(name).cloned()?;
        if !spec.placement.contains(&self.node_id) {
            return None;
        }
        let members: Vec<(NodeId, String)> = spec
            .placement
            .iter()
            .filter_map(|id| self.addr_of(*id).map(|a| (*id, a)))
            .collect();
        if members.len() != spec.placement.len() {
            return None;
        }
        if let Err(e) = self.start_local_member(name, &members).await {
            tracing::warn!(queue = %name, error = %e, "lazy group member start failed");
            return None;
        }
        self.groups.lock().expect("groups lock").get(name).cloned()
    }

    /// Which node currently leads `name`'s group, from this node's view:
    /// the local member's metrics when we host one, else a `WhoLeads` poll
    /// of the placement nodes.
    pub async fn resolve_queue_leader(&self, name: &str) -> Option<NodeId> {
        if let Some(member) = self.group_member(name).await {
            let leader = member.raft.metrics().borrow().current_leader;
            if leader.is_some() {
                return leader;
            }
            // Election in progress: give it one timeout's worth of patience.
            return member
                .raft
                .wait(Some(Duration::from_secs(3)))
                .metrics(|m| m.current_leader.is_some(), "queue leader")
                .await
                .ok()
                .and_then(|m| m.current_leader);
        }
        let spec = self.meta_store.catalog().get(name).cloned()?;
        for id in &spec.placement {
            if *id == self.node_id {
                continue;
            }
            let Some(addr) = self.addr_of(*id) else {
                continue;
            };
            let Ok(conn) = self.peers.client(*id, &addr).conn().await else {
                continue;
            };
            if let Ok(body) = conn
                .call(
                    RequestKind::WhoLeads {
                        queue: name.to_owned(),
                    },
                    Bytes::new(),
                )
                .await
                && let Ok(Some(leader)) = bincode::deserialize::<Option<NodeId>>(&body)
            {
                return Some(leader);
            }
        }
        None
    }

    /// Wait (bounded) for the group to elect a leader.
    async fn wait_group_leader(
        &self,
        name: &str,
        _spec: &QueueSpec,
        timeout: Duration,
    ) -> Result<NodeId, String> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if let Some(leader) = self.resolve_queue_leader(name).await {
                return Ok(leader);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!("queue group {name} formed no leader in time"));
            }
            tokio::time::sleep(Duration::from_millis(150)).await;
        }
    }

    /// The leader-local actor for `name` — only if this node currently leads
    /// its group. `Err` carries the leader hint for the caller to follow.
    pub(crate) async fn leader_actor(&self, name: &str) -> Result<QueueHandle, Option<NodeId>> {
        let Some(member) = self.group_member(name).await else {
            return Err(None);
        };
        let leader = member.raft.metrics().borrow().current_leader;
        match leader {
            Some(id) if id == self.node_id => {
                let mut actors = self.actors.lock().expect("actors lock");
                if let Some(handle) = actors.get(name)
                    && !handle.tx.is_closed()
                {
                    return Ok(handle.clone());
                }
                let policy = crate::policy::EffectivePolicy::resolve(
                    &self.settings.policies,
                    name,
                    self.settings.max_queue_depth,
                    self.settings.max_queue_bytes,
                    self.settings.dlx.clone(),
                );
                // Dead letters route through THIS node's registry: a
                // node-local target (transient/durable) lands on whichever
                // node happens to lead when the message dies, scattering the
                // DLQ across the cluster after failovers. A /quorum/ target
                // is leader-routed and therefore cluster-wide — say so when
                // the config picks otherwise.
                if let Some(target) = &policy.dead_letter
                    && !target.starts_with("/quorum/")
                {
                    tracing::warn!(
                        queue = %name,
                        %target,
                        "clustered queue dead-letters into a NODE-LOCAL target: dead letters \
                         will scatter across whichever nodes lead over time; use a /quorum/ \
                         target for a cluster-wide dead-letter queue"
                    );
                }
                let handle = quorum::spawn(
                    name.to_owned(),
                    member.raft.clone(),
                    member.store.clone(),
                    policy,
                    true, // exit when leadership is lost
                );
                actors.insert(name.to_owned(), handle.clone());
                Ok(handle)
            }
            Some(other) => Err(Some(other)),
            None => Err(None),
        }
    }

    /// Commit a catalog write, forwarding to the metadata leader if needed.
    pub async fn meta_write(&self, cmd: MetaCommand) -> Result<MetaResponse, String> {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let mut hint: Option<(NodeId, Option<String>)> = None;
        loop {
            // Follow the current hint (a known remote leader), else try local.
            let attempt: Result<MetaResponse, MetaWriteError> = match hint.take() {
                Some((leader, addr)) if leader != self.node_id => {
                    self.forward_meta_write(leader, addr, &cmd).await
                }
                _ => self.local_meta_write(&cmd).await,
            };
            match attempt {
                Ok(resp) => return Ok(resp),
                Err(MetaWriteError::NotLeader(next)) => {
                    hint = next.map(|id| (id, None));
                }
                Err(MetaWriteError::Other(e)) => {
                    tracing::debug!(error = %e, "meta write retry");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("catalog write timed out (no metadata leader?)".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    /// One local catalog write attempt (leader-only).
    async fn local_meta_write(&self, cmd: &MetaCommand) -> Result<MetaResponse, MetaWriteError> {
        match self.meta.client_write(cmd.clone()).await {
            Ok(resp) => Ok(resp.data),
            Err(RaftError::APIError(ClientWriteError::ForwardToLeader(f))) => {
                // Register the leader's address so the retry can dial it.
                if let (Some(id), Some(node)) = (f.leader_id, &f.leader_node) {
                    self.peers.client(id, &node.addr);
                }
                Err(MetaWriteError::NotLeader(f.leader_id))
            }
            Err(e) => Err(MetaWriteError::Other(e.to_string())),
        }
    }

    async fn forward_meta_write(
        &self,
        leader: NodeId,
        addr: Option<String>,
        cmd: &MetaCommand,
    ) -> Result<MetaResponse, MetaWriteError> {
        let addr = addr
            .or_else(|| self.addr_of(leader))
            .ok_or_else(|| MetaWriteError::Other(format!("no address for leader {leader}")))?;
        let conn = self
            .peers
            .client(leader, &addr)
            .conn()
            .await
            .map_err(|e| MetaWriteError::Other(e.to_string()))?;
        let body = bincode::serialize(cmd).map_err(|e| MetaWriteError::Other(e.to_string()))?;
        let reply = conn
            .call(RequestKind::MetaWrite, Bytes::from(body))
            .await
            .map_err(MetaWriteError::Other)?;
        bincode::deserialize::<Result<MetaResponse, MetaWriteError>>(&reply)
            .map_err(|e| MetaWriteError::Other(e.to_string()))?
    }

    /// The fabric client connection to a peer (address book: seeds, then
    /// membership).
    pub(crate) async fn peer_conn(&self, id: NodeId) -> Result<Arc<fabric::ConnState>, String> {
        let addr = self
            .addr_of(id)
            .ok_or_else(|| format!("no address for node {id}"))?;
        self.peers
            .client(id, &addr)
            .conn()
            .await
            .map_err(|e| e.to_string())
    }
}

use super::fnv1a;

/// Build a queue group's store: paged (spill + on-disk snapshot blobs) when
/// a data dir is available, plain in-memory otherwise; persistent (log/vote/
/// snapshot recovered and written through) when a persistence factory is
/// wired too.
pub(crate) fn paged_queue_store(
    data_dir: Option<&std::path::Path>,
    queue: &str,
    resident_bytes_max: usize,
    persist: Option<&Arc<dyn super::store::RaftPersistFactory>>,
) -> Result<QueueStore, String> {
    let Some(dir) = data_dir else {
        return Ok(QueueStore::default());
    };
    let (spill_dir, snapshot_dir) = super::queue_dirs(dir, queue);
    match persist {
        Some(factory) => {
            let (sink, recovery) = factory.open_group(&format!("queue/{queue}"))?;
            // Preserve spill segments only when there is persisted state
            // that may reference them.
            let spill = if recovery.snapshot.is_some() {
                super::paging::Spill::open_preserving(spill_dir)?
            } else {
                super::paging::Spill::open(spill_dir)?
            };
            let store = QueueStore::new_persistent(
                super::queue_group::QueueState::paged(spill.clone(), resident_bytes_max),
                Some(snapshot_dir),
                sink,
                recovery,
            )?;
            // Seed recovered segments' live counts from the restored state;
            // unreferenced leftovers reclaim here.
            let counts = store.with_state(|s| s.spill_live_counts());
            spill.set_live(&counts);
            Ok(store)
        }
        None => {
            let spill = super::paging::Spill::open(spill_dir)?;
            Ok(QueueStore::new_with(
                super::queue_group::QueueState::paged(spill, resident_bytes_max),
                Some(snapshot_dir),
            ))
        }
    }
}

/// Rendezvous (highest-random-weight) placement: every node ranks the same,
/// so any declarer picks the same replica set for a given voter set.
pub(crate) fn rendezvous_placement(name: &str, nodes: &[NodeId], want: usize) -> Vec<NodeId> {
    let mut scored: Vec<(u64, NodeId)> = nodes
        .iter()
        .map(|&id| {
            let score = fnv1a(name.as_bytes().iter().copied().chain(id.to_be_bytes()));
            (score, id)
        })
        .collect();
    scored.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    scored.truncate(want);
    let mut placement: Vec<NodeId> = scored.into_iter().map(|(_, id)| id).collect();
    placement.sort_unstable();
    placement
}

/// The generic fabric-backed Raft network: serializes RPCs with bincode and
/// rides the shared per-peer connection, tagged with the group id.
#[derive(Debug, Clone)]
pub(crate) struct FabricNetworkFactory {
    pub peers: Arc<fabric::Peers>,
    pub group: GroupRef,
}

impl<C> openraft::network::RaftNetworkFactory<C> for FabricNetworkFactory
where
    C: openraft::RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    type Network = FabricRaftConn;

    async fn new_client(&mut self, target: NodeId, node: &BasicNode) -> Self::Network {
        FabricRaftConn {
            target,
            peer: self.peers.client(target, &node.addr),
            group: self.group.clone(),
        }
    }
}

/// One group's Raft "connection" to a peer (a view over the shared fabric
/// connection).
#[derive(Debug)]
pub(crate) struct FabricRaftConn {
    target: NodeId,
    peer: Arc<fabric::PeerClient>,
    group: GroupRef,
}

type NetError<E> = openraft::error::RPCError<NodeId, BasicNode, E>;

impl FabricRaftConn {
    async fn raft_call<Req, Resp, E>(
        &mut self,
        kind: RaftKind,
        rpc: &Req,
    ) -> Result<Resp, NetError<E>>
    where
        Req: serde::Serialize,
        Resp: serde::de::DeserializeOwned,
        E: serde::de::DeserializeOwned + std::error::Error,
    {
        let unreachable = |msg: String| {
            openraft::error::RPCError::Unreachable(openraft::error::Unreachable::new(
                &std::io::Error::other(msg),
            ))
        };
        let body = bincode::serialize(rpc).map_err(|e| unreachable(e.to_string()))?;
        let conn = self
            .peer
            .conn()
            .await
            .map_err(|e| unreachable(e.to_string()))?;
        let reply = conn
            .call(
                RequestKind::Raft(self.group.clone(), kind),
                Bytes::from(body),
            )
            .await
            .map_err(unreachable)?;
        let result: Result<Resp, E> =
            bincode::deserialize(&reply).map_err(|e| unreachable(e.to_string()))?;
        result.map_err(|e| {
            openraft::error::RPCError::RemoteError(openraft::error::RemoteError::new(
                self.target,
                e,
            ))
        })
    }
}

impl<C> openraft::network::RaftNetwork<C> for FabricRaftConn
where
    C: openraft::RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    async fn append_entries(
        &mut self,
        rpc: openraft::raft::AppendEntriesRequest<C>,
        _option: openraft::network::RPCOption,
    ) -> Result<openraft::raft::AppendEntriesResponse<NodeId>, NetError<RaftError<NodeId>>> {
        self.raft_call(RaftKind::AppendEntries, &rpc).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: openraft::raft::InstallSnapshotRequest<C>,
        _option: openraft::network::RPCOption,
    ) -> Result<
        openraft::raft::InstallSnapshotResponse<NodeId>,
        NetError<RaftError<NodeId, openraft::error::InstallSnapshotError>>,
    > {
        self.raft_call(RaftKind::InstallSnapshot, &rpc).await
    }

    async fn vote(
        &mut self,
        rpc: openraft::raft::VoteRequest<NodeId>,
        _option: openraft::network::RPCOption,
    ) -> Result<openraft::raft::VoteResponse<NodeId>, NetError<RaftError<NodeId>>> {
        self.raft_call(RaftKind::Vote, &rpc).await
    }
}

// ---------------------------------------------------------------------------
// Fabric server: the leader side of everything.
// ---------------------------------------------------------------------------

/// Accept fabric connections until shutdown.
async fn serve_fabric(
    listener: TcpListener,
    node: Arc<ClusterNode>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        tokio::spawn(handle_conn(stream, node.clone()));
                    }
                    Err(e) => {
                        tracing::warn!(error = %e, "fabric accept error; continuing");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
            _ = shutdown.changed() => return,
        }
    }
}

/// A queue-bound command from this connection, delivered in order by the
/// per-connection forwarder (so one full mailbox never blocks the frame
/// reader, and per-producer FIFO is preserved).
enum QueueBound {
    /// An ordered publish; the reply is sent when the actor settles it.
    /// `reserved` marks a transaction-commit publish consuming a slot held
    /// by a prior [`RequestKind::Reserve`].
    Publish {
        corr: u64,
        queue: String,
        body: Bytes,
        reserved: bool,
    },
    /// A direct actor command (demand/settle/unsubscribe).
    Cmd(mpsc::Sender<QueueMsg>, QueueMsg),
}

/// Per-connection subscription state on the leader side.
struct LeaderSub {
    queue: QueueHandle,
    sub: crate::queue::SubId,
}

/// Serve one inbound fabric connection.
async fn handle_conn(stream: tokio::net::TcpStream, node: Arc<ClusterNode>) {
    let (mut reader, writer_half) = stream.into_split();
    let writer = spawn_writer(writer_half);
    let subs: Arc<std::sync::Mutex<HashMap<u64, LeaderSub>>> =
        Arc::new(std::sync::Mutex::new(HashMap::new()));

    // The ordered toward-queues forwarder.
    let (fw_tx, fw_rx) = mpsc::unbounded_channel::<QueueBound>();
    tokio::spawn(queue_forwarder(fw_rx, node.clone(), writer.clone()));

    let mut buf = bytes::BytesMut::with_capacity(64 * 1024);
    loop {
        let (header, body) = match fabric::read_frame(&mut reader, &mut buf).await {
            Ok(frame) => frame,
            Err(_) => break,
        };
        match header {
            FabricHeader::Request { corr, req } => {
                handle_request(corr, req, body, &node, &writer, &subs, &fw_tx).await;
            }
            FabricHeader::Demand {
                sub_chan,
                credit,
                drain,
            } => {
                let target = subs
                    .lock()
                    .expect("leader subs lock")
                    .get(&sub_chan)
                    .map(|s| (s.queue.tx.clone(), s.sub));
                if let Some((tx, sub)) = target {
                    let _ =
                        fw_tx.send(QueueBound::Cmd(tx, QueueMsg::Demand { sub, credit, drain }));
                }
            }
            FabricHeader::Settle {
                sub_chan,
                msg_id,
                outcome,
            } => {
                let target = subs
                    .lock()
                    .expect("leader subs lock")
                    .get(&sub_chan)
                    .map(|s| (s.queue.tx.clone(), s.sub));
                if let Some((tx, sub)) = target {
                    let _ = fw_tx.send(QueueBound::Cmd(
                        tx,
                        QueueMsg::Settle {
                            sub,
                            msg_id,
                            outcome: outcome.into(),
                        },
                    ));
                }
            }
            FabricHeader::CloseSub { sub_chan } => {
                let removed = subs.lock().expect("leader subs lock").remove(&sub_chan);
                if let Some(s) = removed {
                    let _ = fw_tx.send(QueueBound::Cmd(
                        s.queue.tx.clone(),
                        QueueMsg::Unsubscribe { sub: s.sub },
                    ));
                }
            }
            other => {
                tracing::warn!(?other, "unexpected fabric frame on a server connection");
            }
        }
    }

    // Connection gone: every subscription it held unsubscribes, requeueing
    // whatever its consumers had in flight.
    let drained: Vec<LeaderSub> = {
        let mut map = subs.lock().expect("leader subs lock");
        map.drain().map(|(_, s)| s).collect()
    };
    for s in drained {
        let _ = s.queue.tx.send(QueueMsg::Unsubscribe { sub: s.sub }).await;
    }
}

/// Handle one correlated request. Raft RPCs and subscription opens spawn;
/// publishes go through the ordered forwarder.
async fn handle_request(
    corr: u64,
    req: RequestKind,
    body: Bytes,
    node: &Arc<ClusterNode>,
    writer: &mpsc::UnboundedSender<OutFrame>,
    subs: &Arc<std::sync::Mutex<HashMap<u64, LeaderSub>>>,
    fw_tx: &mpsc::UnboundedSender<QueueBound>,
) {
    match req {
        RequestKind::Raft(group, kind) => {
            let node = node.clone();
            let writer = writer.clone();
            tokio::spawn(async move {
                let reply = handle_raft(&node, group, kind, body).await;
                send_reply(&writer, corr, reply);
            });
        }
        RequestKind::MetaWrite => {
            let node = node.clone();
            let writer = writer.clone();
            tokio::spawn(async move {
                let result: Result<MetaResponse, MetaWriteError> = match bincode::deserialize(&body)
                {
                    Ok(cmd) => node.local_meta_write(&cmd).await,
                    Err(e) => Err(MetaWriteError::Other(e.to_string())),
                };
                send_reply(
                    &writer,
                    corr,
                    bincode::serialize(&result).map_err(|e| e.to_string()),
                );
            });
        }
        RequestKind::StartGroup { queue, members } => {
            let node = node.clone();
            let writer = writer.clone();
            tokio::spawn(async move {
                let reply = node
                    .start_local_member(&queue, &members)
                    .await
                    .map(|()| Vec::new());
                send_reply(&writer, corr, reply);
            });
        }
        RequestKind::WhoLeads { queue } => {
            let leader = node
                .groups
                .lock()
                .expect("groups lock")
                .get(&queue)
                .map(|m| m.raft.metrics().borrow().current_leader);
            let leader: Option<NodeId> = leader.flatten();
            send_reply(
                writer,
                corr,
                bincode::serialize(&leader).map_err(|e| e.to_string()),
            );
        }
        RequestKind::Publish { queue } => {
            // Ordered: rides the forwarder so per-producer FIFO holds.
            let _ = fw_tx.send(QueueBound::Publish {
                corr,
                queue,
                body,
                reserved: false,
            });
        }
        RequestKind::PublishReserved { queue } => {
            let _ = fw_tx.send(QueueBound::Publish {
                corr,
                queue,
                body,
                reserved: true,
            });
        }
        RequestKind::Reserve { queue, count } => {
            let node = node.clone();
            let writer = writer.clone();
            tokio::spawn(async move {
                let ok = match node.leader_actor(&queue).await {
                    Ok(handle) => {
                        let (reply_tx, reply_rx) = oneshot::channel();
                        handle
                            .tx
                            .send(QueueMsg::Reserve {
                                count,
                                reply: reply_tx,
                            })
                            .await
                            .is_ok()
                            && reply_rx.await.unwrap_or(false)
                    }
                    Err(_) => false,
                };
                send_reply(
                    &writer,
                    corr,
                    bincode::serialize(&ok).map_err(|e| e.to_string()),
                );
            });
        }
        RequestKind::Unreserve { queue, count } => {
            let node = node.clone();
            let writer = writer.clone();
            tokio::spawn(async move {
                if let Ok(handle) = node.leader_actor(&queue).await {
                    let _ = handle.tx.send(QueueMsg::Unreserve { count }).await;
                }
                send_reply(&writer, corr, Ok(Vec::new()));
            });
        }
        RequestKind::OpenSub { queue, sub_chan } => {
            let node = node.clone();
            let writer = writer.clone();
            let subs = subs.clone();
            tokio::spawn(async move {
                let outcome = open_leader_sub(&node, &queue, sub_chan, &writer, &subs).await;
                send_reply(
                    &writer,
                    corr,
                    bincode::serialize(&outcome).map_err(|e| e.to_string()),
                );
            });
        }
    }
}

fn send_reply(
    writer: &mpsc::UnboundedSender<OutFrame>,
    corr: u64,
    result: Result<Vec<u8>, String>,
) {
    let frame = match result {
        Ok(body) => OutFrame::new(FabricHeader::Reply { corr }, Bytes::from(body)),
        Err(msg) => OutFrame::control(FabricHeader::ReplyErr { corr, msg }),
    };
    let _ = writer.send(frame);
}

/// Dispatch one Raft RPC into the right local group member.
async fn handle_raft(
    node: &Arc<ClusterNode>,
    group: GroupRef,
    kind: RaftKind,
    body: Bytes,
) -> Result<Vec<u8>, String> {
    match group {
        GroupRef::Meta => {
            let raft = node.meta.clone();
            raft_dispatch::<MetaTypeConfig>(&raft, kind, &body).await
        }
        GroupRef::Queue(name) => {
            let member = node
                .group_member(&name)
                .await
                .ok_or_else(|| format!("no local member of queue group {name}"))?;
            raft_dispatch::<QueueTypeConfig>(&member.raft, kind, &body).await
        }
    }
}

/// Decode, call, and encode one Raft RPC for a concrete type config.
async fn raft_dispatch<C>(
    raft: &openraft::Raft<C>,
    kind: RaftKind,
    body: &[u8],
) -> Result<Vec<u8>, String>
where
    C: openraft::RaftTypeConfig<NodeId = NodeId, Node = BasicNode>,
{
    match kind {
        RaftKind::AppendEntries => {
            let rpc: openraft::raft::AppendEntriesRequest<C> =
                bincode::deserialize(body).map_err(|e| e.to_string())?;
            let result = raft.append_entries(rpc).await;
            bincode::serialize(&result).map_err(|e| e.to_string())
        }
        RaftKind::Vote => {
            let rpc: openraft::raft::VoteRequest<NodeId> =
                bincode::deserialize(body).map_err(|e| e.to_string())?;
            let result = raft.vote(rpc).await;
            bincode::serialize(&result).map_err(|e| e.to_string())
        }
        RaftKind::InstallSnapshot => {
            let rpc: openraft::raft::InstallSnapshotRequest<C> =
                bincode::deserialize(body).map_err(|e| e.to_string())?;
            let result = raft.install_snapshot(rpc).await;
            bincode::serialize(&result).map_err(|e| e.to_string())
        }
    }
}

/// Open a leader-side subscription: subscribe to the local actor and pump
/// its deliveries back over the fabric as `Deliver` frames.
async fn open_leader_sub(
    node: &Arc<ClusterNode>,
    queue: &str,
    sub_chan: u64,
    writer: &mpsc::UnboundedSender<OutFrame>,
    subs: &Arc<std::sync::Mutex<HashMap<u64, LeaderSub>>>,
) -> Result<(), Option<NodeId>> {
    let handle = match node.leader_actor(queue).await {
        Ok(h) => h,
        Err(hint) => return Err(hint),
    };
    let (conn_tx, mut conn_rx) = mpsc::unbounded_channel::<ConnCmd>();
    let (reply_tx, reply_rx) = oneshot::channel();
    let _ = handle
        .tx
        .send(QueueMsg::Subscribe {
            conn: conn_tx,
            channel: 0,
            handle: 0,
            binding_gen: sub_chan,
            reply: reply_tx,
        })
        .await;
    let Ok(sub) = reply_rx.await else {
        // Actor died between resolve and subscribe (leadership moved).
        return Err(None);
    };
    subs.lock().expect("leader subs lock").insert(
        sub_chan,
        LeaderSub {
            queue: handle.clone(),
            sub,
        },
    );

    // The delivery pump: actor → fabric. Exits when the actor drops the
    // subscriber (unsubscribe, actor death) or the connection's writer dies.
    let writer = writer.clone();
    let subs = subs.clone();
    tokio::spawn(async move {
        while let Some(cmd) = conn_rx.recv().await {
            if let ConnCmd::Deliver { msg_id, body, .. } = cmd {
                let frame = OutFrame::new(FabricHeader::Deliver { sub_chan, msg_id }, body);
                if writer.send(frame).is_err() {
                    // Connection gone; the reader-side teardown unsubscribes.
                    return;
                }
            }
        }
        // Actor dropped us (leadership lost / queue deleted): tell the origin.
        subs.lock().expect("leader subs lock").remove(&sub_chan);
        let _ = writer.send(OutFrame::control(FabricHeader::SubClosed { sub_chan }));
    });
    Ok(())
}

/// The ordered toward-queues forwarder for one connection: publishes resolve
/// the leader actor (cached), enqueue in arrival order, and answer on commit.
async fn queue_forwarder(
    mut rx: mpsc::UnboundedReceiver<QueueBound>,
    node: Arc<ClusterNode>,
    writer: mpsc::UnboundedSender<OutFrame>,
) {
    let mut cache: HashMap<String, QueueHandle> = HashMap::new();
    while let Some(cmd) = rx.recv().await {
        match cmd {
            QueueBound::Publish {
                corr,
                queue,
                body,
                reserved,
            } => {
                // Resolve (or re-resolve) the leader-local actor.
                let mut handle = match cache.get(&queue) {
                    Some(h) if !h.tx.is_closed() => Some(h.clone()),
                    _ => None,
                };
                if handle.is_none() {
                    match node.leader_actor(&queue).await {
                        Ok(h) => {
                            cache.insert(queue.clone(), h.clone());
                            handle = Some(h);
                        }
                        Err(hint) => {
                            send_publish_status(&writer, corr, PublishStatus::NotLeader(hint));
                            continue;
                        }
                    }
                }
                let handle = handle.expect("resolved above");
                // A per-publish ack channel: the actor confirms on commit.
                let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<ConnCmd>();
                let ack = Some(PublishAck {
                    conn: ack_tx,
                    channel: 0,
                    handle: 0,
                    binding_gen: 0,
                    delivery_id: 0,
                });
                let msg = if reserved {
                    QueueMsg::PublishReserved { body, ack }
                } else {
                    QueueMsg::Publish { body, ack }
                };
                let sent = handle.tx.send(msg).await.is_ok();
                if !sent {
                    // Actor died (leadership moved between resolve and send).
                    cache.remove(&queue);
                    send_publish_status(&writer, corr, PublishStatus::NotLeader(None));
                    continue;
                }
                let writer = writer.clone();
                tokio::spawn(async move {
                    let status = match ack_rx.recv().await {
                        Some(ConnCmd::SettleIncoming { accepted: true, .. }) => {
                            PublishStatus::Accepted
                        }
                        Some(ConnCmd::SettleIncoming {
                            accepted: false, ..
                        }) => PublishStatus::Rejected,
                        // Actor died before settling: leadership moved; the
                        // origin retries (the enqueue may or may not have
                        // committed — at-least-once).
                        _ => PublishStatus::NotLeader(None),
                    };
                    send_publish_status(&writer, corr, status);
                });
            }
            QueueBound::Cmd(tx, msg) => {
                // Bounded mailbox: this await is the backpressure point, and
                // it deliberately never blocks the frame reader.
                let _ = tx.send(msg).await;
            }
        }
    }
}

fn send_publish_status(writer: &mpsc::UnboundedSender<OutFrame>, corr: u64, status: PublishStatus) {
    send_reply(
        writer,
        corr,
        bincode::serialize(&status).map_err(|e| e.to_string()),
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxy;
    use crate::queue::SettleOutcome;

    /// Reserve `n` loopback ports, then release them for the nodes to
    /// re-bind (a small race window; fine for tests).
    async fn reserve_addrs(n: usize) -> Vec<String> {
        let mut addrs = Vec::new();
        for _ in 0..n {
            let l = TcpListener::bind("127.0.0.1:0").await.unwrap();
            addrs.push(l.local_addr().unwrap().to_string());
            drop(l);
        }
        addrs
    }

    async fn spawn_cluster(n: usize, replicas: u8) -> Vec<Arc<ClusterNode>> {
        let addrs = reserve_addrs(n).await;
        let seeds: Vec<(NodeId, String)> = (1..=n as u64).zip(addrs).collect();
        let mut nodes = Vec::new();
        for (id, addr) in &seeds {
            let node = ClusterNode::bootstrap(NodeSettings {
                node_id: *id,
                listen: addr.clone(),
                seeds: seeds.clone(),
                replicas,
                max_queue_depth: 10_000,
                max_queue_bytes: 0,
                data_dir: None,
                resident_bytes_max: usize::MAX,
                policies: Vec::new(),
                dlx: None,
                persist: None,
            })
            .await
            .expect("bootstrap");
            nodes.push(node);
        }
        for node in &nodes {
            node.await_membership(Duration::from_secs(15))
                .await
                .expect("cluster formed");
        }
        nodes
    }

    #[test]
    fn rendezvous_placement_is_deterministic_and_spreads() {
        let nodes = [1u64, 2, 3, 4, 5];
        let a = rendezvous_placement("orders", &nodes, 3);
        let b = rendezvous_placement("orders", &nodes, 3);
        assert_eq!(a, b, "same inputs, same placement");
        assert_eq!(a.len(), 3);
        // Different queues should not all land on the same set (spread over
        // enough names, every node hosts something).
        let mut hosted = std::collections::BTreeSet::new();
        for i in 0..40 {
            for id in rendezvous_placement(&format!("q{i}"), &nodes, 3) {
                hosted.insert(id);
            }
        }
        assert_eq!(hosted.len(), nodes.len(), "placement uses every node");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn seeds_form_a_cluster_over_the_fabric() {
        let nodes = spawn_cluster(3, 3).await;
        // A catalog write through any node converges on every store.
        nodes[1]
            .meta_write(MetaCommand::CreateQueue {
                name: "seeded".into(),
                spec: QueueSpec {
                    queue_type: QueueType::Quorum,
                    replicas: 3,
                    placement: vec![1, 2, 3],
                },
            })
            .await
            .expect("catalog write (forwarded if follower)");
        for node in &nodes {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while !node.meta_store.catalog().contains_key("seeded") {
                assert!(
                    std::time::Instant::now() < deadline,
                    "node {} never applied the write",
                    node.node_id
                );
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        }
        for node in &nodes {
            node.stop().await;
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn declare_places_and_forms_the_queue_group() {
        let nodes = spawn_cluster(3, 3).await;
        let spec = nodes[2]
            .declare_quorum("orders")
            .await
            .expect("declare through a follower works");
        assert_eq!(spec.placement, vec![1, 2, 3], "3 replicas over 3 nodes");
        let leader = nodes[0]
            .resolve_queue_leader("orders")
            .await
            .expect("group elected a leader");
        assert!(
            (1..=3).contains(&leader),
            "leader {leader} is a placement node"
        );
        // A re-declare from another node adopts the same spec.
        let again = nodes[0].declare_quorum("orders").await.expect("re-declare");
        assert_eq!(again.placement, spec.placement);
        for node in &nodes {
            node.stop().await;
        }
    }

    /// Subscribe through a proxy: a tiny stand-in for the connection driver.
    async fn subscribe(
        proxy: &QueueHandle,
        conn: &mpsc::UnboundedSender<ConnCmd>,
        credit: u32,
    ) -> crate::queue::SubId {
        let (reply_tx, reply_rx) = oneshot::channel();
        proxy
            .tx
            .send(QueueMsg::Subscribe {
                conn: conn.clone(),
                channel: 7,
                handle: 3,
                binding_gen: 1,
                reply: reply_tx,
            })
            .await
            .expect("subscribe");
        let sub = reply_rx.await.expect("sub id");
        proxy
            .tx
            .send(QueueMsg::Demand {
                sub,
                credit,
                drain: false,
            })
            .await
            .expect("demand");
        sub
    }

    /// Publish through a proxy and await the confirm. Returns whether the
    /// publish was accepted.
    async fn publish(proxy: &QueueHandle, body: &[u8], id: u32) -> bool {
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel();
        proxy
            .tx
            .send(QueueMsg::Publish {
                body: Bytes::copy_from_slice(body),
                ack: Some(PublishAck {
                    conn: ack_tx,
                    channel: 0,
                    handle: 0,
                    binding_gen: 0,
                    delivery_id: id,
                }),
            })
            .await
            .expect("publish send");
        match ack_rx.recv().await {
            Some(ConnCmd::SettleIncoming {
                delivery_id,
                accepted,
                ..
            }) => {
                assert_eq!(delivery_id, id, "confirm matches the publish");
                accepted
            }
            other => panic!("expected publish confirm, got {other:?}"),
        }
    }

    /// The forwarding fabric end to end (minus AMQP): produce through one
    /// node's proxy, consume through another's — wherever the leader lives,
    /// at least one of the two paths is remote.
    #[tokio::test(flavor = "multi_thread")]
    async fn produce_and_consume_across_nodes() {
        let nodes = spawn_cluster(3, 3).await;
        nodes[0].declare_quorum("xnode").await.expect("declare");
        let leader = nodes[0]
            .resolve_queue_leader("xnode")
            .await
            .expect("leader");
        // Put the producer and consumer on the two NON-leader nodes so both
        // directions of the remote path are exercised.
        let others: Vec<&Arc<ClusterNode>> = nodes.iter().filter(|n| n.node_id != leader).collect();
        let producer = proxy::spawn("xnode".into(), others[0].clone());
        let consumer = proxy::spawn("xnode".into(), others[1].clone());

        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        let sub = subscribe(&consumer, &conn_tx, 100).await;

        for i in 0..20u32 {
            assert!(
                publish(&producer, &i.to_be_bytes(), i).await,
                "publish {i} accepted (committed)"
            );
        }
        let mut got = Vec::new();
        for _ in 0..20 {
            let cmd = tokio::time::timeout(Duration::from_secs(10), conn_rx.recv())
                .await
                .expect("delivery in time")
                .expect("delivery");
            match cmd {
                ConnCmd::Deliver {
                    channel,
                    handle,
                    binding_gen,
                    msg_id,
                    body,
                } => {
                    assert_eq!(
                        (channel, handle, binding_gen),
                        (7, 3, 1),
                        "stamped for the real link"
                    );
                    got.push(u32::from_be_bytes(body[..4].try_into().unwrap()));
                    consumer
                        .tx
                        .send(QueueMsg::Settle {
                            sub,
                            msg_id,
                            outcome: SettleOutcome::Ack,
                        })
                        .await
                        .expect("settle");
                }
                other => panic!("expected deliver, got {other:?}"),
            }
        }
        got.sort_unstable();
        assert_eq!(got, (0..20).collect::<Vec<_>>(), "all messages, no loss");
        for node in &nodes {
            node.stop().await;
        }
    }

    /// The Phase 6 headline at the fabric level: kill the LEADER NODE
    /// mid-stream; the consumer (on a survivor) keeps receiving; every
    /// accepted publish is delivered (at-least-once, zero committed loss).
    #[tokio::test(flavor = "multi_thread")]
    async fn killing_the_leader_node_loses_no_accepted_message() {
        let nodes = spawn_cluster(3, 3).await;
        nodes[0].declare_quorum("failover").await.expect("declare");
        let leader = nodes[0]
            .resolve_queue_leader("failover")
            .await
            .expect("leader");
        let survivors: Vec<&Arc<ClusterNode>> =
            nodes.iter().filter(|n| n.node_id != leader).collect();
        let producer = proxy::spawn("failover".into(), survivors[0].clone());
        let consumer = proxy::spawn("failover".into(), survivors[1].clone());

        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        let sub = subscribe(&consumer, &conn_tx, 1000).await;

        // Phase 1: 50 accepted publishes with the leader up.
        let mut accepted = Vec::new();
        for i in 0..50u32 {
            if publish(&producer, &i.to_be_bytes(), i).await {
                accepted.push(i);
            }
        }
        assert_eq!(accepted.len(), 50, "all pre-kill publishes accepted");

        // Kill the leader node (fabric, Raft members, actors — everything).
        let leader_node = nodes.iter().find(|n| n.node_id == leader).unwrap();
        leader_node.stop().await;

        // Phase 2: keep publishing through the failover. Some publishes are
        // rejected while the group re-elects; retry them — only *accepted*
        // ones count toward the zero-loss contract.
        for i in 50..100u32 {
            let deadline = std::time::Instant::now() + Duration::from_secs(30);
            loop {
                if publish(&producer, &i.to_be_bytes(), i).await {
                    accepted.push(i);
                    break;
                }
                assert!(
                    std::time::Instant::now() < deadline,
                    "publish {i} never accepted after failover"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }

        // Every accepted message arrives (dedup: at-least-once may repeat).
        let mut got = std::collections::BTreeSet::new();
        let deadline = std::time::Instant::now() + Duration::from_secs(60);
        while got.len() < accepted.len() {
            assert!(
                std::time::Instant::now() < deadline,
                "only {}/{} messages arrived after leader kill",
                got.len(),
                accepted.len()
            );
            let Ok(Some(cmd)) = tokio::time::timeout(Duration::from_secs(10), conn_rx.recv()).await
            else {
                continue;
            };
            if let ConnCmd::Deliver { msg_id, body, .. } = cmd {
                got.insert(u32::from_be_bytes(body[..4].try_into().unwrap()));
                let _ = consumer
                    .tx
                    .send(QueueMsg::Settle {
                        sub,
                        msg_id,
                        outcome: SettleOutcome::Ack,
                    })
                    .await;
            }
        }
        let want: std::collections::BTreeSet<u32> = accepted.into_iter().collect();
        assert_eq!(got, want, "zero accepted-message loss across leader kill");
        for node in survivors {
            node.stop().await;
        }
    }
}
