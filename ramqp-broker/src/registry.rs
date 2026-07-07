//! Queue registry: address → queue resolution and on-demand declaration.
//!
//! Address model (interim until the management API lands in Phase 9):
//! `/queues/<name>` and bare names declare **transient** queues (the Phase 4
//! in-memory actor); `/quorum/<name>` declares a **quorum** queue backed by a
//! per-queue Raft group. Standalone (no cluster configured), a quorum queue
//! is a single-replica group on this node; clustered, it is declared through
//! the replicated catalog, placed on its replica set, and served through a
//! [`crate::proxy`] actor that follows the group's leader wherever it lives.
//!
//! Resolution happens only at attach time (never per-message), so an async
//! mutex around the map is fine — the message path stays lock-free.

use std::collections::HashMap;
use std::sync::{Arc, OnceLock};

use openraft::BasicNode;

use crate::cluster::NodeId;
use crate::cluster::network::UnreachableNetwork;
use crate::cluster::node::ClusterNode;
use crate::cluster::queue_group::QueueRaft;
use crate::config::{BrokerConfig, QueuePolicy};
use crate::policy::{DeadLetterSender, EffectivePolicy};
use crate::proxy;
use crate::queue::{self, QueueHandle};
use crate::quorum;

/// How a resolved address wants its queue backed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueKind {
    /// Node-local, no consensus (the Phase 4 actor).
    Transient,
    /// Backed by a per-queue Raft group.
    Quorum,
    /// Node-local, on-disk (`store-redb` feature; survives restarts).
    Durable,
}

/// Why one queue's initialization aborted.
enum InitAbort {
    /// Initialization genuinely failed: evict the cell, refuse the attach.
    Failed,
    /// A concurrent initializer's failure evicted this cell while we waited
    /// on it — succeeding now would set an ORPHANED cell, and the next
    /// attach would declare a second live actor for the same queue
    /// (split-brain; duplicate delivery for durable queues). Retry on a
    /// fresh cell instead.
    Orphaned,
}

/// Resolves addresses to queue actors, declaring queues on first use.
///
/// The map is guarded by a *std* mutex held only for the O(1) get-or-insert of
/// a per-key init cell — never across queue creation. The actual (async,
/// possibly seconds-long for a quorum group) initialization runs on a
/// [`tokio::sync::OnceCell`], which serializes only concurrent inits of the
/// *same* queue; resolving a different queue is never blocked.
#[derive(Debug)]
pub(crate) struct QueueRegistry {
    queues: std::sync::Mutex<HashMap<String, Arc<tokio::sync::OnceCell<QueueHandle>>>>,
    max_depth: usize,
    /// Broker-wide per-queue byte bound (0 = disabled).
    max_bytes: usize,
    /// Cap on the number of distinct queues (0 = unbounded); bounds how many
    /// actors / Raft groups a client can auto-declare.
    max_queues: usize,
    /// Per-vhost queue cap (0 = disabled); stops one tenant exhausting the
    /// broker-wide `max_queues` pool.
    max_queues_per_vhost: usize,
    /// This node's id for single-replica queue groups.
    node_id: NodeId,
    /// The cluster node, when this broker is clustered. Set once at bind.
    cluster: OnceLock<Arc<ClusterNode>>,
    /// The durable store (`store-redb`), when a data dir is configured.
    /// Opened lazily on the first `/durable/*` resolve.
    #[cfg(feature = "store-redb")]
    store: tokio::sync::OnceCell<crate::store::Store>,
    /// On-disk root: durable-store data (`store-redb`) and quorum-queue
    /// paging/snapshots. `None` → `/durable/*` refused, quorum queues stay
    /// fully in memory.
    data_dir: Option<std::path::PathBuf>,
    /// Per-queue resident-body budget before quorum paging kicks in.
    resident_bytes_max: usize,
    /// Queue policies (prefix-matched at declaration).
    policies: Vec<(String, QueuePolicy)>,
    /// The dead-letter router, wired in right after construction.
    dlx: OnceLock<DeadLetterSender>,
}

impl QueueRegistry {
    pub fn new(config: &BrokerConfig) -> Self {
        QueueRegistry {
            queues: std::sync::Mutex::new(HashMap::new()),
            max_depth: config.max_queue_depth,
            max_bytes: config.max_queue_bytes,
            max_queues: config.max_queues,
            max_queues_per_vhost: config.max_queues_per_vhost,
            node_id: 1,
            cluster: OnceLock::new(),
            #[cfg(feature = "store-redb")]
            store: tokio::sync::OnceCell::new(),
            data_dir: config.data_dir.clone(),
            resident_bytes_max: config.resident_bytes_max,
            policies: config.policies.clone(),
            dlx: OnceLock::new(),
        }
    }

    /// Wire the dead-letter router (once, right after construction).
    pub fn set_dlx(&self, dlx: DeadLetterSender) {
        let _ = self.dlx.set(dlx);
    }

    /// The resolved policy for a queue.
    fn policy_for(&self, name: &str) -> EffectivePolicy {
        EffectivePolicy::resolve(
            &self.policies,
            name,
            self.max_depth,
            self.max_bytes,
            self.dlx.get().cloned(),
        )
    }

    /// The durable store, opened on first use (`None` when unconfigured or
    /// the open failed — the attach is then refused). A failed open is NOT
    /// cached: a transient failure (e.g. the previous instance's file lock
    /// not yet released) is retried on the next attach.
    #[cfg(feature = "store-redb")]
    async fn store(&self) -> Option<crate::store::Store> {
        let dir = self.data_dir.as_ref()?;
        self.store
            .get_or_try_init(|| async {
                crate::store::Store::open(dir).map_err(|e| {
                    tracing::error!(dir = %dir.display(), error = %e, "durable store open failed");
                })
            })
            .await
            .ok()
            .cloned()
    }

    /// The Raft persistence factory (`store-redb` + data dir), if available.
    #[cfg(feature = "store-redb")]
    pub async fn persist_factory(
        &self,
    ) -> Option<Arc<dyn crate::cluster::store::RaftPersistFactory>> {
        let store = self.store().await?;
        Some(Arc::new(store) as Arc<dyn crate::cluster::store::RaftPersistFactory>)
    }

    /// Without `store-redb` there is no Raft persistence.
    #[cfg(not(feature = "store-redb"))]
    pub async fn persist_factory(
        &self,
    ) -> Option<Arc<dyn crate::cluster::store::RaftPersistFactory>> {
        None
    }

    /// Enumerate declared queues: `(kind-qualified key, handle)` — the
    /// management/metrics surface (never on a message path).
    pub fn queues(&self) -> Vec<(String, QueueHandle)> {
        let map = self.queues.lock().expect("registry lock");
        map.iter()
            .filter_map(|(key, cell)| cell.get().map(|h| (key.clone(), h.clone())))
            .collect()
    }

    /// Attach the cluster node (idempotent; first caller wins).
    pub fn set_cluster(&self, node: Arc<ClusterNode>) {
        let _ = self.cluster.set(node);
    }

    /// The cluster node, when clustered.
    pub fn cluster(&self) -> Option<&Arc<ClusterNode>> {
        self.cluster.get()
    }

    /// Normalize an AMQP address to `(kind, queue name)`. Accepts the
    /// RabbitMQ-4.x style `/queues/<name>`, `/quorum/<name>` for replicated
    /// queues, `/durable/<name>` for on-disk local queues, and bare names
    /// (with or without a leading `/`) as transient.
    pub fn parse_address(address: &str) -> Option<(QueueKind, &str)> {
        if let Some(name) = address.strip_prefix("/quorum/") {
            return (!name.is_empty()).then_some((QueueKind::Quorum, name));
        }
        if let Some(name) = address.strip_prefix("/durable/") {
            return (!name.is_empty()).then_some((QueueKind::Durable, name));
        }
        let name = address
            .strip_prefix("/queues/")
            .unwrap_or_else(|| address.trim_start_matches('/'));
        (!name.is_empty()).then_some((QueueKind::Transient, name))
    }

    /// Resolve an address in the default namespace — broker-INTERNAL callers
    /// only (the dead-letter router, management). Unlike client resolution,
    /// this accepts qualified names containing `/`: a per-vhost policy
    /// addresses `/queues/<vhost>/dead`, which lands on that vhost's key
    /// (the documented dead-letter composition).
    pub async fn resolve(&self, address: &str) -> Option<QueueHandle> {
        let (kind, name) = Self::parse_address(address)?;
        self.resolve_name(kind, name, "").await
    }

    /// Resolve a CLIENT address within a vhost, declaring the queue if it
    /// doesn't exist. A non-empty vhost namespaces the queue (name,
    /// policies, storage, catalog) as `<vhost>/<name>` — so neither
    /// component may contain the `/` separator: a client-chosen name
    /// crossing it (`tenantA/secret` from the default vhost) would land on
    /// another tenant's storage key, below the authz layer. Control
    /// characters are refused for log/metrics hygiene.
    pub async fn resolve_in(&self, vhost: &str, address: &str) -> Option<QueueHandle> {
        let (kind, bare) = Self::parse_address(address)?;
        let valid = |s: &str| !s.contains('/') && !s.chars().any(char::is_control);
        if !valid(bare) || !valid(vhost) {
            tracing::debug!(
                vhost,
                address,
                "address refused: reserved separator or control character"
            );
            return None;
        }
        let qualified;
        let name: &str = if vhost.is_empty() {
            bare
        } else {
            qualified = format!("{vhost}/{bare}");
            &qualified
        };
        self.resolve_name(kind, name, vhost).await
    }

    /// Count declared queues belonging to `vhost` (any kind). Keys are
    /// `<kind>:<vhost>/<name>`, so a queue is in `vhost` when the part after
    /// the kind prefix begins with `<vhost>/`.
    fn count_vhost_queues(
        map: &HashMap<String, Arc<tokio::sync::OnceCell<QueueHandle>>>,
        vhost: &str,
    ) -> usize {
        let needle = format!("{vhost}/");
        map.keys()
            .filter(|k| {
                k.split_once(':')
                    .is_some_and(|(_, rest)| rest.starts_with(&needle))
            })
            .count()
    }

    /// Get-or-declare the queue actor for a normalized, namespace-qualified
    /// name. `vhost` (empty for the default / broker-internal callers) gates
    /// the per-vhost cap.
    async fn resolve_name(&self, kind: QueueKind, name: &str, vhost: &str) -> Option<QueueHandle> {
        // Kind-qualified key: `/queues/foo`, `/quorum/foo`, and
        // `/durable/foo` are distinct queues.
        let key = match kind {
            QueueKind::Transient => format!("t:{name}"),
            QueueKind::Quorum => format!("q:{name}"),
            QueueKind::Durable => format!("d:{name}"),
        };
        // Bounded retry so a queue that dies on spawn can't loop forever.
        for _ in 0..3 {
            // Brief lock: get or create this key's init cell, then release. A
            // brand-new key is refused once the queue cap is reached (an
            // unbounded auto-declare DoS guard); existing queues still resolve.
            let cell = {
                let mut map = self.queues.lock().expect("registry lock");
                let is_new = !map.contains_key(&key);
                if is_new && self.max_queues != 0 && map.len() >= self.max_queues {
                    tracing::warn!(
                        queue = %name,
                        max = self.max_queues,
                        "queue limit reached; refusing to auto-declare"
                    );
                    return None;
                }
                // Per-vhost cap: keep one tenant from exhausting the shared
                // broker-wide pool (LOW-12). Applies to non-default vhosts;
                // the key suffix is `<vhost>/...` for those.
                if is_new
                    && !vhost.is_empty()
                    && self.max_queues_per_vhost != 0
                    && Self::count_vhost_queues(&map, vhost) >= self.max_queues_per_vhost
                {
                    tracing::warn!(
                        %vhost,
                        max = self.max_queues_per_vhost,
                        "per-vhost queue limit reached; refusing to auto-declare"
                    );
                    return None;
                }
                map.entry(key.clone()).or_default().clone()
            };
            // Initialize outside the lock; the cell serializes same-key inits.
            let init = cell
                .get_or_try_init(|| async {
                    // A concurrent initializer may have failed and evicted
                    // this cell while we waited on it (see InitAbort::
                    // Orphaned). Checked under the map lock: either the
                    // eviction already happened (we see it here) or it will
                    // be skipped (its `get().is_none()` guard fails once we
                    // set a value).
                    {
                        let map = self.queues.lock().expect("registry lock");
                        if !map.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                            return Err(InitAbort::Orphaned);
                        }
                    }
                    let h = match kind {
                        QueueKind::Transient => {
                            queue::spawn(name.to_owned(), self.policy_for(name))
                        }
                        // Clustered: declare through the replicated catalog and
                        // serve through a leader-following proxy.
                        QueueKind::Quorum => match self.cluster.get() {
                            Some(node) => {
                                node.declare_quorum(name).await.map_err(|e| {
                                    tracing::warn!(queue = %name, error = %e, "quorum declare failed");
                                    InitAbort::Failed
                                })?;
                                proxy::spawn(name.to_owned(), node.clone())
                            }
                            None => {
                                let persist = self.persist_factory().await;
                                // With a data dir + the store feature, quorum
                                // queues REQUIRE the store: falling back to a
                                // fresh in-memory group would shadow (and wipe
                                // the spill of) persisted state — e.g. while a
                                // previous instance's file lock lingers.
                                // Refuse instead; the next attach retries.
                                #[cfg(feature = "store-redb")]
                                if self.data_dir.is_some() && persist.is_none() {
                                    tracing::warn!(
                                        queue = %name,
                                        "durable store not openable yet; refusing quorum declare"
                                    );
                                    return Err(InitAbort::Failed);
                                }
                                spawn_quorum_group(
                                    name.to_owned(),
                                    self.node_id,
                                    self.policy_for(name),
                                    self.data_dir.as_deref(),
                                    self.resident_bytes_max,
                                    persist,
                                )
                                .await
                                .ok_or(InitAbort::Failed)?
                            }
                        },
                        #[cfg(feature = "store-redb")]
                        QueueKind::Durable => {
                            let store = self.store().await.ok_or(InitAbort::Failed)?;
                            let queue_id = store.queue_id(name).map_err(|e| {
                                tracing::error!(queue = %name, error = %e, "durable queue id failed");
                                InitAbort::Failed
                            })?;
                            crate::durable::spawn(
                                name.to_owned(),
                                store,
                                queue_id,
                                self.policy_for(name),
                            )
                            .map_err(|e| {
                                tracing::error!(queue = %name, error = %e, "durable recovery failed");
                                InitAbort::Failed
                            })?
                        }
                        #[cfg(not(feature = "store-redb"))]
                        QueueKind::Durable => {
                            tracing::warn!(
                                queue = %name,
                                "durable queue requested but the broker was built without `store-redb`"
                            );
                            return Err(InitAbort::Failed);
                        }
                    };
                    Ok::<_, InitAbort>(h)
                })
                .await;
            let handle = match init {
                Ok(h) => h,
                // Our cell was evicted while we waited: retry on a fresh one.
                Err(InitAbort::Orphaned) => continue,
                // Init failed: drop the empty cell so it neither counts against
                // the cap nor serves a poisoned entry; the next attach retries.
                Err(InitAbort::Failed) => {
                    let mut map = self.queues.lock().expect("registry lock");
                    if map
                        .get(&key)
                        .is_some_and(|c| Arc::ptr_eq(c, &cell) && c.get().is_none())
                    {
                        map.remove(&key);
                    }
                    return None;
                }
            };
            // Evict a dead queue (its actor task stopped) and re-declare, so a
            // publish/attach never hangs against a defunct handle.
            if handle.tx.is_closed() {
                let mut map = self.queues.lock().expect("registry lock");
                if map.get(&key).is_some_and(|c| Arc::ptr_eq(c, &cell)) {
                    map.remove(&key);
                }
                continue;
            }
            return Some(handle.clone());
        }
        None
    }
}

/// Start a single-replica queue group and its quorum actor. Logs (rather than
/// silently swallows) each failure so a resolution error is diagnosable.
async fn spawn_quorum_group(
    name: String,
    node_id: NodeId,
    policy: EffectivePolicy,
    data_dir: Option<&std::path::Path>,
    resident_bytes_max: usize,
    persist: Option<Arc<dyn crate::cluster::store::RaftPersistFactory>>,
) -> Option<QueueHandle> {
    let config = openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Compaction: snapshot every 5k applied entries and keep only a short
        // log tail behind it, so log memory tracks queue depth rather than
        // total messages ever enqueued (broker.md §3.2 bounded-memory rule).
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
    .validate()
    .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum config invalid"))
    .ok()?;
    let store = crate::cluster::node::paged_queue_store(
        data_dir,
        &name,
        resident_bytes_max,
        persist.as_ref(),
    )
    .map_err(|e| tracing::error!(queue = %name, error = %e, "paged store open failed"))
    .ok()?;
    let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
    let raft = QueueRaft::new(
        node_id,
        Arc::new(config),
        UnreachableNetwork,
        log_store,
        state_machine,
    )
    .await
    .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum raft init failed"))
    .ok()?;
    match raft
        .initialize(std::collections::BTreeMap::from([(
            node_id,
            BasicNode::new("local"),
        )]))
        .await
    {
        Ok(()) => {}
        // A recovered (persisted) group is already initialized — expected
        // on restart, not an error.
        Err(openraft::error::RaftError::APIError(
            openraft::error::InitializeError::NotAllowed(_),
        )) => {}
        Err(e) => {
            tracing::error!(queue = %name, error = %e, "quorum initialize failed");
            return None;
        }
    }
    raft.wait(Some(std::time::Duration::from_secs(10)))
        .current_leader(node_id, "single-replica leader")
        .await
        .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum leader-wait failed"))
        .ok()?;
    // A single-replica standalone group can never be demoted.
    Some(quorum::spawn(name, raft, store, policy, false))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_normalization() {
        assert_eq!(
            QueueRegistry::parse_address("/queues/orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("/orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("/quorum/orders"),
            Some((QueueKind::Quorum, "orders"))
        );
        assert_eq!(QueueRegistry::parse_address("/queues/"), None);
        assert_eq!(QueueRegistry::parse_address("/quorum/"), None);
        assert_eq!(QueueRegistry::parse_address(""), None);
    }

    #[tokio::test]
    async fn resolve_is_idempotent_and_kind_scoped() {
        let r = QueueRegistry::new(&BrokerConfig {
            max_queue_depth: 10,
            max_queues: 0,
            ..Default::default()
        });
        let a = r.resolve("/queues/q1").await.unwrap();
        let b = r.resolve("q1").await.unwrap();
        assert!(a.tx.same_channel(&b.tx), "same transient queue");

        let quorum = r.resolve("/quorum/q1").await.unwrap();
        assert!(
            !a.tx.same_channel(&quorum.tx),
            "quorum q1 is a distinct queue from transient q1"
        );
        let quorum2 = r.resolve("/quorum/q1").await.unwrap();
        assert!(quorum.tx.same_channel(&quorum2.tx));
    }

    /// HIGH-6 (issue #19): client addresses may not cross the
    /// `<vhost>/<name>` key separator or carry control characters; the
    /// broker-internal path (dead-letter routing) still composes qualified
    /// names.
    #[tokio::test]
    async fn client_addresses_cannot_cross_tenant_keys() {
        let r = QueueRegistry::new(&BrokerConfig::default());
        let secret = r
            .resolve_in("tenant-a", "/queues/secret")
            .await
            .expect("tenant queue declares");
        // Crossing the separator from the default vhost is refused...
        assert!(r.resolve_in("", "/queues/tenant-a/secret").await.is_none());
        // ...as are control characters and separator-bearing vhosts.
        assert!(r.resolve_in("", "/queues/bad\nname").await.is_none());
        assert!(r.resolve_in("bad/host", "/queues/x").await.is_none());
        // The INTERNAL path still composes per-vhost targets (DLX routing).
        let dlx = r
            .resolve("/queues/tenant-a/secret")
            .await
            .expect("internal qualified resolve");
        assert!(
            secret.tx.same_channel(&dlx.tx),
            "internal qualified resolution reaches the tenant's queue"
        );
    }

    /// LOW-12 (issue #19): the per-vhost cap stops one tenant from
    /// exhausting the shared broker-wide pool.
    #[tokio::test]
    async fn per_vhost_cap_isolates_tenants() {
        let r = QueueRegistry::new(&BrokerConfig {
            max_queue_depth: 10,
            max_queues: 0, // global cap disabled
            max_queues_per_vhost: 2,
            ..Default::default()
        });
        // Tenant A fills its per-vhost allowance.
        assert!(r.resolve_in("a", "/queues/q1").await.is_some());
        assert!(r.resolve_in("a", "/queues/q2").await.is_some());
        // A third NEW queue for tenant A is refused...
        assert!(r.resolve_in("a", "/queues/q3").await.is_none());
        // ...but tenant B is unaffected, and A's existing queues still resolve.
        assert!(r.resolve_in("b", "/queues/q1").await.is_some());
        assert!(r.resolve_in("a", "/queues/q1").await.is_some());
    }

    #[tokio::test]
    async fn resolve_enforces_the_queue_cap() {
        let r = QueueRegistry::new(&BrokerConfig {
            max_queue_depth: 10,
            max_queues: 2,
            ..Default::default()
        });
        // Two distinct queues declare fine.
        assert!(r.resolve("/queues/a").await.is_some());
        assert!(r.resolve("/queues/b").await.is_some());
        // A third *new* queue is refused at the cap...
        assert!(r.resolve("/queues/c").await.is_none());
        // ...but already-declared queues still resolve.
        assert!(r.resolve("/queues/a").await.is_some());
        assert!(r.resolve("b").await.is_some());
    }

    /// Probe: standalone quorum persistence round trip at the module level.
    #[cfg(feature = "store-redb")]
    #[tokio::test(flavor = "multi_thread")]
    async fn quorum_group_persists_and_recovers() {
        let dir = tempfile::tempdir().expect("tempdir");
        let config = BrokerConfig {
            data_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        // First life: publish 3.
        {
            let r = QueueRegistry::new(&config);
            let h = r.resolve("/quorum/persist-probe").await.expect("resolve");
            for i in 0..3u8 {
                let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
                h.tx.send(crate::queue::QueueMsg::Publish {
                    body: bytes::Bytes::from(vec![i; 4]),
                    ack: Some(crate::queue::PublishAck {
                        conn: tx,
                        channel: 0,
                        handle: 0,
                        binding_gen: 0,
                        delivery_id: i as u32,
                    }),
                })
                .await
                .expect("publish");
                match rx.recv().await {
                    Some(crate::queue::ConnCmd::SettleIncoming { accepted, .. }) => {
                        assert!(accepted, "publish {i} must commit")
                    }
                    other => panic!("expected settle, got {other:?}"),
                }
            }
            // registry drops here; actors/raft wind down
        }
        // Give the store writer thread time to release the lock.
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        // Second life: the queue recovers 3 ready messages.
        let r = QueueRegistry::new(&config);
        let h = match r.resolve("/quorum/persist-probe").await {
            Some(h) => h,
            None => panic!("recovered resolve failed"),
        };
        let (conn_tx, mut conn_rx) = tokio::sync::mpsc::unbounded_channel();
        let (reply_tx, reply_rx) = tokio::sync::oneshot::channel();
        h.tx.send(crate::queue::QueueMsg::Subscribe {
            conn: conn_tx,
            channel: 0,
            handle: 0,
            binding_gen: 0,
            reply: reply_tx,
        })
        .await
        .expect("subscribe");
        let sub = reply_rx.await.expect("sub id");
        h.tx.send(crate::queue::QueueMsg::Demand {
            sub,
            credit: 10,
            drain: false,
        })
        .await
        .expect("demand");
        for i in 0..3u8 {
            let cmd = tokio::time::timeout(std::time::Duration::from_secs(10), conn_rx.recv())
                .await
                .expect("recovered delivery in time")
                .expect("delivery");
            match cmd {
                crate::queue::ConnCmd::Deliver { body, .. } => assert_eq!(&body[..], &[i; 4]),
                other => panic!("expected deliver, got {other:?}"),
            }
        }
    }
}
