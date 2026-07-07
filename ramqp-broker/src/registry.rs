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
use crate::cluster::queue_group::{QueueRaft, QueueStore};
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
    /// Cap on the number of distinct queues (0 = unbounded); bounds how many
    /// actors / Raft groups a client can auto-declare.
    max_queues: usize,
    /// This node's id for single-replica queue groups.
    node_id: NodeId,
    /// The cluster node, when this broker is clustered. Set once at bind.
    cluster: OnceLock<Arc<ClusterNode>>,
    /// The durable store (`store-redb`), when a data dir is configured.
    /// Opened lazily on the first `/durable/*` resolve.
    #[cfg(feature = "store-redb")]
    store: tokio::sync::OnceCell<crate::store::Store>,
    /// Where the durable store lives (`None` → `/durable/*` refused).
    #[cfg(feature = "store-redb")]
    data_dir: Option<std::path::PathBuf>,
}

impl QueueRegistry {
    pub fn new(
        max_depth: usize,
        max_queues: usize,
        #[cfg_attr(not(feature = "store-redb"), allow(unused_variables))] data_dir: Option<
            std::path::PathBuf,
        >,
    ) -> Self {
        QueueRegistry {
            queues: std::sync::Mutex::new(HashMap::new()),
            max_depth,
            max_queues,
            node_id: 1,
            cluster: OnceLock::new(),
            #[cfg(feature = "store-redb")]
            store: tokio::sync::OnceCell::new(),
            #[cfg(feature = "store-redb")]
            data_dir,
        }
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

    /// Resolve an address, declaring the queue if it doesn't exist.
    pub async fn resolve(&self, address: &str) -> Option<QueueHandle> {
        let (kind, name) = Self::parse_address(address)?;
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
                if !map.contains_key(&key) && self.max_queues != 0 && map.len() >= self.max_queues {
                    tracing::warn!(
                        queue = %name,
                        max = self.max_queues,
                        "queue limit reached; refusing to auto-declare"
                    );
                    return None;
                }
                map.entry(key.clone()).or_default().clone()
            };
            // Initialize outside the lock; the cell serializes same-key inits.
            let init = cell
                .get_or_try_init(|| async {
                    let h = match kind {
                        QueueKind::Transient => queue::spawn(name.to_owned(), self.max_depth),
                        // Clustered: declare through the replicated catalog and
                        // serve through a leader-following proxy.
                        QueueKind::Quorum => match self.cluster.get() {
                            Some(node) => {
                                node.declare_quorum(name)
                                    .await
                                    .map_err(|e| {
                                        tracing::warn!(queue = %name, error = %e, "quorum declare failed");
                                    })?;
                                proxy::spawn(name.to_owned(), node.clone())
                            }
                            None => spawn_quorum_group(name.to_owned(), self.node_id, self.max_depth)
                                .await
                                .ok_or(())?,
                        },
                        #[cfg(feature = "store-redb")]
                        QueueKind::Durable => {
                            let store = self.store().await.ok_or(())?;
                            let queue_id = store.queue_id(name).map_err(|e| {
                                tracing::error!(queue = %name, error = %e, "durable queue id failed");
                            })?;
                            crate::durable::spawn(name.to_owned(), store, queue_id, self.max_depth)
                                .map_err(|e| {
                                    tracing::error!(queue = %name, error = %e, "durable recovery failed");
                                })?
                        }
                        #[cfg(not(feature = "store-redb"))]
                        QueueKind::Durable => {
                            tracing::warn!(
                                queue = %name,
                                "durable queue requested but the broker was built without `store-redb`"
                            );
                            return Err(());
                        }
                    };
                    Ok::<_, ()>(h)
                })
                .await;
            let handle = match init {
                Ok(h) => h,
                // Init failed: drop the empty cell so it neither counts against
                // the cap nor serves a poisoned entry; the next attach retries.
                Err(()) => {
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
    max_depth: usize,
) -> Option<QueueHandle> {
    let config = openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Compaction: snapshot every 5k applied entries and keep only a short
        // log tail behind it, so log memory tracks queue depth rather than
        // total messages ever enqueued (broker.md §3.2 bounded-memory rule).
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5_000),
        max_in_snapshot_log_to_keep: 1_000,
        purge_batch_size: 1_000,
        ..Default::default()
    }
    .validate()
    .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum config invalid"))
    .ok()?;
    let store = QueueStore::default();
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
    raft.initialize(std::collections::BTreeMap::from([(
        node_id,
        BasicNode::new("local"),
    )]))
    .await
    .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum initialize failed"))
    .ok()?;
    raft.wait(Some(std::time::Duration::from_secs(10)))
        .current_leader(node_id, "single-replica leader")
        .await
        .map_err(|e| tracing::error!(queue = %name, error = %e, "quorum leader-wait failed"))
        .ok()?;
    // A single-replica standalone group can never be demoted.
    Some(quorum::spawn(name, raft, store, max_depth, false))
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
        let r = QueueRegistry::new(10, 0, None);
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

    #[tokio::test]
    async fn resolve_enforces_the_queue_cap() {
        let r = QueueRegistry::new(10, 2, None);
        // Two distinct queues declare fine.
        assert!(r.resolve("/queues/a").await.is_some());
        assert!(r.resolve("/queues/b").await.is_some());
        // A third *new* queue is refused at the cap...
        assert!(r.resolve("/queues/c").await.is_none());
        // ...but already-declared queues still resolve.
        assert!(r.resolve("/queues/a").await.is_some());
        assert!(r.resolve("b").await.is_some());
    }
}
