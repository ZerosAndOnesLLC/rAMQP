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
use crate::exchange::{ExchangeError, ExchangeOp, ExchangeRegistry};
use crate::policy::{DeadLetterSender, EffectivePolicy};
use crate::proxy;
use crate::queue::{self, QueueHandle, QueueMsg};
use crate::quorum;

/// The store key prefix for standalone exchange definitions.
#[cfg(feature = "store-redb")]
const EXCHANGE_ROW: &str = "exchange:";

/// Why a queue deletion did not happen.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum QueueDeleteError {
    /// No such queue.
    NotFound,
    /// The deletion cannot proceed right now (e.g. a cluster node still
    /// runs a version without quorum-queue deletion).
    Unavailable(String),
    /// The deletion failed (store error); the queue is unchanged.
    Failed,
}

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
    /// Exchange definitions (the routing view).
    exchanges: ExchangeRegistry,
    /// Serializes standalone topology changes (read-modify-persist-swap).
    exchange_write: tokio::sync::Mutex<()>,
    /// Quorum queue incarnations this registry is serving: standalone,
    /// the authority when there is no store (in memory); clustered, the
    /// incarnation each proxy was created for (a newer one means the queue
    /// was deleted underneath it).
    quorum_incarnations: std::sync::Mutex<HashMap<String, u64>>,
    /// Whether the routing view reflects the persisted definitions. False
    /// only for a standalone broker with a store that has not been read yet
    /// (e.g. the previous instance still held its lock at bind); until then
    /// exchange attaches and changes are refused rather than served from an
    /// empty view.
    exchanges_loaded: std::sync::atomic::AtomicBool,
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
            exchanges: ExchangeRegistry::default(),
            exchange_write: tokio::sync::Mutex::new(()),
            quorum_incarnations: std::sync::Mutex::new(HashMap::new()),
            exchanges_loaded: std::sync::atomic::AtomicBool::new(
                !cfg!(feature = "store-redb") || config.data_dir.is_none(),
            ),
        }
    }

    /// The exchange routing view.
    pub fn exchanges(&self) -> &ExchangeRegistry {
        &self.exchanges
    }

    /// Apply a topology change: through the metadata group when clustered,
    /// else locally (persisted to the store when one is configured). Returns
    /// whether it changed anything; on return this node's routing view
    /// reflects it.
    pub async fn exchange_op(&self, op: ExchangeOp) -> Result<bool, ExchangeError> {
        crate::exchange::validate(&op)?;
        if let Some(node) = self.cluster.get() {
            let (changed, epoch) = node.exchange_op(op).await?;
            // The commit may not have applied on this node yet: wait for it
            // so the caller's next attach sees the change.
            let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(5);
            while node.exchanges_epoch() < epoch && tokio::time::Instant::now() < deadline {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
            self.sync_exchanges();
            return Ok(changed);
        }
        let _serial = self.exchange_write.lock().await;
        if !self.load_exchanges_locked().await {
            return Err(ExchangeError::Unavailable(
                "persisted exchange definitions not loaded yet (store unavailable); retry"
                    .to_owned(),
            ));
        }
        let current = self.exchanges.snapshot();
        let mut next = (*current).clone();
        let changed = crate::exchange::apply_op(&mut next, &op)?;
        if changed {
            self.persist_exchanges(&current, &next).await?;
            self.exchanges.replace(next);
        }
        Ok(changed)
    }

    /// Whether the routing view is authoritative, loading the persisted
    /// definitions first if that has not happened yet (standalone with a
    /// store). `false` = not loadable right now; the caller refuses.
    pub async fn ensure_exchanges_loaded(&self) -> bool {
        use std::sync::atomic::Ordering;
        if self.exchanges_loaded.load(Ordering::Acquire) || self.cluster.get().is_some() {
            return true;
        }
        let _serial = self.exchange_write.lock().await;
        self.load_exchanges_locked().await
    }

    /// [`Self::ensure_exchanges_loaded`] with `exchange_write` held.
    async fn load_exchanges_locked(&self) -> bool {
        use std::sync::atomic::Ordering;
        if self.exchanges_loaded.load(Ordering::Acquire) || self.cluster.get().is_some() {
            return true;
        }
        match self.read_persisted_exchanges().await {
            Ok(defs) => {
                if let Some(defs) = defs {
                    self.exchanges.replace(defs);
                }
                self.exchanges_loaded.store(true, Ordering::Release);
                true
            }
            Err(e) => {
                tracing::warn!(error = %e, "exchange definitions not loadable yet");
                false
            }
        }
    }

    /// Clustered: refresh the routing view from the applied metadata state.
    pub fn sync_exchanges(&self) {
        if let Some(node) = self.cluster.get() {
            let epoch = node.exchanges_epoch();
            self.exchanges
                .replace_if_newer(epoch, || node.exchange_state().1);
        }
    }

    /// Standalone: persist what changed between `before` and `after` (one
    /// row per exchange; no-op without a store).
    #[cfg(feature = "store-redb")]
    async fn persist_exchanges(
        &self,
        before: &crate::exchange::Defs,
        after: &crate::exchange::Defs,
    ) -> Result<(), ExchangeError> {
        if self.data_dir.is_none() {
            return Ok(());
        }
        let store = self
            .store()
            .await
            .ok_or_else(|| ExchangeError::Unavailable("durable store not openable".to_owned()))?;
        let mut puts = Vec::new();
        for (k, def) in after {
            if before.get(k) != Some(def) {
                let blob = serde_json::to_vec(def)
                    .map_err(|e| ExchangeError::Unavailable(e.to_string()))?;
                puts.push((format!("{EXCHANGE_ROW}{k}"), blob));
            }
        }
        let deletes: Vec<String> = before
            .keys()
            .filter(|k| !after.contains_key(*k))
            .map(|k| format!("{EXCHANGE_ROW}{k}"))
            .collect();
        tokio::task::spawn_blocking(move || store.write_definitions(&puts, &deletes))
            .await
            .map_err(|e| ExchangeError::Unavailable(e.to_string()))?
            .map_err(ExchangeError::Unavailable)
    }

    #[cfg(not(feature = "store-redb"))]
    async fn persist_exchanges(
        &self,
        _before: &crate::exchange::Defs,
        _after: &crate::exchange::Defs,
    ) -> Result<(), ExchangeError> {
        Ok(())
    }

    /// Read the persisted standalone definitions (`None` = none stored). A
    /// row that does not parse is moved aside (`corrupt:<key>`) with a loud
    /// error instead of blocking every exchange: the config file's startup
    /// declarations — or the admin API — can restore it.
    #[cfg(feature = "store-redb")]
    async fn read_persisted_exchanges(&self) -> Result<Option<crate::exchange::Defs>, String> {
        let store = self.store().await.ok_or_else(|| {
            "durable store not openable (previous instance still holds the lock?)".to_owned()
        })?;
        let rows = store.definitions(EXCHANGE_ROW)?;
        if rows.is_empty() {
            return Ok(None);
        }
        let mut defs = crate::exchange::Defs::new();
        let mut quarantine = Vec::new();
        for (key, blob) in rows {
            match serde_json::from_slice::<crate::exchange::ExchangeDef>(&blob) {
                Ok(def) => {
                    defs.insert(key[EXCHANGE_ROW.len()..].to_owned(), def);
                }
                Err(e) => {
                    tracing::error!(
                        row = %key,
                        error = %e,
                        "corrupt exchange definition moved aside; redeclare the exchange"
                    );
                    quarantine.push((key, blob));
                }
            }
        }
        if !quarantine.is_empty() {
            let puts: Vec<(String, Vec<u8>)> = quarantine
                .iter()
                .map(|(k, b)| (format!("corrupt:{k}"), b.clone()))
                .collect();
            let deletes: Vec<String> = quarantine.into_iter().map(|(k, _)| k).collect();
            tokio::task::spawn_blocking(move || store.write_definitions(&puts, &deletes))
                .await
                .map_err(|e| e.to_string())??;
        }
        Ok(Some(defs))
    }

    #[cfg(not(feature = "store-redb"))]
    async fn read_persisted_exchanges(&self) -> Result<Option<crate::exchange::Defs>, String> {
        Ok(None)
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

    /// Whether `address` explicitly names a queue (`/queues/`, `/quorum/`,
    /// `/durable/` with a non-empty name) — the strict form declarations
    /// require (bare names are accepted only at attach, for compatibility).
    pub fn is_queue_address(address: &str) -> bool {
        ["/queues/", "/quorum/", "/durable/"]
            .iter()
            .any(|p| address.strip_prefix(p).is_some_and(|n| !n.is_empty()))
    }

    /// Whether a vhost name is usable as a namespace (no `/`, no control
    /// characters).
    pub fn valid_vhost(vhost: &str) -> bool {
        !vhost.contains('/') && !vhost.chars().any(char::is_control)
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
        // `$`-prefixed names are reserved for broker pseudo-addresses (the
        // transaction coordinator authorizes as `$coordinator`).
        if !valid(bare) || !valid(vhost) || bare.starts_with('$') {
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

    /// The registry key for a kind + namespace-qualified name.
    fn key_for(kind: QueueKind, name: &str) -> String {
        match kind {
            QueueKind::Transient => format!("t:{name}"),
            QueueKind::Quorum => format!("q:{name}"),
            QueueKind::Durable => format!("d:{name}"),
        }
    }

    /// The namespace-qualified name (`<vhost>/<name>`, or `<name>` in the
    /// default vhost), or `None` if either part is invalid.
    pub fn qualify(vhost: &str, bare: &str) -> Option<String> {
        let valid = |s: &str| !s.contains('/') && !s.chars().any(char::is_control);
        if bare.is_empty() || !valid(bare) || !valid(vhost) || bare.starts_with('$') {
            return None;
        }
        Some(if vhost.is_empty() {
            bare.to_owned()
        } else {
            format!("{vhost}/{bare}")
        })
    }

    /// The declared (live) queue of `kind` with this qualified name.
    pub fn lookup(&self, kind: QueueKind, qualified: &str) -> Option<QueueHandle> {
        let map = self.queues.lock().expect("registry lock");
        map.get(&Self::key_for(kind, qualified))
            .and_then(|c| c.get().cloned())
            .filter(|h| !h.tx.is_closed())
    }

    /// Find a declared queue (of `kind`, or any kind preferring quorum,
    /// durable, transient). A durable queue with persisted messages but no
    /// live actor (not attached since a restart) is recovered so callers see
    /// its real contents.
    pub async fn find_queue(
        &self,
        kind: Option<QueueKind>,
        qualified: &str,
    ) -> Option<(QueueKind, QueueHandle)> {
        let kinds = match kind {
            Some(k) => vec![k],
            None => vec![QueueKind::Quorum, QueueKind::Durable, QueueKind::Transient],
        };
        for k in &kinds {
            if let Some(h) = self.lookup(*k, qualified) {
                return Some((*k, h));
            }
        }
        // A clustered quorum queue declared anywhere is in the replicated
        // catalog: serve it here too (a leader-following proxy), so a lookup
        // works on every node, not only where something attached.
        if kinds.contains(&QueueKind::Quorum)
            && let Some(node) = self.cluster.get()
            && node.has_queue(qualified)
        {
            let vhost = qualified.split_once('/').map_or("", |(v, _)| v);
            if let Some(h) = self.resolve_name(QueueKind::Quorum, qualified, vhost).await {
                return Some((QueueKind::Quorum, h));
            }
        }
        #[cfg(feature = "store-redb")]
        if kinds.contains(&QueueKind::Durable)
            && self.data_dir.is_some()
            && let Some(store) = self.store().await
            && store.has_queue(qualified).unwrap_or(false)
        {
            let vhost = qualified.split_once('/').map_or("", |(v, _)| v);
            if let Some(h) = self
                .resolve_name(QueueKind::Durable, qualified, vhost)
                .await
            {
                return Some((QueueKind::Durable, h));
            }
        }
        None
    }

    /// Which kinds currently have a live queue under this qualified name.
    pub fn kinds_of(&self, qualified: &str) -> Vec<QueueKind> {
        [QueueKind::Quorum, QueueKind::Durable, QueueKind::Transient]
            .into_iter()
            .filter(|k| self.lookup(*k, qualified).is_some())
            .collect()
    }

    /// Delete a queue (management API). Its consumers are detached
    /// (`amqp:resource-deleted`), its messages and stored state removed (a
    /// durable queue purges its rows; a quorum queue its Raft group — see
    /// [`Self::delete_quorum`]), and the next attach to the address
    /// declares a fresh, empty queue.
    pub async fn delete_queue(
        &self,
        kind: QueueKind,
        qualified: &str,
    ) -> Result<(), QueueDeleteError> {
        if kind == QueueKind::Quorum {
            return self.delete_quorum(qualified).await;
        }
        let (_, handle) = self
            .find_queue(Some(kind), qualified)
            .await
            .ok_or(QueueDeleteError::NotFound)?;
        let (tx, rx) = tokio::sync::oneshot::channel();
        if handle
            .tx
            .send(QueueMsg::Delete { reply: tx })
            .await
            .is_err()
        {
            return Err(QueueDeleteError::NotFound);
        }
        match rx.await {
            Ok(true) => {
                let key = Self::key_for(kind, qualified);
                {
                    let mut map = self.queues.lock().expect("registry lock");
                    if map
                        .get(&key)
                        .and_then(|c| c.get())
                        .is_some_and(|h| h.tx.same_channel(&handle.tx))
                    {
                        map.remove(&key);
                    }
                }
                // Drop the bindings that target it, or the next exchange
                // publish would re-declare it (and it would fill unconsumed).
                let (vhost, bare) = qualified.split_once('/').unwrap_or(("", qualified));
                let prefix = match kind {
                    QueueKind::Transient => "/queues/",
                    QueueKind::Quorum => "/quorum/",
                    QueueKind::Durable => "/durable/",
                };
                if let Err(e) = self
                    .exchange_op(ExchangeOp::UnbindDestination {
                        vhost: vhost.to_owned(),
                        destination: format!("{prefix}{bare}"),
                    })
                    .await
                {
                    tracing::warn!(queue = %qualified, error = %e, "could not remove the deleted queue's bindings");
                }
                Ok(())
            }
            Ok(false) => Err(QueueDeleteError::Failed),
            Err(_) => Err(QueueDeleteError::NotFound),
        }
    }

    /// A standalone quorum queue's current incarnation: persisted in the
    /// store when there is one (so a crash mid-delete never revives the old
    /// queue), else in memory. `None` if the store cannot be read.
    async fn standalone_incarnation(&self, name: &str) -> Option<u64> {
        #[cfg(feature = "store-redb")]
        if self.data_dir.is_some() {
            let store = self.store().await?;
            return store.quorum_incarnation(name).ok();
        }
        Some(
            self.quorum_incarnations
                .lock()
                .expect("incarnations lock")
                .get(name)
                .copied()
                .unwrap_or(0),
        )
    }

    /// Delete a quorum queue. Clustered: committed through the metadata
    /// group (every node then tears down its member and purges its state —
    /// see `ClusterNode::reconcile`), and this node's proxy detaches its
    /// consumers at once. Standalone: the next incarnation is recorded
    /// FIRST (durably), so a re-declare — even after a crash mid-delete —
    /// starts empty; then the actor stops and the old group's state is
    /// purged.
    async fn delete_quorum(&self, qualified: &str) -> Result<(), QueueDeleteError> {
        let (vhost, bare) = qualified.split_once('/').unwrap_or(("", qualified));
        if let Some(node) = self.cluster.get().cloned() {
            match node.delete_quorum(qualified).await {
                Ok(true) => {}
                Ok(false) => return Err(QueueDeleteError::NotFound),
                Err(e) => return Err(QueueDeleteError::Unavailable(e)),
            }
            self.reconcile_quorum().await;
            node.reconcile().await;
        } else {
            let (_, handle) = self
                .find_queue(Some(QueueKind::Quorum), qualified)
                .await
                .ok_or(QueueDeleteError::NotFound)?;
            let current = self
                .standalone_incarnation(qualified)
                .await
                .ok_or(QueueDeleteError::Failed)?;
            self.set_standalone_incarnation(qualified, current + 1)
                .await
                .map_err(QueueDeleteError::Unavailable)?;
            let (tx, rx) = tokio::sync::oneshot::channel();
            let _ = handle.tx.send(QueueMsg::Delete { reply: tx }).await;
            let _ = rx.await;
            self.evict(QueueKind::Quorum, qualified, &handle);
            self.purge_standalone_group(&crate::cluster::group_id(qualified, current))
                .await;
        }
        if let Err(e) = self
            .exchange_op(ExchangeOp::UnbindDestination {
                vhost: vhost.to_owned(),
                destination: format!("/quorum/{bare}"),
            })
            .await
        {
            tracing::warn!(queue = %qualified, error = %e, "could not remove the deleted queue's bindings");
        }
        Ok(())
    }

    /// Record a standalone quorum queue's next incarnation.
    async fn set_standalone_incarnation(&self, name: &str, incarnation: u64) -> Result<(), String> {
        #[cfg(feature = "store-redb")]
        if self.data_dir.is_some() {
            let store = self.store().await.ok_or("durable store not openable")?;
            let name = name.to_owned();
            return tokio::task::spawn_blocking(move || {
                store.set_quorum_incarnation(&name, incarnation)
            })
            .await
            .map_err(|e| e.to_string())?;
        }
        self.quorum_incarnations
            .lock()
            .expect("incarnations lock")
            .insert(name.to_owned(), incarnation);
        Ok(())
    }

    /// Drop a registry entry if it still holds `handle`.
    fn evict(&self, kind: QueueKind, qualified: &str, handle: &QueueHandle) {
        let key = Self::key_for(kind, qualified);
        let mut map = self.queues.lock().expect("registry lock");
        if map
            .get(&key)
            .and_then(|c| c.get())
            .is_some_and(|h| h.tx.same_channel(&handle.tx))
        {
            map.remove(&key);
        }
    }

    /// Remove a stopped standalone quorum group's files, then its Raft rows
    /// (files first: an interruption leaves rows the next GC still finds).
    async fn purge_standalone_group(&self, gid: &str) {
        if let Some(dir) = self.data_dir.as_deref() {
            let (spill, snapshots) = crate::cluster::queue_dirs(dir, gid);
            for d in [spill, snapshots] {
                if let Err(e) = std::fs::remove_dir_all(&d)
                    && e.kind() != std::io::ErrorKind::NotFound
                {
                    tracing::warn!(dir = %d.display(), error = %e, "could not remove a deleted queue's files");
                }
            }
        }
        if let Some(persist) = self.persist_factory().await {
            let group = format!("queue/{gid}");
            match tokio::task::spawn_blocking(move || persist.purge_group(&group)).await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    tracing::warn!(group = %gid, error = %e, "purge failed; retried at next startup")
                }
                Err(e) => tracing::warn!(group = %gid, error = %e, "purge task failed"),
            }
        }
    }

    /// Standalone startup: purge the stored state of quorum groups that a
    /// delete superseded (a crash between recording the new incarnation and
    /// finishing the purge leaves some behind). Groups of names never
    /// deleted are untouched.
    pub async fn gc_quorum_groups(&self) {
        if self.cluster.get().is_some() {
            return;
        }
        let Some(persist) = self.persist_factory().await else {
            return;
        };
        let p = persist.clone();
        let stored = tokio::task::spawn_blocking(move || p.groups())
            .await
            .ok()
            .and_then(Result::ok)
            .unwrap_or_default();
        for group in stored {
            let Some(gid) = group.strip_prefix("queue/") else {
                continue;
            };
            let (name, inc) = crate::cluster::parse_group_id(gid);
            if self
                .standalone_incarnation(name)
                .await
                .is_some_and(|current| inc < current)
            {
                self.purge_standalone_group(gid).await;
            }
        }
    }

    /// Clustered: stop this node's proxies for quorum queues that were
    /// deleted (by any node) — they detach their consumers with
    /// `amqp:resource-deleted` — so the next attach declares afresh.
    pub async fn reconcile_quorum(&self) {
        let Some(node) = self.cluster.get().cloned() else {
            return;
        };
        let served: Vec<(String, u64)> = self
            .quorum_incarnations
            .lock()
            .expect("incarnations lock")
            .iter()
            .map(|(n, i)| (n.clone(), *i))
            .collect();
        for (name, served_inc) in served {
            // A delete always moves the name to a newer incarnation (even
            // if it was re-declared since), so that alone says "gone".
            if node.queue_incarnation(&name) == served_inc {
                continue;
            }
            let handle = self.lookup(QueueKind::Quorum, &name);
            self.quorum_incarnations
                .lock()
                .expect("incarnations lock")
                .remove(&name);
            if let Some(handle) = handle {
                // Bounded: a proxy busy re-binding notices the delete itself.
                let (tx, rx) = tokio::sync::oneshot::channel();
                if tokio::time::timeout(
                    std::time::Duration::from_secs(2),
                    handle.tx.send(QueueMsg::Delete { reply: tx }),
                )
                .await
                .is_ok_and(|sent| sent.is_ok())
                {
                    let _ = tokio::time::timeout(std::time::Duration::from_secs(2), rx).await;
                }
                self.evict(QueueKind::Quorum, &name, &handle);
            }
        }
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
        let key = Self::key_for(kind, name);
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
                                self.quorum_incarnations
                                    .lock()
                                    .expect("incarnations lock")
                                    .insert(name.to_owned(), node.queue_incarnation(name));
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
                                let group = crate::cluster::group_id(
                                    name,
                                    self.standalone_incarnation(name).await.ok_or(InitAbort::Failed)?,
                                );
                                spawn_quorum_group(
                                    name.to_owned(),
                                    &group,
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
    group: &str,
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
        group,
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

    /// Review L10: an unreadable definitions row is moved aside (with an
    /// error) instead of blocking every exchange; good rows still load.
    #[cfg(feature = "store-redb")]
    #[tokio::test]
    async fn corrupt_exchange_rows_are_moved_aside() {
        let dir = tempfile::tempdir().unwrap();
        let config = BrokerConfig {
            data_dir: Some(dir.path().to_path_buf()),
            ..Default::default()
        };
        let r = QueueRegistry::new(&config);
        let store = r.store().await.expect("store opens");
        let good = crate::exchange::ExchangeDef {
            kind: crate::exchange::ExchangeKind::Topic,
            alternate: None,
            bindings: Vec::new(),
        };
        store
            .write_definitions(
                &[
                    (
                        "exchange:/good".to_owned(),
                        serde_json::to_vec(&good).unwrap(),
                    ),
                    ("exchange:/bad".to_owned(), b"{oops".to_vec()),
                ],
                &[],
            )
            .unwrap();
        assert!(r.ensure_exchanges_loaded().await, "loading is not blocked");
        let defs = r.exchanges().snapshot();
        assert_eq!(defs.get("/good"), Some(&good));
        assert!(!defs.contains_key("/bad"));
        let rows = store.definitions("exchange:").unwrap();
        assert_eq!(rows.len(), 1, "the corrupt row left the live set");
        let aside = store.definitions("corrupt:").unwrap();
        assert_eq!(aside[0].0, "corrupt:exchange:/bad");
    }

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
