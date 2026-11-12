//! The broker entry point: bind a listener, accept connections, one owning
//! task per connection.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{Semaphore, watch};

use ramqp_core::error::ConnectError;
use ramqp_core::transport::IoStream;

use crate::auth::{AllowAll, Authenticator};
use crate::cluster::node::{ClusterNode, NodeSettings};
use crate::config::BrokerConfig;
use crate::connection;
use crate::exchange::{ExchangeError, ExchangeKind, ExchangeOp};
use crate::mgmt::{self, BrokerCounters};
use crate::policy::{self, DeadLetterSender};
use crate::registry::QueueRegistry;

/// A broker instance under construction.
#[derive(Clone)]
pub struct Broker {
    config: Arc<BrokerConfig>,
    auth: Arc<dyn Authenticator>,
    registry: Arc<QueueRegistry>,
    dlx: DeadLetterSender,
    counters: Arc<BrokerCounters>,
}

impl std::fmt::Debug for Broker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Broker")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl Broker {
    /// Create a broker with the given configuration (and [`AllowAll`] auth —
    /// swap it with [`Broker::with_authenticator`] for anything real).
    pub fn new(config: BrokerConfig) -> Self {
        let registry = Arc::new(QueueRegistry::new(&config));
        let dlx = policy::spawn_dlx_router(&registry);
        registry.set_dlx(dlx.clone());
        Broker {
            config: Arc::new(config),
            auth: Arc::new(AllowAll),
            registry,
            dlx,
            counters: Arc::new(BrokerCounters::default()),
        }
    }

    /// Replace the authenticator.
    pub fn with_authenticator(mut self, auth: Arc<dyn Authenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Authenticate with `users` (the configuration's) plus users and
    /// permissions created through the management API (`PUT /users/...`,
    /// `PUT /permissions/...`), which persist (store / metadata group).
    /// Deleting a runtime user closes its open connections.
    pub fn with_runtime_users(mut self, users: crate::auth::StaticUsers) -> Self {
        let users = Arc::new(crate::runtime_users::RuntimeUsers::new(users));
        self.registry.set_runtime_users(users.clone());
        self.auth = users;
        self
    }

    /// Bind a TCP listener (e.g. `"0.0.0.0:5672"` or `"127.0.0.1:0"`). An
    /// empty `addr` binds no plain listener — TLS-only, which needs
    /// [`BrokerConfig::tls_listen`].
    ///
    /// When the config carries a [`crate::config::ClusterMemberConfig`], this
    /// also starts the node's cluster half: the fabric listener, the
    /// metadata-group member, and (on the lowest seed) cluster formation.
    pub async fn bind(self, addr: &str) -> std::io::Result<BoundBroker> {
        let wants_tls = self.config.tls_listen.is_some()
            || self.config.management_tls
            || self
                .config
                .cluster
                .as_ref()
                .is_some_and(|c| c.tls.is_some());
        if wants_tls && !cfg!(any(feature = "tls", feature = "fips")) {
            return Err(std::io::Error::other(
                "TLS is configured but this build has neither the `tls` nor the `fips` feature",
            ));
        }
        if (self.config.tls_listen.is_some() || self.config.management_tls)
            && self.config.tls.is_none()
        {
            return Err(std::io::Error::other(
                "tls_listen / management_tls need BrokerConfig::tls (certificate and key)",
            ));
        }
        if let Some(cluster) = &self.config.cluster
            && self.registry.cluster().is_none()
        {
            let persist = self.registry.persist_factory().await;
            // With the store feature + a data dir, a clustered bind REQUIRES
            // the store: silently starting with an empty metadata group
            // would shadow persisted state (e.g. while a previous
            // instance's file lock lingers). Fail the bind; callers retry.
            #[cfg(feature = "store-redb")]
            if self.config.data_dir.is_some() && persist.is_none() {
                return Err(std::io::Error::other(
                    "durable store not openable (previous instance still holds the lock?)",
                ));
            }
            let node = ClusterNode::bootstrap(NodeSettings {
                node_id: cluster.node_id,
                listen: cluster.listen.clone(),
                seeds: cluster.seeds.clone(),
                replicas: cluster.replicas,
                max_queue_depth: self.config.max_queue_depth,
                max_queue_bytes: self.config.max_queue_bytes,
                data_dir: self.config.data_dir.clone(),
                resident_bytes_max: self.config.resident_bytes_max,
                policies: self.config.policies.clone(),
                dlx: Some(self.dlx.clone()),
                persist,
                tls: cluster.tls.clone(),
            })
            .await?;
            self.registry.set_cluster(node.clone());
            self.registry.install_policy_resolver();
            self.registry.sync_defs();
            // Keep this node's exchange routing view in step with the
            // metadata group: re-sync whenever it applies something.
            let mut changes = node.meta_changes();
            let weak = Arc::downgrade(&self.registry);
            let node_for_watch = node.clone();
            tokio::spawn(async move {
                // Quorum-queue deletes: re-run the teardown when the
                // incarnations change, and periodically (a purge that failed
                // — e.g. a busy disk — is retried; a node that was down
                // catches up here).
                let mut seen_incarnations = None;
                let mut retry = tokio::time::interval(std::time::Duration::from_secs(60));
                loop {
                    let Some(registry) = weak.upgrade() else {
                        return;
                    };
                    registry.sync_exchanges();
                    registry.sync_defs();
                    let incarnations = node_for_watch.incarnations();
                    if seen_incarnations.as_ref() != Some(&incarnations) {
                        registry.reconcile_quorum().await;
                        node_for_watch.reconcile().await;
                        seen_incarnations = Some(incarnations);
                    }
                    drop(registry);
                    tokio::select! {
                        changed = changes.changed() => {
                            if changed.is_err() {
                                return;
                            }
                        }
                        _ = retry.tick() => {
                            seen_incarnations = None;
                        }
                    }
                }
            });
        } else {
            // Standalone: finish any quorum-queue delete a crash interrupted.
            self.registry.gc_quorum_groups().await;
            // Runtime definitions (per-queue policies, users, permissions):
            // retried in the background if the store is not openable yet.
            if let Err(e) = self.registry.load_defs().await {
                tracing::warn!(error = %e, "runtime definitions not loaded yet; retrying");
                let weak = Arc::downgrade(&self.registry);
                tokio::spawn(async move {
                    loop {
                        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                        let Some(registry) = weak.upgrade() else {
                            return;
                        };
                        if registry.load_defs().await.is_ok() {
                            tracing::info!("runtime definitions loaded");
                            return;
                        }
                    }
                });
            }
        }
        // Start the queues on disk once the on-disk state is loaded (#50):
        // until then a restarted broker's queue listing and `/metrics` miss
        // every persisted backlog nobody has attached to yet. In the
        // background: recovery reads stores and starts Raft groups.
        {
            let weak = Arc::downgrade(&self.registry);
            tokio::spawn(async move {
                loop {
                    let Some(registry) = weak.upgrade() else {
                        return;
                    };
                    if registry.ensure_defs_loaded().await {
                        registry.recover_disk_queues().await;
                        return;
                    }
                    drop(registry);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            });
        }
        if self.registry.cluster().is_none() && !self.registry.ensure_exchanges_loaded().await {
            // Standalone with a store that is not openable yet (e.g. the
            // previous instance's lock lingers): exchange attaches/changes
            // are refused, and the load retried on each, until it is.
            tracing::warn!(
                "exchange definitions not loaded yet; exchange use refused until they are"
            );
        }
        // The management endpoint (metrics + queue inspection), if enabled.
        if let Some(mgmt_addr) = &self.config.management_listen {
            let mgmt_listener = TcpListener::bind(mgmt_addr).await?;
            tracing::info!(addr = %mgmt_listener.local_addr()?, "management endpoint listening");
            if self.config.management_token.is_none() {
                tracing::info!(
                    "management API: no token configured — writes need an admin user's \
                     Basic credentials (read-only if there are none)"
                );
            }
            let acceptor = if self.config.management_tls {
                tls_mgmt_acceptor(self.config.tls.as_ref().expect("checked above"))?
            } else {
                mgmt::plain_acceptor()
            };
            mgmt::spawn_mgmt(
                mgmt_listener,
                acceptor,
                Arc::downgrade(&self.registry),
                self.counters.clone(),
                Arc::new(mgmt::MgmtAuth {
                    token: self.config.management_token.clone(),
                    auth_reads: self.config.management_auth_reads,
                    authenticator: self.auth.clone(),
                }),
            );
        }
        if addr.is_empty() && self.config.tls_listen.is_none() {
            return Err(std::io::Error::other(
                "no listener: an empty plain address needs tls_listen (TLS-only mode)",
            ));
        }
        let listener = if addr.is_empty() {
            None
        } else {
            Some(TcpListener::bind(addr).await?)
        };
        let tls = match &self.config.tls_listen {
            Some(tls_addr) => Some(
                TlsListener::bind(tls_addr, self.config.tls.as_ref().expect("checked above"))
                    .await?,
            ),
            None => None,
        };
        let local_addr = match (&listener, &tls) {
            (Some(l), _) => l.local_addr()?,
            (None, Some(t)) => t.listener.local_addr()?,
            (None, None) => unreachable!("checked above"),
        };
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // A `0` cap disables the limit (unbounded); otherwise a permit is held
        // for each connection's lifetime, bounding concurrent connections.
        let conn_limit = match self.config.max_connections {
            0 => None,
            n => Some(Arc::new(Semaphore::new(n))),
        };
        #[cfg(any(feature = "tls", feature = "fips"))]
        let upgrade = match (&tls, &self.config.tls) {
            (Some(t), _) => Some(UpgradeAcceptor(t.acceptor.clone())),
            (None, Some(cfg)) => Some(UpgradeAcceptor(
                crate::tls::acceptor(cfg).map_err(std::io::Error::other)?,
            )),
            (None, None) => None,
        };
        Ok(BoundBroker {
            broker: self,
            listener,
            tls,
            #[cfg(any(feature = "tls", feature = "fips"))]
            upgrade,
            local_addr,
            shutdown_tx,
            shutdown_rx,
            conn_limit,
        })
    }

    /// Which node currently leads the queue group behind `address`, when
    /// this broker is clustered. Diagnostics/testing surface; the management
    /// API (Phase 9) will supersede it.
    #[doc(hidden)]
    pub async fn queue_leader(&self, address: &str) -> Option<u64> {
        let (_, name) = QueueRegistry::parse_address(address)?;
        self.registry.cluster()?.resolve_queue_leader(name).await
    }

    /// Wait until the runtime definitions are loaded (standalone with a
    /// store; immediate otherwise). Queues are not declared before. `true`
    /// once loaded, `false` on timeout.
    #[doc(hidden)]
    pub async fn definitions_ready(&self, timeout: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            if self.registry.ensure_defs_loaded().await {
                return true;
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
    }

    /// Wait until the cluster (when configured) has formed. `true` once a
    /// metadata leader exists; `false` on timeout or when not clustered.
    #[doc(hidden)]
    pub async fn cluster_formed(&self, timeout: std::time::Duration) -> bool {
        match self.registry.cluster() {
            Some(node) => node.await_membership(timeout).await.is_ok(),
            None => false,
        }
    }

    /// Declare a queue by address (`/queues/x`, `/quorum/x`, `/durable/x`)
    /// within `vhost` (empty = default). Idempotent: an existing queue is
    /// left as is. Used for startup topology from the config file and the
    /// management API.
    pub async fn declare_queue(&self, vhost: &str, address: &str) -> Result<(), String> {
        if !QueueRegistry::is_queue_address(address) {
            return Err(format!(
                "{address:?} is not a queue address (expected /queues/<name>, \
                 /quorum/<name> or /durable/<name>)"
            ));
        }
        // Explicit: `/queues/<name>` here declares a TRANSIENT queue (it
        // never means "whatever exists", unlike a client attach).
        let (kind, name) = QueueRegistry::parse_address(address)
            .ok_or_else(|| format!("{address:?} is not a queue address"))?;
        match self.registry.declare_in(vhost, kind, name).await {
            Ok(_) => Ok(()),
            Err(crate::registry::ResolveError::OtherKind(existing)) => Err(format!(
                "{address:?}: a {existing:?} queue already uses that name in vhost {vhost:?}"
            )),
            Err(e) => Err(format!(
                "could not declare {address:?} in vhost {vhost:?}: {e:?}"
            )),
        }
    }

    /// Declare an exchange in `vhost`. Idempotent: redeclaring with the
    /// same kind and alternate succeeds (`Ok(false)`); different arguments
    /// are a [`ExchangeError::Conflict`]. `alternate` is a queue address for
    /// unroutable messages. Persisted (store / metadata group) when the
    /// broker has one.
    pub async fn declare_exchange(
        &self,
        vhost: &str,
        name: &str,
        kind: ExchangeKind,
        alternate: Option<&str>,
    ) -> Result<bool, ExchangeError> {
        self.registry
            .exchange_op(ExchangeOp::Declare {
                vhost: vhost.to_owned(),
                name: name.to_owned(),
                kind,
                alternate: alternate.map(str::to_owned),
            })
            .await
    }

    /// Delete an exchange and its bindings ([`ExchangeError::NotFound`] if
    /// absent). Producers attached to it see their messages as unroutable.
    pub async fn delete_exchange(&self, vhost: &str, name: &str) -> Result<bool, ExchangeError> {
        self.registry
            .exchange_op(ExchangeOp::Delete {
                vhost: vhost.to_owned(),
                name: name.to_owned(),
            })
            .await
    }

    /// Bind `destination` (a queue address in the same vhost) to an
    /// exchange with `key`. Idempotent (`Ok(false)` if already bound).
    pub async fn bind_queue(
        &self,
        vhost: &str,
        exchange: &str,
        destination: &str,
        key: &str,
    ) -> Result<bool, ExchangeError> {
        self.registry
            .exchange_op(ExchangeOp::Bind {
                vhost: vhost.to_owned(),
                exchange: exchange.to_owned(),
                destination: destination.to_owned(),
                key: key.to_owned(),
            })
            .await
    }

    /// Remove a binding ([`ExchangeError::NotFound`] if absent).
    pub async fn unbind_queue(
        &self,
        vhost: &str,
        exchange: &str,
        destination: &str,
        key: &str,
    ) -> Result<bool, ExchangeError> {
        self.registry
            .exchange_op(ExchangeOp::Unbind {
                vhost: vhost.to_owned(),
                exchange: exchange.to_owned(),
                destination: destination.to_owned(),
                key: key.to_owned(),
            })
            .await
    }

    /// Serve a single already-established byte stream (in-process transports,
    /// tests, or custom acceptors). The returned task resolves when the
    /// connection completes.
    pub fn serve_stream<S: IoStream + 'static>(
        &self,
        stream: S,
        shutdown: watch::Receiver<bool>,
    ) -> tokio::task::JoinHandle<Result<(), ConnectError>> {
        let config = self.config.clone();
        let auth = self.auth.clone();
        let registry = self.registry.clone();
        tokio::spawn(
            async move { connection::serve(stream, config, auth, registry, shutdown).await },
        )
    }
}

/// A broker bound to a listener, ready to [`run`](BoundBroker::run).
#[derive(Debug)]
pub struct BoundBroker {
    broker: Broker,
    /// The plain AMQP listener (`None` in TLS-only mode).
    listener: Option<TcpListener>,
    /// The `amqps` listener, when `tls_listen` is configured.
    tls: Option<TlsListener>,
    /// TLS for clients that negotiate it on the PLAIN listener (the AMQP TLS
    /// protocol header), when a certificate is configured.
    #[cfg(any(feature = "tls", feature = "fips"))]
    upgrade: Option<UpgradeAcceptor>,
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    /// Bounds concurrent connections (a permit per live connection). `None`
    /// when the cap is disabled (`max_connections == 0`).
    conn_limit: Option<Arc<Semaphore>>,
}

/// The AMQP TLS listener and its handshake acceptor.
struct TlsListener {
    listener: TcpListener,
    #[cfg(any(feature = "tls", feature = "fips"))]
    acceptor: tokio_rustls::TlsAcceptor,
}

impl std::fmt::Debug for TlsListener {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TlsListener")
            .field("addr", &self.listener.local_addr().ok())
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "tls", feature = "fips"))]
impl TlsListener {
    async fn bind(addr: &str, tls: &crate::config::TlsServerConfig) -> std::io::Result<Self> {
        let acceptor = crate::tls::acceptor(tls).map_err(std::io::Error::other)?;
        let listener = TcpListener::bind(addr).await?;
        tracing::info!(addr = %listener.local_addr()?, "AMQP TLS listener bound");
        Ok(TlsListener { listener, acceptor })
    }
}

#[cfg(not(any(feature = "tls", feature = "fips")))]
impl TlsListener {
    async fn bind(_addr: &str, _tls: &crate::config::TlsServerConfig) -> std::io::Result<Self> {
        Err(std::io::Error::other("built without TLS"))
    }
}

/// The management endpoint's TLS acceptor.
#[cfg(any(feature = "tls", feature = "fips"))]
fn tls_mgmt_acceptor(tls: &crate::config::TlsServerConfig) -> std::io::Result<mgmt::Acceptor> {
    // The management port authenticates with tokens / admin users; it never
    // demands client certificates (Prometheus and KEDA would need them).
    let mut tls = tls.clone();
    tls.client_ca_file = None;
    let acceptor = crate::tls::acceptor(&tls).map_err(std::io::Error::other)?;
    Ok(Arc::new(move |stream: TcpStream| {
        let acceptor = acceptor.clone();
        Box::pin(async move {
            match tokio::time::timeout(mgmt::TLS_HANDSHAKE_TIMEOUT, acceptor.accept(stream)).await {
                Ok(Ok(s)) => Some(Box::new(s) as Box<dyn mgmt::MgmtStream>),
                Ok(Err(e)) => {
                    tracing::debug!(error = %e, "management TLS handshake failed");
                    None
                }
                Err(_) => None,
            }
        })
    }))
}

#[cfg(not(any(feature = "tls", feature = "fips")))]
fn tls_mgmt_acceptor(_tls: &crate::config::TlsServerConfig) -> std::io::Result<mgmt::Acceptor> {
    Err(std::io::Error::other("built without TLS"))
}

/// Accept on the plain listener, or never (TLS-only).
async fn accept_plain(listener: Option<&TcpListener>) -> std::io::Result<(TcpStream, SocketAddr)> {
    match listener {
        Some(l) => l.accept().await,
        None => std::future::pending().await,
    }
}

/// The TLS acceptor for plain-port upgrades (`Debug` for [`BoundBroker`]).
#[cfg(any(feature = "tls", feature = "fips"))]
#[derive(Clone)]
struct UpgradeAcceptor(tokio_rustls::TlsAcceptor);

#[cfg(any(feature = "tls", feature = "fips"))]
impl std::fmt::Debug for UpgradeAcceptor {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("UpgradeAcceptor")
    }
}

/// The AMQP 1.0 TLS protocol header (§5.2.1): a client that negotiates TLS
/// the spec's default way sends it first and expects it echoed before the
/// TLS handshake.
#[cfg(any(feature = "tls", feature = "fips"))]
const TLS_PROTO_HEADER: [u8; 8] = *b"AMQP\x02\x01\x00\x00";

/// Peek (without consuming) whether the client opened with the TLS protocol
/// header. Waits for the first 8 bytes; `false` on anything else (plain
/// AMQP/SASL headers, or a raw TLS ClientHello).
#[cfg(any(feature = "tls", feature = "fips"))]
async fn opens_with_tls_header(stream: &TcpStream) -> std::io::Result<bool> {
    let mut buf = [0u8; 8];
    loop {
        let n = stream.peek(&mut buf).await?;
        if n == 0 {
            return Ok(false);
        }
        if buf[..n] != TLS_PROTO_HEADER[..n] {
            return Ok(false);
        }
        if n == buf.len() {
            return Ok(true);
        }
        // A partial header so far: wait for the rest without spinning.
        tokio::time::sleep(std::time::Duration::from_millis(1)).await;
    }
}

/// Establish TLS on an accepted socket, both ways the spec allows: if the
/// client sent the TLS protocol header, consume it and echo it, then run the
/// handshake (fe2o3-amqp and other spec-default clients); otherwise run the
/// handshake directly (RabbitMQ-style `amqps`: Qpid JMS/Proton, ramqp).
#[cfg(any(feature = "tls", feature = "fips"))]
async fn establish_tls(
    mut stream: TcpStream,
    acceptor: &tokio_rustls::TlsAcceptor,
) -> std::io::Result<tokio_rustls::server::TlsStream<TcpStream>> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    if opens_with_tls_header(&stream).await? {
        let mut header = [0u8; 8];
        stream.read_exact(&mut header).await?;
        stream.write_all(&TLS_PROTO_HEADER).await?;
    }
    acceptor.accept(stream).await
}

/// Accept on the TLS listener, or never (no TLS listener).
async fn accept_tls(tls: Option<&TlsListener>) -> std::io::Result<(TcpStream, SocketAddr)> {
    match tls {
        Some(t) => t.listener.accept().await,
        None => std::future::pending().await,
    }
}

/// Signals a running broker to shut down.
#[derive(Debug, Clone)]
pub struct ShutdownHandle(watch::Sender<bool>);

impl ShutdownHandle {
    /// Begin shutdown: [`BoundBroker::run`] stops accepting and returns, and
    /// every live connection observes the same signal and closes gracefully
    /// (sending `close`). Connections close *asynchronously* — `run()` does not
    /// block on them, so a caller that needs to wait for full drain should
    /// track the connection tasks itself.
    pub fn shutdown(&self) {
        let _ = self.0.send(true);
    }
}

impl BoundBroker {
    /// The broker behind this listener (for declarations and diagnostics
    /// while it runs; cheap to clone).
    pub fn broker(&self) -> &Broker {
        &self.broker
    }

    /// The bound listen address (useful with port `0`): the plain listener,
    /// or the TLS one in TLS-only mode.
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// The bound TLS listen address, when `tls_listen` is configured.
    pub fn tls_local_addr(&self) -> Option<SocketAddr> {
        self.tls.as_ref().and_then(|t| t.listener.local_addr().ok())
    }

    /// A handle that can stop the broker from another task.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown_tx.clone())
    }

    /// Accept connections until shut down. Each connection runs in its own
    /// owning task (no shared state, no locks on the frame path).
    pub async fn run(mut self) -> std::io::Result<()> {
        tracing::info!(addr = %self.local_addr, "ramqp-broker listening");
        loop {
            tokio::select! {
                accepted = accept_plain(self.listener.as_ref()) => {
                    match accepted {
                        Ok((stream, peer)) => {
                            let Some(permit) = self.admit(peer) else {
                                drop(stream);
                                continue;
                            };
                            let _ = stream.set_nodelay(true);
                            self.spawn_plain_connection(stream, peer, permit);
                        }
                        // A per-accept error (fd exhaustion, a connection reset
                        // before accept, etc.) is transient — log it and keep
                        // serving rather than killing the whole broker. A short
                        // pause avoids a busy-spin if the condition persists.
                        Err(e) => {
                            tracing::warn!(error = %e, "accept error; continuing");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
                accepted = accept_tls(self.tls.as_ref()) => {
                    match accepted {
                        Ok((stream, peer)) => {
                            let Some(permit) = self.admit(peer) else {
                                drop(stream);
                                continue;
                            };
                            let _ = stream.set_nodelay(true);
                            self.spawn_tls_connection(stream, peer, permit);
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, "TLS accept error; continuing");
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                    }
                }
                _ = self.shutdown_rx.changed() => {
                    tracing::info!("broker shutting down");
                    // Stop the cluster half too: fabric listener, Raft
                    // members, leader-local actors. Abrupt by design — a
                    // shut-down node must look dead to its peers so their
                    // groups re-elect.
                    if let Some(node) = self.broker.registry.cluster() {
                        node.stop().await;
                    }
                    return Ok(());
                }
            }
        }
    }

    /// A connection permit, or `None` at the cap (the caller drops the
    /// socket rather than spawn unbounded per-connection state — DoS
    /// guard). `Some(None)` when the cap is disabled.
    #[allow(clippy::option_option)]
    fn admit(&self, peer: SocketAddr) -> Option<Option<tokio::sync::OwnedSemaphorePermit>> {
        match &self.conn_limit {
            Some(sem) => match sem.clone().try_acquire_owned() {
                Ok(p) => Some(Some(p)),
                Err(_) => {
                    tracing::warn!(
                        %peer,
                        limit = self.broker.config.max_connections,
                        "connection limit reached; refusing"
                    );
                    None
                }
            },
            None => Some(None),
        }
    }

    /// Serve an `amqps` connection: the TLS handshake runs in the
    /// connection's own task (bounded by the inbound-handshake timeout), so
    /// a slow handshake never stalls the accept loop.
    #[cfg(any(feature = "tls", feature = "fips"))]
    fn spawn_tls_connection(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        let acceptor = self
            .tls
            .as_ref()
            .expect("TLS accept implies a TLS listener")
            .acceptor
            .clone();
        let timeout = self
            .broker
            .config
            .connection
            .connect_timeout
            .unwrap_or(std::time::Duration::from_secs(30));
        self.spawn_connection(
            async move {
                match tokio::time::timeout(timeout, establish_tls(stream, &acceptor)).await {
                    Ok(Ok(tls)) => Some(tls),
                    Ok(Err(e)) => {
                        tracing::debug!(%peer, error = %e, "TLS handshake failed");
                        None
                    }
                    Err(_) => {
                        tracing::debug!(%peer, "TLS handshake timed out");
                        None
                    }
                }
            },
            peer,
            permit,
        );
    }

    /// Serve a connection from the plain listener. With a certificate
    /// configured, a client that opens with the AMQP TLS protocol header is
    /// upgraded to TLS in place (the spec's negotiated TLS on the plain
    /// port); everyone else stays plain.
    #[cfg(any(feature = "tls", feature = "fips"))]
    fn spawn_plain_connection(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        let Some(UpgradeAcceptor(acceptor)) = self.upgrade.clone() else {
            self.spawn_connection(async move { Some(stream) }, peer, permit);
            return;
        };
        let timeout = self
            .broker
            .config
            .connection
            .connect_timeout
            .unwrap_or(std::time::Duration::from_secs(30));
        self.spawn_connection(
            async move {
                let upgrade = tokio::time::timeout(timeout, opens_with_tls_header(&stream))
                    .await
                    .ok()?
                    .ok()?;
                if !upgrade {
                    return Some(Box::new(stream) as Box<dyn IoStream>);
                }
                match tokio::time::timeout(timeout, establish_tls(stream, &acceptor)).await {
                    Ok(Ok(tls)) => Some(Box::new(tls) as Box<dyn IoStream>),
                    Ok(Err(e)) => {
                        tracing::debug!(%peer, error = %e, "TLS upgrade failed");
                        None
                    }
                    Err(_) => None,
                }
            },
            peer,
            permit,
        );
    }

    #[cfg(not(any(feature = "tls", feature = "fips")))]
    fn spawn_plain_connection(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        self.spawn_connection(async move { Some(stream) }, peer, permit);
    }

    #[cfg(not(any(feature = "tls", feature = "fips")))]
    fn spawn_tls_connection(
        &self,
        _stream: TcpStream,
        _peer: SocketAddr,
        _permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        unreachable!("no TLS listener without the tls feature")
    }

    fn spawn_connection<S, F>(
        &self,
        stream: F,
        peer: SocketAddr,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) where
        S: IoStream + 'static,
        F: Future<Output = Option<S>> + Send + 'static,
    {
        let config = self.broker.config.clone();
        let auth = self.broker.auth.clone();
        let registry = self.broker.registry.clone();
        let shutdown = self.shutdown_rx.clone();
        let counters = self.broker.counters.clone();
        counters
            .connections
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        counters
            .connections_total
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        tokio::spawn(async move {
            if let Some(stream) = stream.await {
                match connection::serve(stream, config, auth, registry, shutdown).await {
                    Ok(()) => tracing::debug!(%peer, "connection closed"),
                    Err(e) => tracing::debug!(%peer, error = %e, "connection failed"),
                }
            }
            counters
                .connections
                .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
            // Held until here: the permit is released when the connection ends.
            drop(permit);
        });
    }
}
