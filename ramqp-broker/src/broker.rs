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
use crate::policy::{self, DeadLetterSender};
use crate::registry::QueueRegistry;

/// A broker instance under construction.
#[derive(Clone)]
pub struct Broker {
    config: Arc<BrokerConfig>,
    auth: Arc<dyn Authenticator>,
    registry: Arc<QueueRegistry>,
    dlx: DeadLetterSender,
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
        }
    }

    /// Replace the authenticator.
    pub fn with_authenticator(mut self, auth: Arc<dyn Authenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Bind a TCP listener (e.g. `"0.0.0.0:5672"` or `"127.0.0.1:0"`).
    ///
    /// When the config carries a [`crate::config::ClusterMemberConfig`], this
    /// also starts the node's cluster half: the fabric listener, the
    /// metadata-group member, and (on the lowest seed) cluster formation.
    pub async fn bind(self, addr: &str) -> std::io::Result<BoundBroker> {
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
                data_dir: self.config.data_dir.clone(),
                resident_bytes_max: self.config.resident_bytes_max,
                policies: self.config.policies.clone(),
                dlx: Some(self.dlx.clone()),
                persist,
            })
            .await?;
            self.registry.set_cluster(node);
        }
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        // A `0` cap disables the limit (unbounded); otherwise a permit is held
        // for each connection's lifetime, bounding concurrent connections.
        let conn_limit = match self.config.max_connections {
            0 => None,
            n => Some(Arc::new(Semaphore::new(n))),
        };
        Ok(BoundBroker {
            broker: self,
            listener,
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

    /// Wait until the cluster (when configured) has formed. `true` once a
    /// metadata leader exists; `false` on timeout or when not clustered.
    #[doc(hidden)]
    pub async fn cluster_formed(&self, timeout: std::time::Duration) -> bool {
        match self.registry.cluster() {
            Some(node) => node.await_membership(timeout).await.is_ok(),
            None => false,
        }
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
    listener: TcpListener,
    local_addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
    shutdown_rx: watch::Receiver<bool>,
    /// Bounds concurrent connections (a permit per live connection). `None`
    /// when the cap is disabled (`max_connections == 0`).
    conn_limit: Option<Arc<Semaphore>>,
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
    /// The bound listen address (useful with port `0`).
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
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
                accepted = self.listener.accept() => {
                    match accepted {
                        Ok((stream, peer)) => {
                            // Acquire a connection permit first; at the cap we
                            // drop the socket immediately rather than spawn
                            // unbounded per-connection state (DoS guard).
                            let permit = match &self.conn_limit {
                                Some(sem) => match sem.clone().try_acquire_owned() {
                                    Ok(p) => Some(p),
                                    Err(_) => {
                                        tracing::warn!(
                                            %peer,
                                            limit = self.broker.config.max_connections,
                                            "connection limit reached; refusing"
                                        );
                                        drop(stream);
                                        continue;
                                    }
                                },
                                None => None,
                            };
                            let _ = stream.set_nodelay(true);
                            self.spawn_connection(stream, peer, permit);
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

    fn spawn_connection(
        &self,
        stream: TcpStream,
        peer: SocketAddr,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        let config = self.broker.config.clone();
        let auth = self.broker.auth.clone();
        let registry = self.broker.registry.clone();
        let shutdown = self.shutdown_rx.clone();
        tokio::spawn(async move {
            match connection::serve(stream, config, auth, registry, shutdown).await {
                Ok(()) => tracing::debug!(%peer, "connection closed"),
                Err(e) => tracing::debug!(%peer, error = %e, "connection failed"),
            }
            // Held until here: the permit is released when the connection ends.
            drop(permit);
        });
    }
}
