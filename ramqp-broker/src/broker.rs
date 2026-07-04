//! The broker entry point: bind a listener, accept connections, one owning
//! task per connection.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;

use ramqp_core::error::ConnectError;
use ramqp_core::transport::IoStream;

use crate::auth::{AllowAll, Authenticator};
use crate::config::BrokerConfig;
use crate::connection;

/// A broker instance under construction.
#[derive(Clone)]
pub struct Broker {
    config: Arc<BrokerConfig>,
    auth: Arc<dyn Authenticator>,
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
        Broker {
            config: Arc::new(config),
            auth: Arc::new(AllowAll),
        }
    }

    /// Replace the authenticator.
    pub fn with_authenticator(mut self, auth: Arc<dyn Authenticator>) -> Self {
        self.auth = auth;
        self
    }

    /// Bind a TCP listener (e.g. `"0.0.0.0:5672"` or `"127.0.0.1:0"`).
    pub async fn bind(self, addr: &str) -> std::io::Result<BoundBroker> {
        let listener = TcpListener::bind(addr).await?;
        let local_addr = listener.local_addr()?;
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        Ok(BoundBroker {
            broker: self,
            listener,
            local_addr,
            shutdown_tx,
            shutdown_rx,
        })
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
        tokio::spawn(async move { connection::serve(stream, config, auth, shutdown).await })
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
}

/// Signals a running broker to shut down (close listeners + connections).
#[derive(Debug, Clone)]
pub struct ShutdownHandle(watch::Sender<bool>);

impl ShutdownHandle {
    /// Begin shutdown: stop accepting and close every connection gracefully.
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
                    let (stream, peer) = accepted?;
                    let _ = stream.set_nodelay(true);
                    self.spawn_connection(stream, peer);
                }
                _ = self.shutdown_rx.changed() => {
                    tracing::info!("broker shutting down");
                    return Ok(());
                }
            }
        }
    }

    fn spawn_connection(&self, stream: TcpStream, peer: SocketAddr) {
        let config = self.broker.config.clone();
        let auth = self.broker.auth.clone();
        let shutdown = self.shutdown_rx.clone();
        tokio::spawn(async move {
            match connection::serve(stream, config, auth, shutdown).await {
                Ok(()) => tracing::debug!(%peer, "connection closed"),
                Err(e) => tracing::debug!(%peer, error = %e, "connection failed"),
            }
        });
    }
}
