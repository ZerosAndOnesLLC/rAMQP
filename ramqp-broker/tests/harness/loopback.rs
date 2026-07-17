//! Loopback-broker starter: bind a broker on an ephemeral `127.0.0.1` port and
//! hand back an address plus a self-shutting handle.

use ramqp_broker::{Broker, BrokerConfig, ShutdownHandle};

/// A running loopback broker plus the handle that stops it. Dropping it shuts
/// the broker down, so a test can simply keep it in scope; call
/// [`shutdown`](Loopback::shutdown) to stop it early (e.g. to observe a peer's
/// reaction to the broker going away).
pub struct Loopback {
    pub addr: std::net::SocketAddr,
    shutdown: Option<ShutdownHandle>,
}

impl Loopback {
    /// The `amqp://host:port` URL a `ramqp` client connects to.
    pub fn url(&self) -> String {
        format!("amqp://{}", self.addr)
    }

    /// Stop the broker now (rather than at drop).
    pub fn shutdown(mut self) {
        if let Some(h) = self.shutdown.take() {
            h.shutdown();
        }
    }
}

impl Drop for Loopback {
    fn drop(&mut self) {
        if let Some(h) = self.shutdown.take() {
            h.shutdown();
        }
    }
}

/// Start a default-config broker on an ephemeral loopback port.
pub async fn loopback() -> Loopback {
    loopback_with(Broker::new(BrokerConfig::default())).await
}

/// Start a caller-configured broker on an ephemeral loopback port.
pub async fn loopback_with(broker: Broker) -> Loopback {
    let bound = broker.bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    Loopback {
        addr,
        shutdown: Some(shutdown),
    }
}
