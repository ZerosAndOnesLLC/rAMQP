//! The connection builder + presets (WP-5.1).

use crate::api::connection::Connection;
use crate::config::Config;
use crate::error::ConnectError;
use crate::observe::{SharedMetrics, noop_metrics};
use crate::sasl::SaslProfile;
use crate::transport::{Address, TlsConfig};

/// Fluent builder for opening a [`Connection`].
///
/// ```no_run
/// # async fn ex() -> Result<(), ramqp::error::ConnectError> {
/// use ramqp::api::client::ConnectionBuilder;
/// let conn = ConnectionBuilder::new("amqp://guest:guest@localhost:5672")
///     .high_throughput()
///     .connect()
///     .await?;
/// # let _ = conn; Ok(()) }
/// ```
pub struct ConnectionBuilder {
    url: String,
    config: Config,
    metrics: SharedMetrics,
    sasl: Option<SaslProfile>,
    tls: TlsConfig,
    reconnecting: bool,
}

impl std::fmt::Debug for ConnectionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionBuilder")
            .field("url", &self.url)
            .field("config", &self.config)
            .field("sasl", &self.sasl)
            .field("tls", &self.tls)
            .finish_non_exhaustive()
    }
}

impl ConnectionBuilder {
    /// Start building a connection to `url`.
    pub fn new(url: impl Into<String>) -> Self {
        ConnectionBuilder {
            url: url.into(),
            config: Config::default(),
            metrics: noop_metrics(),
            sasl: None,
            tls: TlsConfig::default(),
            reconnecting: false,
        }
    }

    /// Use a custom configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Use the low-latency preset.
    pub fn low_latency(mut self) -> Self {
        self.config = Config::low_latency();
        self
    }

    /// Use the high-throughput preset.
    pub fn high_throughput(mut self) -> Self {
        self.config = Config::high_throughput();
        self
    }

    /// Install a metrics collector.
    pub fn metrics(mut self, metrics: SharedMetrics) -> Self {
        self.metrics = metrics;
        self
    }

    /// Override the SASL profile (otherwise derived from the URL credentials).
    pub fn sasl(mut self, profile: SaslProfile) -> Self {
        self.sasl = Some(profile);
        self
    }

    /// Replace the full TLS configuration used for `amqps://` / `wss://`.
    pub fn tls(mut self, tls: TlsConfig) -> Self {
        self.tls = tls;
        self
    }

    /// Trust an additional CA certificate (PEM) for TLS, on top of the webpki
    /// roots. Call multiple times to add several.
    pub fn add_root_ca_pem(mut self, pem: impl Into<Vec<u8>>) -> Self {
        self.tls.root_ca_pem.push(pem.into());
        self
    }

    /// Present a client certificate chain + private key (PEM) for mutual TLS.
    pub fn client_auth_pem(mut self, cert_chain: impl Into<Vec<u8>>, key: impl Into<Vec<u8>>) -> Self {
        self.tls.client_auth_pem = Some((cert_chain.into(), key.into()));
        self
    }

    /// Override the server name used for SNI and certificate verification.
    pub fn tls_server_name(mut self, name: impl Into<String>) -> Self {
        self.tls.server_name = Some(name.into());
        self
    }

    /// Whether the webpki (Mozilla) root set is trusted (default `true`). Turn
    /// off to trust *only* the CAs added via [`add_root_ca_pem`](Self::add_root_ca_pem).
    pub fn webpki_roots(mut self, enabled: bool) -> Self {
        self.tls.webpki_roots = enabled;
        self
    }

    /// **DANGER** — disable TLS certificate verification. Test-only; never use
    /// against a production broker.
    pub fn danger_accept_invalid_certs(mut self, accept: bool) -> Self {
        self.tls.danger_accept_invalid_certs = accept;
        self
    }

    /// Make the connection **transparently reconnect**: if the link drops, the
    /// returned handle (and every [`Session`](crate::Session)/producer/consumer
    /// derived from it) keeps working — the supervisor re-establishes the
    /// connection with backoff, re-begins sessions, re-attaches links, and
    /// replays in-flight sends. Operations issued while disconnected block until
    /// the link is back. Backoff is governed by `config.connection.reconnect`.
    ///
    /// # Examples
    /// ```no_run
    /// # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
    /// use ramqp::{ConnectionBuilder, Message};
    ///
    /// let conn = ConnectionBuilder::new("amqp://localhost:5672")
    ///     .reconnecting(true)
    ///     .connect()
    ///     .await?;
    /// let session = conn.begin_session().await?;
    /// let producer = session.create_producer("durable-queue").await?;
    ///
    /// // If the broker restarts here, `producer` keeps working — the send simply
    /// // waits out the reconnect instead of failing.
    /// producer.send(Message::text("survives drops")).await?;
    /// # Ok(()) }
    /// ```
    pub fn reconnecting(mut self, enabled: bool) -> Self {
        self.reconnecting = enabled;
        self
    }

    /// Open the connection.
    pub async fn connect(mut self) -> Result<Connection, ConnectError> {
        let addr = Address::parse(&self.url)?;
        if self.config.connection.hostname.is_none() {
            self.config.connection.hostname = Some(addr.host.clone());
        }
        let profile = self.sasl.take().unwrap_or_else(|| {
            SaslProfile::from_credentials(addr.username.clone(), addr.password.clone())
        });
        if self.reconnecting {
            crate::resilience::transparent::connect_supervised(
                addr,
                self.config,
                self.metrics,
                profile,
                self.tls,
            )
            .await
        } else {
            Connection::establish(addr, self.config, self.metrics, profile, self.tls).await
        }
    }
}
