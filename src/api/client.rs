//! The connection builder + presets (WP-5.1).

use crate::api::connection::Connection;
use crate::config::Config;
use crate::error::ConnectError;
use crate::observe::{SharedMetrics, noop_metrics};
use crate::sasl::SaslProfile;
use crate::transport::Address;

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
}

impl std::fmt::Debug for ConnectionBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnectionBuilder")
            .field("url", &self.url)
            .field("config", &self.config)
            .field("sasl", &self.sasl)
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

    /// Open the connection.
    pub async fn connect(mut self) -> Result<Connection, ConnectError> {
        let addr = Address::parse(&self.url)?;
        if self.config.connection.hostname.is_none() {
            self.config.connection.hostname = Some(addr.host.clone());
        }
        let profile = self.sasl.take().unwrap_or_else(|| {
            SaslProfile::from_credentials(addr.username.clone(), addr.password.clone())
        });
        Connection::establish(addr, self.config, self.metrics, profile).await
    }
}
