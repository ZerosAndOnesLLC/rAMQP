//! Broker configuration.

use ramqp_core::config::{ConnectionConfig, SessionConfig};

/// Configuration for a broker instance.
///
/// Connection-level knobs reuse the role-neutral [`ConnectionConfig`]
/// (container-id, `max-frame-size`, `channel-max`, idle-timeout). Its
/// `connect_timeout` bounds the **inbound handshake** here (header + SASL +
/// `open`), guarding against a client that opens the socket then stalls
/// (slow-loris); its client-only fields (`command_buffer`, `reconnect`) are
/// unused by the broker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Connection-level settings (also the source of our `open`).
    pub connection: ConnectionConfig,
    /// Session window defaults for accepted sessions.
    pub session: SessionConfig,
    /// Link credit granted to an inbound producer link at attach. Phase 3
    /// default is `0` — no queues exist yet, so inviting transfers would only
    /// drop them; Phase 4 grants real credit backed by queues.
    pub initial_credit: u32,
    /// Per-delivery size cap for inbound producer links (`None` = only the
    /// built-in hard ceiling).
    pub max_message_size: Option<u64>,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            connection: ConnectionConfig::default(),
            session: SessionConfig::default(),
            initial_credit: 0,
            max_message_size: Some(16 * 1024 * 1024),
        }
    }
}
