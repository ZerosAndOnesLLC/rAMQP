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
    /// Link credit granted to an inbound producer link at attach (topped up
    /// in batches as deliveries are ingested).
    pub initial_credit: u32,
    /// Per-delivery size cap for inbound producer links (`None` = only the
    /// built-in hard ceiling).
    pub max_message_size: Option<u64>,
    /// Maximum messages a queue holds (ready + unacked) before refusing
    /// publishes (`rejected`, `resource-limit-exceeded`). Bounded always —
    /// an unbounded queue is an OOM (broker.md §3.2).
    pub max_queue_depth: usize,
    /// Maximum number of concurrently established connections. Once reached,
    /// further accepted sockets are closed immediately (rather than spawning
    /// unbounded per-connection state) — a network-facing DoS guard. Each
    /// connection also holds per-connection buffers and a task, so this is the
    /// primary memory/fd bound. `0` disables the cap (not recommended on a
    /// public bind).
    pub max_connections: usize,
    /// Maximum number of distinct queues that may be auto-declared. Since any
    /// attach declares a queue on first use, this bounds how many actors (and,
    /// for `/quorum/*`, Raft groups) a client can spawn — an unbounded-
    /// allocation DoS guard. At the cap, an attach to a *new* address is
    /// refused. `0` disables the cap.
    pub max_queues: usize,
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            connection: ConnectionConfig::default(),
            session: SessionConfig::default(),
            initial_credit: 512,
            max_message_size: Some(16 * 1024 * 1024),
            max_queue_depth: 1_000_000,
            max_connections: 16_384,
            max_queues: 100_000,
        }
    }
}
