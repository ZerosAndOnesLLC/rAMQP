//! Configuration structs and presets (WP-0.3).
//!
//! [`Config`] bundles connection/session/link defaults; the
//! [`Config::low_latency`] and [`Config::high_throughput`] presets pick values
//! tuned for the two dominant workloads. Every duration/window is explicit and
//! justified inline.

use std::time::Duration;

use crate::ids::ContainerId;
use crate::types::definitions::{ReceiverSettleMode, SenderSettleMode};

/// Connection-level configuration.
#[derive(Debug, Clone)]
pub struct ConnectionConfig {
    /// Our container id (sent in `open.container-id`).
    pub container_id: ContainerId,
    /// Optional virtual host (`open.hostname`).
    pub hostname: Option<String>,
    /// Largest frame we will accept; the effective size is `min` of both peers.
    pub max_frame_size: u32,
    /// Highest channel number we will use; effective is `min` of both peers.
    pub channel_max: u16,
    /// Idle timeout advertised to the peer; `None` disables heartbeats.
    pub idle_timeout: Option<Duration>,
    /// Maximum time for the connection handshake (transport connect + SASL + the
    /// `open` exchange). `None` disables it. Guards against a peer that accepts
    /// the socket then stalls mid-handshake (slow-loris).
    pub connect_timeout: Option<Duration>,
    /// Bound on the driver command queue (back-pressures producers).
    pub command_buffer: usize,
    /// Reconnect / backoff policy.
    pub reconnect: ReconnectConfig,
}

impl Default for ConnectionConfig {
    fn default() -> Self {
        ConnectionConfig {
            container_id: ContainerId::generate(),
            hostname: None,
            // 128 KiB: comfortably above broker minimums, below memory pressure.
            max_frame_size: 128 * 1024,
            channel_max: 1024,
            idle_timeout: Some(Duration::from_secs(60)),
            connect_timeout: Some(Duration::from_secs(30)),
            command_buffer: 1024,
            reconnect: ReconnectConfig::default(),
        }
    }
}

/// Reconnect supervisor policy (jittered exponential backoff).
#[derive(Debug, Clone)]
pub struct ReconnectConfig {
    /// Whether the supervisor reconnects on retryable failures at all.
    pub enabled: bool,
    /// Maximum reconnect attempts before giving up (`None` = unbounded).
    pub max_retries: Option<u32>,
    /// Backoff applied before the first retry.
    pub initial_backoff: Duration,
    /// Ceiling on the exponential backoff.
    pub max_backoff: Duration,
    /// Per-attempt backoff multiplier.
    pub multiplier: f64,
    /// Fractional jitter applied to each backoff (`0.0..=1.0`).
    pub jitter: f64,
}

impl Default for ReconnectConfig {
    fn default() -> Self {
        ReconnectConfig {
            enabled: true,
            max_retries: None,
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
            multiplier: 2.0,
            jitter: 0.3,
        }
    }
}

/// Session-level flow-control configuration.
#[derive(Debug, Clone, Copy)]
pub struct SessionConfig {
    /// Frames we will buffer from the peer before back-pressuring (incoming-window).
    pub incoming_window: u32,
    /// Frames we may have in flight to the peer (outgoing-window).
    pub outgoing_window: u32,
    /// Highest link handle this session will use.
    pub handle_max: u32,
}

impl Default for SessionConfig {
    fn default() -> Self {
        SessionConfig {
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: 1024,
        }
    }
}

/// How a receiver issues link credit.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum CreditMode {
    /// The application issues credit explicitly via the consumer handle.
    Manual,
    /// The runtime keeps `initial` credit topped up, refilling whenever it falls
    /// to `refill_threshold`.
    Auto {
        /// Credit granted on attach and the high-water target.
        initial: u32,
        /// Refill when available credit drops to this low-water mark.
        refill_threshold: u32,
    },
}

/// Link-level configuration (applies to producers and consumers).
#[derive(Debug, Clone, Copy)]
pub struct LinkConfig {
    /// Receiver credit strategy.
    pub credit_mode: CreditMode,
    /// Sender settlement mode requested at attach.
    pub sender_settle_mode: SenderSettleMode,
    /// Receiver settlement mode requested at attach.
    pub receiver_settle_mode: ReceiverSettleMode,
    /// Maximum message size we accept on the link (`None` = unlimited).
    pub max_message_size: Option<u64>,
    /// Maximum number of unwritten fire-and-forget (`Producer::send_settled`)
    /// messages buffered while awaiting broker credit, before the call
    /// back-pressures. `0` disables the bound (unbounded buffering — the
    /// pre-0.3 behavior).
    pub max_outbox: usize,
}

impl Default for LinkConfig {
    fn default() -> Self {
        LinkConfig {
            credit_mode: CreditMode::Auto {
                initial: 1000,
                refill_threshold: 500,
            },
            sender_settle_mode: SenderSettleMode::Mixed,
            receiver_settle_mode: ReceiverSettleMode::First,
            max_message_size: None,
            max_outbox: 8192,
        }
    }
}

/// The full configuration bundle.
#[derive(Debug, Clone, Default)]
pub struct Config {
    /// Connection-level settings.
    pub connection: ConnectionConfig,
    /// Session-level settings.
    pub session: SessionConfig,
    /// Link-level settings.
    pub link: LinkConfig,
}

impl Config {
    /// A preset tuned for latency: small frames, small windows, and shallow
    /// credit so messages are flushed promptly and little is buffered.
    pub fn low_latency() -> Self {
        Config {
            connection: ConnectionConfig {
                max_frame_size: 64 * 1024,
                command_buffer: 256,
                idle_timeout: Some(Duration::from_secs(30)),
                ..ConnectionConfig::default()
            },
            session: SessionConfig {
                incoming_window: 256,
                outgoing_window: 256,
                handle_max: 256,
            },
            link: LinkConfig {
                credit_mode: CreditMode::Auto {
                    initial: 100,
                    refill_threshold: 50,
                },
                max_outbox: 1024,
                ..LinkConfig::default()
            },
        }
    }

    /// A preset tuned for throughput: large frames, wide windows, and deep
    /// credit so the pipe stays full and batching is effective.
    pub fn high_throughput() -> Self {
        Config {
            connection: ConnectionConfig {
                max_frame_size: 1024 * 1024,
                command_buffer: 8192,
                idle_timeout: Some(Duration::from_secs(120)),
                ..ConnectionConfig::default()
            },
            session: SessionConfig {
                incoming_window: 65_535,
                outgoing_window: 65_535,
                handle_max: 4096,
            },
            link: LinkConfig {
                credit_mode: CreditMode::Auto {
                    initial: 10_000,
                    refill_threshold: 5_000,
                },
                max_outbox: 65_536,
                ..LinkConfig::default()
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn presets_differ_and_are_sane() {
        let ll = Config::low_latency();
        let ht = Config::high_throughput();
        assert!(ll.session.incoming_window < ht.session.incoming_window);
        assert!(ll.connection.max_frame_size < ht.connection.max_frame_size);
        // every preset has a non-empty container id
        assert!(!ll.connection.container_id.as_str().is_empty());
    }
}
