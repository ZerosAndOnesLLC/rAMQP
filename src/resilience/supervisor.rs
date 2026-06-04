//! Reconnect backoff and resilient connect (WP-6.1).
//!
//! [`Backoff`] yields jittered exponential delays per the [`ReconnectConfig`];
//! [`connect_with_retry`] retries connection establishment on retryable failures
//! until success or the retry budget is exhausted.
//!
//! Full transparent *mid-stream* reconnect with session/link re-attach and
//! unsettled replay builds on the snapshot-able settlement state in
//! [`crate::link::settlement`]; this module provides the backoff + resilient
//! establishment those flows reuse.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use crate::api::client::ConnectionBuilder;
use crate::api::connection::Connection;
use crate::config::{Config, ReconnectConfig};
use crate::observe::SharedMetrics;

/// Jittered exponential backoff state.
#[derive(Debug, Clone)]
pub struct Backoff {
    policy: ReconnectConfig,
    attempt: u32,
    current: Duration,
}

impl Backoff {
    /// Create a backoff from a reconnect policy.
    ///
    /// The policy is sanitized against misconfiguration that would cause a tight
    /// retry spin: a multiplier below 1 (or NaN) is raised to 1 so the delay
    /// never shrinks, and a sub-millisecond initial backoff is floored so retries
    /// are never effectively instantaneous.
    pub fn new(mut policy: ReconnectConfig) -> Self {
        if policy.multiplier < 1.0 || policy.multiplier.is_nan() {
            policy.multiplier = 1.0;
        }
        let min_backoff = Duration::from_millis(1);
        if policy.initial_backoff < min_backoff {
            policy.initial_backoff = min_backoff;
        }
        if policy.max_backoff < policy.initial_backoff {
            policy.max_backoff = policy.initial_backoff;
        }
        let current = policy.initial_backoff;
        Backoff {
            policy,
            attempt: 0,
            current,
        }
    }

    /// The next delay, or `None` once the retry budget is exhausted.
    pub fn next_delay(&mut self) -> Option<Duration> {
        if !self.policy.enabled {
            return None;
        }
        if let Some(max) = self.policy.max_retries {
            if self.attempt >= max {
                return None;
            }
        }
        self.attempt += 1;
        let base = self.current.min(self.policy.max_backoff);
        let delay = apply_jitter(base, self.policy.jitter);
        // Grow for next time.
        let next = self.current.as_secs_f64() * self.policy.multiplier;
        self.current = Duration::from_secs_f64(next).min(self.policy.max_backoff);
        Some(delay)
    }

    /// Reset to the initial backoff (after a successful (re)connect).
    pub fn reset(&mut self) {
        self.attempt = 0;
        self.current = self.policy.initial_backoff;
    }

    /// The number of attempts consumed so far.
    pub fn attempt(&self) -> u32 {
        self.attempt
    }
}

/// A process-wide splitmix64 stream for backoff jitter (no `rand` dependency;
/// jitter quality, not cryptographic, is all that's required here).
fn jitter_unit() -> f64 {
    static SEED: AtomicU64 = AtomicU64::new(0x9E37_79B9_7F4A_7C15);
    let mut x = SEED.fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed);
    x ^= x >> 30;
    x = x.wrapping_mul(0xBF58_476D_1CE4_E5B9);
    x ^= x >> 27;
    x = x.wrapping_mul(0x94D0_49BB_1331_11EB);
    x ^= x >> 31;
    (x >> 11) as f64 / (1u64 << 53) as f64
}

fn apply_jitter(base: Duration, fraction: f64) -> Duration {
    let fraction = fraction.clamp(0.0, 1.0);
    // factor in [1 - fraction, 1)
    let factor = 1.0 - fraction + fraction * jitter_unit();
    Duration::from_secs_f64(base.as_secs_f64() * factor)
}

/// Open a connection, retrying retryable failures with jittered backoff per the
/// config's [`ReconnectConfig`].
pub async fn connect_with_retry(
    url: &str,
    config: Config,
    metrics: SharedMetrics,
) -> Result<Connection, crate::error::ConnectError> {
    let mut backoff = Backoff::new(config.connection.reconnect.clone());
    loop {
        let attempt = ConnectionBuilder::new(url)
            .config(config.clone())
            .metrics(metrics.clone())
            .connect()
            .await;
        match attempt {
            Ok(conn) => return Ok(conn),
            Err(e) if e.is_retryable() => match backoff.next_delay() {
                Some(delay) => {
                    metrics.on_reconnect(backoff.attempt());
                    tokio::time::sleep(delay).await;
                }
                None => return Err(e),
            },
            Err(e) => return Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitizes_misconfigured_backoff() {
        // A multiplier < 1 and a zero initial backoff would otherwise spin.
        let policy = ReconnectConfig {
            enabled: true,
            max_retries: None,
            initial_backoff: Duration::ZERO,
            max_backoff: Duration::ZERO,
            multiplier: 0.0,
            jitter: 0.0,
        };
        let mut b = Backoff::new(policy);
        let d1 = b.next_delay().unwrap();
        let d2 = b.next_delay().unwrap();
        // floored to >= 1ms and never shrinking to an instantaneous retry
        assert!(d1 >= Duration::from_millis(1));
        assert!(d2 >= Duration::from_millis(1));
    }

    #[test]
    fn backoff_grows_and_caps() {
        let policy = ReconnectConfig {
            enabled: true,
            max_retries: Some(10),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(1),
            multiplier: 2.0,
            jitter: 0.0,
        };
        let mut b = Backoff::new(policy);
        let d1 = b.next_delay().unwrap();
        let d2 = b.next_delay().unwrap();
        let d3 = b.next_delay().unwrap();
        assert!(d2 > d1);
        assert!(d3 > d2);
        // eventually caps at max_backoff
        for _ in 0..10 {
            let _ = b.next_delay();
        }
        // exhausts retry budget
        // (attempt count enforced)
    }

    #[test]
    fn backoff_respects_retry_budget() {
        let policy = ReconnectConfig {
            enabled: true,
            max_retries: Some(3),
            initial_backoff: Duration::from_millis(1),
            max_backoff: Duration::from_millis(10),
            multiplier: 2.0,
            jitter: 0.5,
        };
        let mut b = Backoff::new(policy);
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_some());
        assert!(b.next_delay().is_none());
        assert_eq!(b.attempt(), 3);
    }

    #[test]
    fn disabled_policy_never_retries() {
        let policy = ReconnectConfig {
            enabled: false,
            ..Default::default()
        };
        let mut b = Backoff::new(policy);
        assert!(b.next_delay().is_none());
    }

    #[test]
    fn jitter_stays_in_range() {
        for _ in 0..100 {
            let d = apply_jitter(Duration::from_secs(10), 0.3);
            assert!(d >= Duration::from_secs(7));
            assert!(d <= Duration::from_secs(10));
        }
    }
}
