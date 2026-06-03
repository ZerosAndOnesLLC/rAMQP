//! Connection pool with health-aware checkout (WP-6.3).
//!
//! A fixed-size pool of [`Connection`]s. Checkout is round-robin; a dead
//! connection (its driver exited) is transparently re-established with retry,
//! bounded by an acquisition timeout.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use crate::api::connection::Connection;
use crate::config::Config;
use crate::error::{ConnectError, ErrorKind};
use crate::observe::{SharedMetrics, noop_metrics};
use crate::resilience::supervisor::connect_with_retry;

/// A pool of supervised AMQP connections.
#[derive(Clone)]
pub struct Pool {
    inner: Arc<PoolInner>,
}

struct PoolInner {
    url: String,
    config: Config,
    metrics: SharedMetrics,
    slots: Mutex<Vec<Option<Arc<Connection>>>>,
    next: AtomicUsize,
    acquire_timeout: Duration,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool")
            .field("url", &self.inner.url)
            .field("size", &self.inner.slots.lock().map(|s| s.len()).unwrap_or(0))
            .finish_non_exhaustive()
    }
}

/// Builder for a [`Pool`].
pub struct PoolBuilder {
    url: String,
    size: usize,
    config: Config,
    acquire_timeout: Duration,
    metrics: Option<SharedMetrics>,
}

impl std::fmt::Debug for PoolBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PoolBuilder")
            .field("url", &self.url)
            .field("size", &self.size)
            .field("acquire_timeout", &self.acquire_timeout)
            .finish_non_exhaustive()
    }
}

impl PoolBuilder {
    /// Start configuring a pool for `url`.
    pub fn new(url: impl Into<String>) -> Self {
        PoolBuilder {
            url: url.into(),
            size: 4,
            config: Config::default(),
            acquire_timeout: Duration::from_secs(30),
            metrics: None,
        }
    }

    /// Number of pooled connections.
    pub fn size(mut self, size: usize) -> Self {
        self.size = size.max(1);
        self
    }

    /// Per-connection configuration.
    pub fn config(mut self, config: Config) -> Self {
        self.config = config;
        self
    }

    /// Maximum time to wait when (re-)establishing a connection on acquire.
    pub fn acquire_timeout(mut self, timeout: Duration) -> Self {
        self.acquire_timeout = timeout;
        self
    }

    /// Install a shared metrics collector.
    pub fn metrics(mut self, metrics: SharedMetrics) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Build the pool (connections are established lazily on first acquire).
    pub fn build(self) -> Pool {
        Pool {
            inner: Arc::new(PoolInner {
                url: self.url,
                config: self.config,
                metrics: self.metrics.unwrap_or_else(noop_metrics),
                slots: Mutex::new((0..self.size).map(|_| None).collect()),
                next: AtomicUsize::new(0),
                acquire_timeout: self.acquire_timeout,
            }),
        }
    }
}

impl Pool {
    /// Start building a pool.
    pub fn builder(url: impl Into<String>) -> PoolBuilder {
        PoolBuilder::new(url)
    }

    /// The number of slots in the pool.
    pub fn size(&self) -> usize {
        self.inner.slots.lock().map(|s| s.len()).unwrap_or(0)
    }

    /// Acquire a healthy connection, re-establishing a dead slot if needed.
    pub async fn acquire(&self) -> Result<Arc<Connection>, ConnectError> {
        let size = self.size();
        let start = self.inner.next.fetch_add(1, Ordering::Relaxed);

        // First pass: hand out an existing healthy connection.
        for offset in 0..size {
            let idx = (start + offset) % size;
            if let Some(conn) = self.healthy_at(idx) {
                return Ok(conn);
            }
        }

        // None healthy: (re)establish the slot the caller landed on.
        let idx = start % size;
        let conn = tokio::time::timeout(
            self.inner.acquire_timeout,
            connect_with_retry(
                &self.inner.url,
                self.inner.config.clone(),
                self.inner.metrics.clone(),
            ),
        )
        .await
        .map_err(|_| ConnectError::msg(ErrorKind::Timeout, "pool acquisition timed out"))??;

        let conn = Arc::new(conn);
        if let Ok(mut slots) = self.inner.slots.lock() {
            slots[idx] = Some(conn.clone());
        }
        Ok(conn)
    }

    fn healthy_at(&self, idx: usize) -> Option<Arc<Connection>> {
        let mut slots = self.inner.slots.lock().ok()?;
        match &slots[idx] {
            Some(conn) if conn.is_alive() => Some(conn.clone()),
            Some(_) => {
                slots[idx] = None; // evict dead
                None
            }
            None => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let pool = Pool::builder("amqp://localhost:5672").size(3).build();
        assert_eq!(pool.size(), 3);
    }

    #[tokio::test]
    async fn acquire_times_out_on_unreachable() {
        // An unroutable address should fail fast within the acquire timeout.
        let pool = Pool::builder("amqp://127.0.0.1:1")
            .size(1)
            .acquire_timeout(Duration::from_millis(200))
            .config({
                let mut c = Config::default();
                c.connection.reconnect.max_retries = Some(1);
                c.connection.reconnect.initial_backoff = Duration::from_millis(10);
                c
            })
            .build();
        let err = pool.acquire().await.unwrap_err();
        assert!(matches!(
            err.kind(),
            ErrorKind::Timeout | ErrorKind::Io | ErrorKind::NotConnected
        ));
    }
}
