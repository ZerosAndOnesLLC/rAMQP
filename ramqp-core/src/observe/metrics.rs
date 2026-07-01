//! The pluggable [`Metrics`] trait (WP-0.4).
//!
//! All methods have no-op defaults, so a custom collector overrides only the
//! signals it cares about. The runtime holds a [`SharedMetrics`] and calls these
//! at the emission points wired up in [`crate::observe::wiring`].

use std::sync::Arc;
use std::time::Duration;

/// A sink for runtime metrics. Cheap, infallible, called on the hot path — keep
/// implementations lock-light (atomics / sharded counters).
///
/// # Example
/// ```
/// use std::sync::atomic::{AtomicU64, Ordering};
/// use ramqp::observe::Metrics;
///
/// #[derive(Default)]
/// struct Counters {
///     frames_out: AtomicU64,
/// }
///
/// impl Metrics for Counters {
///     fn on_frame_sent(&self, _bytes: usize) {
///         self.frames_out.fetch_add(1, Ordering::Relaxed);
///     }
/// }
///
/// let c = Counters::default();
/// c.on_frame_sent(128);
/// assert_eq!(c.frames_out.load(Ordering::Relaxed), 1);
/// ```
pub trait Metrics: Send + Sync + 'static {
    /// A frame of `bytes` bytes was written to the transport.
    fn on_frame_sent(&self, bytes: usize) {
        let _ = bytes;
    }
    /// A frame of `bytes` bytes was read from the transport.
    fn on_frame_received(&self, bytes: usize) {
        let _ = bytes;
    }
    /// An outbound transfer (message) was sent.
    fn on_transfer_sent(&self) {}
    /// An inbound transfer (message) was received.
    fn on_transfer_received(&self) {}
    /// A delivery reached a settled state.
    fn on_settlement(&self) {}
    /// The credit gauge for a link changed to `credit`.
    fn on_credit(&self, handle: u32, credit: u32) {
        let _ = (handle, credit);
    }
    /// The in-flight (unsettled) delivery count changed by `delta`.
    fn on_inflight(&self, delta: i64) {
        let _ = delta;
    }
    /// A reconnect attempt began (1-based `attempt`).
    fn on_reconnect(&self, attempt: u32) {
        let _ = attempt;
    }
    /// A send-to-settle latency sample.
    fn on_send_to_settle(&self, latency: Duration) {
        let _ = latency;
    }
}

/// The default no-op metrics sink.
#[derive(Debug, Default, Clone, Copy)]
pub struct NoopMetrics;

impl Metrics for NoopMetrics {}

/// A shared, type-erased metrics handle held throughout the runtime.
pub type SharedMetrics = Arc<dyn Metrics>;

/// A [`SharedMetrics`] that discards everything.
pub fn noop_metrics() -> SharedMetrics {
    Arc::new(NoopMetrics)
}
