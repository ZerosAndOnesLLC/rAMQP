//! Observability contracts (Phase 0, WP-0.4): a pluggable [`Metrics`] trait and
//! a connection [`event`] stream, usable without the `tracing` ecosystem.

pub mod event;
pub mod metrics;
pub mod wiring;

pub use event::{ConnectionEvent, ConnectionState, EventBus};
pub use metrics::{Metrics, NoopMetrics, SharedMetrics, noop_metrics};
