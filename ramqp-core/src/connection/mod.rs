//! Role-neutral connection building blocks: `open` negotiation, channel
//! multiplexing, and the idle-timeout heartbeat.
//!
//! The connection *driver* (the owning actor task) is role-specific: the
//! client's lives in `ramqp`, the broker's in `ramqp-broker`. Both are built
//! from these parts.

pub mod heartbeat;
pub mod mux;
pub mod negotiate;
