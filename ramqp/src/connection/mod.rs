//! Connection runtime (Phase 2): the single-owner driver task and its
//! negotiation, channel multiplexing, and heartbeat helpers.

pub mod driver;
pub mod heartbeat;
pub mod mux;
pub mod negotiate;
