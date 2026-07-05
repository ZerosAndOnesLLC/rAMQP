//! Connection runtime: the client's single-owner driver task, built on the
//! role-neutral negotiation/mux/heartbeat helpers from `ramqp-core`.

// Role-neutral pieces live in ramqp-core; re-export them so existing
// `crate::connection::...` / `ramqp::connection::...` paths keep resolving.
pub use ramqp_core::connection::{heartbeat, mux, negotiate};

pub mod driver;
