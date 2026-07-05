//! Role-neutral SASL building blocks.
//!
//! The SASL *frame* types live in [`crate::types::sasl`]; this module holds the
//! mechanism-level primitives shared by both directions of authentication: the
//! client negotiation state machine lives in `ramqp`, the server flow in
//! `ramqp-broker`, and both build on the SCRAM math here (behind the `scram`
//! feature).

#[cfg(feature = "scram")]
pub mod scram;
pub mod server;
