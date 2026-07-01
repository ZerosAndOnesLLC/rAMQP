//! `ramqp-core` — the role-agnostic AMQP 1.0 engine shared by the [`ramqp`]
//! client and the `ramqp-broker` server.
//!
//! This crate holds the clean-room protocol layer with no directional (client
//! vs server) bias: the type/encoding layer ([`codec`], [`types`]), identifier
//! newtypes ([`ids`]), and pluggable observability ([`observe`]). The
//! remaining role-neutral engine pieces (framing, session/link state machines,
//! negotiation) move here during the Phase 1 extraction tracked in `broker.md`.
//!
//! Downstream crates re-export these modules, so `use ramqp::codec::...`
//! paths keep working for existing client users.
//!
//! [`ramqp`]: https://crates.io/crates/ramqp
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

// ---- Clean-room encoding & type layer ----
pub mod codec;
pub mod types;

// ---- Contracts ----
pub mod config;
pub mod error;
pub mod ids;
pub mod observe;
