//! `ramqp-core` — the role-agnostic AMQP 1.0 engine shared by the [`ramqp`]
//! client and the `ramqp-broker` server.
//!
//! Empty scaffold. Phase 1 of `broker.md` extracts the codec, wire types,
//! framing, and session/link state machines into this crate; the client then
//! re-exports them so `use ramqp::...` paths stay stable.
//!
//! [`ramqp`]: https://crates.io/crates/ramqp
#![forbid(unsafe_code)]
