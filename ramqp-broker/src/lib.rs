//! `ramqp-broker` — a performance-first, highly-available AMQP 1.0 broker.
//!
//! Empty scaffold. See `broker.md` for the plan. The broker is built on
//! [`ramqp_core`] (Phase 3+); clustering (openraft, per-queue Raft groups)
//! lands in Phase 5+. Performance is the product — see `broker.md` §3.
#![forbid(unsafe_code)]
