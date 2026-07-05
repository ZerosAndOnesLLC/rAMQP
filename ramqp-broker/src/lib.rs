//! `ramqp-broker` — a performance-first, highly-available AMQP 1.0 broker.
//!
//! Built on [`ramqp_core`] — the same clean-room protocol engine as the
//! `ramqp` client. See `broker.md` at the repository root for the
//! architecture and phased plan.
//!
//! # Status
//! Phase 4 single-node MVP: TCP acceptor, server-order handshake (protocol
//! header → SASL ANONYMOUS/PLAIN → `open`), a per-connection driver, and
//! in-memory **transient queues** (one lock-free actor per queue) with
//! competing consumers, credit-based dispatch, and settlement→ack/requeue.
//! Durability lands in Phase 7; clustering (per-queue Raft) in Phases 5–6.
//!
//! ```no_run
//! use ramqp_broker::{Broker, BrokerConfig};
//!
//! # async fn ex() -> std::io::Result<()> {
//! let bound = Broker::new(BrokerConfig::default()).bind("0.0.0.0:5672").await?;
//! bound.run().await
//! # }
//! ```
#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

pub mod auth;
mod broker;
pub mod cluster;
pub mod config;
mod connection;
mod queue;
mod registry;

pub use auth::{AllowAll, Authenticator, Credentials, StaticPlain};
pub use broker::{BoundBroker, Broker, ShutdownHandle};
pub use config::BrokerConfig;
