//! `ramqp` — a from-scratch, **clean-room** AMQP 1.0 client.
//!
//! This crate implements the OASIS AMQP 1.0 specification from scratch with no
//! external AMQP dependencies: the entire type/encoding layer ([`codec`],
//! [`types`]) and the async runtime (transport → connection → session → link →
//! public API → resilience) are built here on top of generic building blocks
//! (`bytes`, `tokio`, `futures`).
//!
//! # Design pillars
//! - **Single-pass, zero-copy framing.** Bodies are exposed as [`bytes::Bytes`]
//!   slices; transfer/body splitting is computed once from the negotiated
//!   `max-frame-size` rather than by trial re-serialization.
//! - **Lock-free actor runtime.** One owning driver task per connection holds
//!   all protocol state; user handles are cheap clones that exchange messages.
//! - **Flat, classified errors.** One error enum per public operation with a
//!   `source()` chain and retry classification.
//! - **Pluggable observability.** A [`Metrics`](observe::Metrics) trait and a
//!   connection-[`event`](observe::event) stream, usable without `tracing`.
//!
//! The crate is `#![forbid(unsafe_code)]`.

#![forbid(unsafe_code)]
#![warn(missing_docs)]
#![warn(missing_debug_implementations)]

// ---- Clean-room encoding & type layer (re-exported from ramqp-core) ----
pub use ramqp_core::codec;
pub use ramqp_core::types;
// The composite-type codegen macro `amqp_composite!` is `#[macro_export]`ed
// from `ramqp-core`; re-export it so existing `crate::amqp_composite` /
// `ramqp::amqp_composite` paths keep resolving.
pub use ramqp_core::amqp_composite;

// ---- Contracts (re-exported from ramqp-core where role-neutral) ----
pub use ramqp_core::ids;
pub use ramqp_core::observe;

pub mod config;
pub mod error;
pub mod proto;

// ---- Runtime (Phases 1–6) ----
pub mod api;
pub mod connection;
pub mod link;
pub mod resilience;
pub mod sasl;
pub mod session;
pub mod transport;

// ---- Transactions (clean-room, feature-gated) ----
#[cfg(feature = "transaction")]
pub mod txn;

// ---- Convenience re-exports ----
pub use api::{Connection, ConnectionBuilder, Consumer, Producer, Session};
pub use config::Config;
pub use link::Delivery;
pub use resilience::{Pool, PoolBuilder};
pub use transport::TlsConfig;
pub use types::messaging::Message;
