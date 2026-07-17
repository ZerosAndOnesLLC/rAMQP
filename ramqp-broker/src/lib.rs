//! `ramqp-broker` — a performance-first, highly-available AMQP 1.0 broker.
//!
//! Built on [`ramqp_core`] — the same clean-room protocol engine as the
//! `ramqp` client. See `broker.md` at the repository root for the
//! architecture and phased plan.
//!
//! # Status
//! Working, pre-1.0. Any AMQP 1.0 client connects (TCP acceptor, server-order
//! handshake, SASL ANONYMOUS/PLAIN behind a pluggable [`auth::Authenticator`]).
//! Three queue families, selected by address:
//!
//! - `/queues/<name>` — **transient**: one lock-free actor per queue,
//!   competing consumers, credit-based dispatch, settlement → ack/requeue,
//!   bounded depth.
//! - `/quorum/<name>` — **quorum**: each queue its own Raft group (openraft);
//!   the accepted disposition is the replicated-commit durability confirm.
//! - `/durable/<name>` — **durable** (feature `store-redb` + a `data_dir`):
//!   on-disk via redb, group-commit fsync, full restart recovery.
//!
//! Clustering: a metadata Raft group plus a multiplexed inter-node fabric;
//! any node serves any queue via leader-following proxying, with failover.
//! Per-queue policies (TTL, length bounds, overflow, dead-lettering,
//! delivery-attempt caps), transactions, and a Prometheus/JSON management
//! endpoint are in. Interop is exercised against the `ramqp` and
//! `fe2o3-amqp` clients, JMS (Qpid JMS), and Qpid Proton.
//!
//! The API (notably [`config::BrokerConfig`]) is still settling — hence
//! `#[non_exhaustive]` config types and the 0.x version. Architecture and
//! phased plan: `broker.md` in the repository.
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
// Cluster internals (openraft glue, the fabric transport, the cluster node)
// are not part of the crate's public API — they are pre-alpha and
// openraft-typed. Clustering is configured through the plain
// [`config::ClusterMemberConfig`].
pub(crate) mod cluster;
pub mod config;
mod connection;
mod dispatch;
#[cfg(feature = "store-redb")]
mod durable;
mod mgmt;
mod policy;
mod proxy;
mod queue;
mod quorum;
mod registry;
mod serde_bin;
#[cfg(feature = "store-redb")]
mod store;
mod txn;

pub use auth::{AllowAll, Authenticator, Credentials, Operation, StaticPlain, StaticScram};
pub use broker::{BoundBroker, Broker, ShutdownHandle};
pub use config::{BrokerConfig, ClusterMemberConfig, OverflowBehavior, QueuePolicy};
