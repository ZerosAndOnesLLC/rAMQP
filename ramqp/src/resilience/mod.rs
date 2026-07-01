//! Resilience (Phase 6): the reconnect supervisor and the connection pool.

pub mod pool;
pub mod supervisor;
pub mod transparent;

pub use pool::{Pool, PoolBuilder};
pub use supervisor::{Backoff, connect_with_retry};
