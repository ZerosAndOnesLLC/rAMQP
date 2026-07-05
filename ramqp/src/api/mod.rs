//! Public API (Phase 5): the `Client`/`Connection`/`Session` entry points and
//! `Producer`/`Consumer` handles, plus graceful lifecycle.

pub mod client;
pub mod connection;
pub mod consumer;
pub mod lifecycle;
pub mod producer;
pub mod session;

pub use client::ConnectionBuilder;
pub use connection::Connection;
pub use consumer::Consumer;
pub use producer::Producer;
pub use session::Session;
