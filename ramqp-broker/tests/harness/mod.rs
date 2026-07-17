//! Shared broker test harness.
//!
//! One place for the loopback-broker starter and the raw-frame peer used to
//! drive hand-built (including deliberately illegal) AMQP against the broker.
//! Consolidates the per-file `start()` duplication and the raw helpers that
//! used to live inline in `adversarial.rs`, so the conformance matrix and the
//! functional suites share one vocabulary.
//!
//! Each integration-test binary that needs it does `mod harness;` — Cargo
//! compiles this module into that binary. Not every binary uses every helper,
//! hence the module-level `dead_code`/`unused_imports` allowances (a binary that
//! uses only a subset of the harness must not warn).
#![allow(dead_code, unused_imports)]

mod client;
mod loopback;
mod raw_peer;

pub use client::{connect, text_of};
pub use loopback::{Loopback, loopback, loopback_with};
pub use raw_peer::{CloseOutcome, RawPeer, encode_frame, read_raw_frame};
