# ramqp-broker

A **performance-first, highly-available AMQP 1.0 broker** in Rust, built on
[`ramqp-core`](https://crates.io/crates/ramqp-core) — the same clean-room
protocol engine as the [`ramqp`](https://crates.io/crates/ramqp) client.

> **Status: in development.** See `broker.md` at the repository root for the
> architecture and phased plan (per-queue Raft groups via openraft, quorum vs
> transient queues, predictable-tail-latency targets, continuous benchmarks
> against tuned Artemis/RabbitMQ).

`#![forbid(unsafe_code)]`. MIT license.
