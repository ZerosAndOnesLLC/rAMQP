# Changelog

All notable changes to ramqp will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.5.4] - 2026-06-03

### Changed
- Repository hygiene: community health files (`CONTRIBUTING`, `SECURITY`,
  `CODE_OF_CONDUCT`, this changelog, issue/PR templates), GitHub Actions CI
  (check / test / fmt / clippy `-D warnings` / docs / MSRV) and a tag-driven
  crates.io release workflow.
- The tree is now `cargo clippy -D warnings` and `cargo fmt` clean.

No library or public-API changes from 0.5.3.

## [0.5.3] - 2026-06-03

First release published to [crates.io](https://crates.io/crates/ramqp).

### Added
- **Clean-room AMQP 1.0 type system + wire codec** — the entire OASIS AMQP 1.0
  encoding layer implemented from scratch, with no external AMQP dependencies.
  Single-pass, zero-copy framing (`bytes::Bytes`); golden byte-vector tests.
- **Async runtime** on Tokio — a lock-free actor driver per connection; cheap,
  cloneable `Connection` / `Session` / `Producer` / `Consumer` handles.
- **Transports** — TCP, TLS (`amqps`, `rustls` default or `native-tls`), and
  AMQP-over-WebSocket (`ws` / `wss`).
- **TLS configuration** — custom root CAs, mutual TLS (client certificate), SNI
  override, and a test-only verification-bypass, via `TlsConfig` and the
  `ConnectionBuilder` helpers.
- **SASL** — `ANONYMOUS` / `PLAIN` / `EXTERNAL` always available;
  `SCRAM-SHA-1/256/512` under the `scram` feature.
- **Messaging** — credit/window-gated send with multi-frame splitting, delivery
  reassembly, first- and second-stage settlement, and all terminal outcomes
  (accept / reject / release / modify).
- **Transparent mid-stream reconnect** (opt in via
  `ConnectionBuilder::reconnecting(true)`) — handles survive a connection drop:
  sessions and links are re-established with backoff and in-flight sends are
  replayed.
- **Resilience** — jittered reconnect backoff, resilient connect, a health-aware
  connection pool, and a bounded fire-and-forget outbox (`LinkConfig.max_outbox`).
- **Observability** — a `Metrics` trait and a connection-event stream, usable
  without `tracing`.
- **Transactions** — a clean-room transaction coordinator under the
  `transaction` feature.

### Security
- `#![forbid(unsafe_code)]` throughout.
- Decoder allocation hints are clamped to the remaining input; reassembled
  message size is bounded; internal channels and the send outbox are bounded.

### Verified
- Live interop against RabbitMQ 4.x and ActiveMQ Artemis (produce/consume,
  outcomes, 100-message bulk), live `amqps` (custom-CA TLS) and
  AMQP-over-WebSocket, and a 45-second soak (~170k messages, flat memory).
- `cargo audit` clean.
