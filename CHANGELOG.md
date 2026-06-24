# Changelog

All notable changes to ramqp will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.7.1] - 2026-06-24

### Changed (performance)
- **Zero-copy receive for single-frame deliveries.** A self-contained transfer
  (the common case) is now turned into a delivery directly from the frame's
  `Bytes` slice, eliminating the per-message body memcpy through the multi-frame
  assembly buffer. Multi-frame deliveries are unchanged.
- **Gathered (vectored) writes for large transfer bodies.** On vectored-capable
  streams (plain TCP) a transfer body ≥ 4 KiB is held out of the write buffer and
  written zero-copy via `writev` interleaved with the frame headers, avoiding the
  body copy on send. TLS and WebSocket streams (non-vectored) are byte-identical
  to before — small bodies are always inlined.

## [0.7.0] - 2026-06-24

### Security
- **Bound array decoding against a malformed-frame DoS.** A `Value` array whose
  elements share a zero-width constructor (e.g. `null`, `boolean-true`) consumed
  no body bytes per element, so a few-byte frame could declare a count up to
  `u32::MAX` and drive unbounded allocation / OOM in the connection driver. The
  element count is now bounded by the available body (and a small ceiling for the
  degenerate zero-width case).
- Maps with an odd element count are now rejected instead of silently dropping a
  dangling key.
- Documented that `danger_accept_invalid_certs` also disables hostname
  verification on both TLS backends.

### Added
- `Consumer::accept_through(&delivery)` — accept every delivery from the oldest
  unsettled one through `delivery` in a single ranged `first..last` disposition,
  for cheap batched acknowledgement.

### Changed (performance — closes the issue #3 receive-throughput gap)
- **Fire-and-forget settlement.** `accept`/`reject`/`release`/`modify`/`settle`
  no longer await a per-message driver round-trip (the reply only ever confirmed
  the frame was queued, never a broker ack), so a `recv → settle` loop pipelines
  instead of stalling on an actor hop per message.
- **Driver write-batching.** The connection driver drains queued commands and
  writes them under a single flush, collapsing a burst of settlements/sends into
  one socket write instead of one per command.
- **Transport read buffer** reserves a read chunk before each socket read,
  avoiding repeated small reallocations under sustained receive.
- Delivery tag is moved into the transfer instead of cloned; links are indexed
  by name for O(1) attach binding; compound/described array encoding reuses one
  scratch buffer; send delivery-state is moved rather than cloned.

## [0.5.4] - 2026-06-03

### Changed
- Repository hygiene: community health files (`CONTRIBUTING`, `SECURITY`,
  `CODE_OF_CONDUCT`, `SUPPORT`, this changelog, issue/PR templates), GitHub
  Actions CI (check / test / fmt / clippy `-D warnings` / docs / MSRV) and a
  tag-driven crates.io release workflow.
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
