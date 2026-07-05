# Contributing to ramqp

Thank you for your interest in contributing to ramqp. This document covers the process for contributing to this project.

## Getting Started

1. Fork the repository
2. Clone your fork: `git clone git@github.com:YOUR_USERNAME/rAMQP.git`
3. Create a branch: `git checkout -b feature/your-feature`
4. Make your changes
5. Submit a pull request

## Workspace layout

The repo is a Cargo workspace of four crates — know where your change goes:

| Crate | Contents |
|---|---|
| `ramqp-core/` | The shared protocol engine (codec, types, framing, session/link state machines, SASL). Role-neutral: **both** the client and broker build on it — a change here affects both. |
| `ramqp/` | The published client (public API, resilience, dial-side transport, client driver). Its public surface is locked by `ramqp/tests/public_api.rs` — if that test breaks, you changed the API. |
| `ramqp-broker/` | The broker (acceptor, connection driver, queues, cluster). Plan + status: [`broker.md`](broker.md). |
| `bench-compare/` | Dev-only benchmark harness (never published; the only place `fe2o3-amqp` may appear). |

All the usual commands run from the repo root and cover the workspace's
default members. `bench-compare` is off the default set — build it explicitly
with `-p ramqp-bench-compare`.

## Development Setup

```bash
# Install Rust (stable; ramqp's MSRV is 1.85, edition 2024)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh

# Build and test
cargo build
cargo test                       # unit + mock-peer integration
cargo test --all-features        # incl. TLS / WebSocket / SCRAM / transactions
cargo doc --all-features --no-deps

# Codec micro-benchmarks
cargo bench --bench codec
```

### Testing against a real broker

The broker interop tests (`ramqp/tests/broker.rs`, `tls.rs`, `ws.rs`) are
**env-gated** — they no-op unless their broker URL is set, so a plain `cargo test`
stays green without Docker. See [`ramqp/tests/docker/README.md`](ramqp/tests/docker/README.md)
for the RabbitMQ / Artemis / TLS / WebSocket harness. CI also runs the suite
against live RabbitMQ 4.x and Artemis containers. Example:

```bash
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/my-queue \
    cargo test --test broker -- --test-threads=1
```

## Code Guidelines

### Rust

- Use `cargo fmt` before committing
- Run `cargo clippy --all-targets -- -D warnings` and fix all warnings
- Run `cargo test` (and `cargo test --all-features`) and ensure all tests pass
- Run `cargo check` with zero errors and no new warnings
- Use Rust 2024 edition features where appropriate
- Every crate is `#![forbid(unsafe_code)]` — keep it that way
- Public items must be documented (`#![warn(missing_docs)]` is on)

### Clean-room constraint

ramqp implements the OASIS AMQP 1.0 specification **from scratch** — its own
type system and wire codec, with **no external AMQP crate dependencies**. Do not
add a dependency on another AMQP library (e.g. `fe2o3-amqp`, `lapin`,
`serde_amqp`), and do not copy code from one. Format/descriptor codes come from
the public spec, cited in comments.

### Performance

This is a client on the hot path of message-oriented systems. Allocations matter.

- Prefer zero-copy: bodies flow as `bytes::Bytes` slices, never re-serialized to probe size
- Keep the per-message path lock-free (the driver owns protocol state; handles are channels)
- Bound buffers/queues — never let a slow peer grow memory without limit
- Profile before/after for hot-path changes; run `cargo bench --bench codec`

### Correctness

- Cite the relevant OASIS AMQP 1.0 spec section in comments for protocol logic
- Add unit tests for any new encode/decode code; verify bytes against the spec — never guess
- Add a golden byte-vector test for new wire types where practical
- Validate interop against a real broker (RabbitMQ 4.x, ActiveMQ Artemis) for protocol changes

## Pull Request Process

1. Update `CHANGELOG.md` with your changes
2. Bump the changed crate's version in its `Cargo.toml` (patch for fixes,
   minor for features, major for breaking). If you bump `ramqp-core`, sync the
   `version = "…"` pins in `ramqp/Cargo.toml` and `ramqp-broker/Cargo.toml` —
   see [`RELEASING.md`](RELEASING.md) for why publishing breaks otherwise
3. Ensure `cargo test` (and `cargo test --all-features`) passes
4. Ensure `cargo check` and `cargo clippy -- -D warnings` produce no errors
5. Ensure `cargo fmt --all -- --check` is clean
6. Describe what your PR does and why in the PR description
7. Link any related issues

## What We're Looking For

- Bug fixes with test cases
- Spec-conformance improvements (cite the section)
- Broker interop coverage (new brokers, edge cases)
- Performance improvements with benchmark results
- Documentation improvements
- Test coverage improvements

## What We're Not Looking For

- A dependency on (or copied code from) another AMQP crate — this breaks the clean-room design
- Paid/proprietary crate dependencies
- `unsafe` code
- Features that add complexity without clear benefit
- Breaking public-API changes without a clear migration path

## Reporting Issues

- Use GitHub Issues
- Include your OS, Rust version, and ramqp version
- Include the broker and version (e.g. RabbitMQ 4.x, Artemis), and which Cargo features you enabled
- Include a minimal reproducing snippet and, where useful, `RUST_LOG=ramqp=debug` logs

## Code of Conduct

This project follows its [Code of Conduct](CODE_OF_CONDUCT.md). Be respectful, be
constructive, focus on the code.

## License

By contributing, you agree that your contributions will be licensed under the MIT License.
