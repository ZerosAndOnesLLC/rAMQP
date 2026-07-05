# rAMQP

[![CI](https://github.com/ZerosAndOnesLLC/rAMQP/actions/workflows/ci.yml/badge.svg)](https://github.com/ZerosAndOnesLLC/rAMQP/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/ramqp.svg)](https://crates.io/crates/ramqp)
[![docs.rs](https://img.shields.io/docsrs/ramqp)](https://docs.rs/ramqp)
[![downloads](https://img.shields.io/crates/d/ramqp.svg)](https://crates.io/crates/ramqp)
[![license](https://img.shields.io/crates/l/ramqp.svg)](./LICENSE)

A from-scratch, **clean-room AMQP 1.0 stack** for Rust on `tokio` — a published
client, a shared protocol engine, and a performance-first, highly-available
broker — with no external AMQP dependencies anywhere.

| Crate | What it is | Status |
|---|---|---|
| [`ramqp`](https://crates.io/crates/ramqp) | The async **client** — connects to RabbitMQ 4.x, ActiveMQ Artemis, and other AMQP 1.0 brokers | Published (0.7.2; 0.8.0 pending — see [Upgrading to 0.8](#upgrading-to-08)) |
| `ramqp-core` | The role-agnostic **engine**: clean-room codec + type system, framing, session/link state machines, SASL (both directions) | 0.2.0, publishes together with `ramqp` 0.8.0 |
| `ramqp-broker` | The **broker**: store-and-forward AMQP 1.0 server with transient + Raft-replicated quorum queues | In development, working — see [The broker](#the-broker-ramqp-broker) |

Everything is `#![forbid(unsafe_code)]`, async-first, and MIT.

---

## The client (`ramqp`)

```rust,no_run
use ramqp::{Connection, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let conn = Connection::open("amqp://guest:guest@localhost:5672").await?;
    let session = conn.begin_session().await?;

    let producer = session.create_producer("/queues/demo").await?;
    producer.send(Message::text("hello")).await?;

    let mut consumer = session.create_consumer("/queues/demo").await?;
    let delivery = consumer.recv().await?;
    consumer.accept(&delivery).await?;

    conn.close().await?;
    Ok(())
}
```

- **Single-pass, zero-copy framing** — bodies are exposed as `bytes::Bytes`
  slices; the transfer/body split is computed once from the negotiated
  `max-frame-size`, never by trial re-serialization.
- **Lock-free actor runtime** — one owning driver task per connection holds all
  protocol state; user handles are cheap clones that exchange messages over
  bounded channels. No locks on the per-message path.
- **Lazy receive by default** — `recv()` yields a `Delivery` exposing raw bytes;
  typed decoding (`.message()`, `.decode::<T>()`) is an explicit, cheap opt-in.
- **Flat, classified errors** — one error type per operation (`ConnectError`,
  `SendError`, `RecvError`, `SessionError`, `LinkError`) with `source()` chains,
  `is_retryable()`/`is_fatal()`, and typed access to any peer-sent error.
- **Transparent reconnect** — opt in with `ConnectionBuilder::reconnecting(true)`
  and your `Producer`/`Consumer` handles survive a broker drop: reconnection with
  backoff, re-attach, and in-flight replay happen behind the scenes.
- **Pluggable observability** — a `Metrics` trait and a connection-event stream,
  usable without enabling `tracing`.

| Feature       | Effect                                                       |
|---------------|--------------------------------------------------------------|
| `rustls`      | `amqps://` via rustls + webpki-roots                         |
| `native-tls`  | `amqps://` via native-tls                                    |
| `ws`          | `ws://` (AMQP over WebSocket); `wss://` also needs `rustls`  |
| `scram`       | SASL `SCRAM-SHA-1/256/512`                                   |
| `transaction` | Transaction controller (clean-room, spec part 4)             |

ANONYMOUS / PLAIN / EXTERNAL SASL are always available.

Verified against live RabbitMQ 4.x and ActiveMQ Artemis — the interop suite
runs in CI against both: SASL, open/begin/attach, credit flow, transfer, all
four settlement outcomes, custom-CA TLS, AMQP-over-WebSocket, and transparent
mid-stream reconnect.

### How the client compares

Benchmarked head-to-head against the established `fe2o3-amqp` client over a
live broker, with matched credit windows and warmup (see
[`bench-compare/`](./bench-compare)). On RabbitMQ 4.x, 5000 messages, receive
throughput (median msg/s):

| body | ramqp (per-msg ack) | fe2o3-amqp (per-msg ack) | ramqp (batched ack) |
|------|--------------------:|-------------------------:|--------------------:|
| 64 B |             ~180,000 |                 ~177,000 |        **~230,000** |
| 1 KB |             ~119,000 |                 ~129,000 |        **~150,000** |
| 8 KB |              ~42,000 |                  ~42,000 |         **~53,000** |

At parity on the standard per-message path; **~1.2–1.4× faster** with batched
ranged settlement (`accept_through`). Numbers are directional (single-node
RabbitMQ); reproduce them with the harness in [`bench-compare/`](./bench-compare).

### Upgrading to 0.8

**Your `Cargo.toml` and code need no changes.** `ramqp = "0.8"` behaves exactly
like 0.7: the 0.8.0 release is an internal restructure — the protocol engine
moved into the new `ramqp-core` crate, and `ramqp` re-exports every moved
module, so all `ramqp::...` paths, feature names, and types are unchanged
(compile-time-locked by `tests/public_api.rs`). Cargo pulls in `ramqp-core`
automatically as an ordinary transitive dependency; the `scram`/`transaction`
features transparently enable their `ramqp-core` counterparts.

What's new is *optional*, for your `Cargo.toml` only if you want it:

```toml
ramqp = "0.8"          # the client, exactly as before
ramqp-core = "0.2"     # just the engine (codec/types/state machines), no client
ramqp-broker = "0.1"   # embed the broker (pre-alpha; API unstable, not yet published)
```

A client-only build never compiles broker code, and vice versa — isolation is
by crate boundary, not feature flags.

---

## The broker (`ramqp-broker`)

A performance-first, highly-available AMQP 1.0 broker on the same clean-room
engine. **In development and moving fast** — the design, targets, and phased
plan live in [`broker.md`](broker.md); its §11 checkboxes are the live status.

Working today:

- TCP acceptor, server-side handshake, SASL ANONYMOUS/PLAIN with a pluggable
  authenticator — any AMQP 1.0 client connects out of the box.
- **Transient queues** (`/queues/<name>`, auto-declared): in-memory queue
  actors with competing consumers, credit-based dispatch,
  accept/release/modified/reject settlement, redelivery on consumer failure,
  and bounded depth with overflow rejection.
- **Quorum queues** (`/quorum/<name>`): backed by a per-queue Raft group
  (openraft) — a publish is acknowledged only after the enqueue **commits to
  the replicated log**; snapshots + log compaction keep memory tracking queue
  depth, not history. Single-replica today; multi-node placement and failover
  routing are the current work.
- **Cluster foundation**: a metadata Raft group (replicated queue catalog)
  over a real TCP inter-node transport with static-seed bootstrap. Three-node
  clusters form, replicate, and survive leader failure with re-election —
  with zero committed-message loss (tested).
- The `ramqp-brokerd` daemon.

```sh
cargo run -p ramqp-broker --bin ramqp-brokerd -- --listen 0.0.0.0:5672
# then point any AMQP 1.0 client at it:
RAMQP_URL=amqp://localhost:5672 RAMQP_ADDRESS=/queues/demo \
    cargo run -p ramqp --example produce_consume
```

### First broker numbers

Same machine, same harness, same client stack on both legs; 256 B payloads,
untuned defaults (methodology and honest caveats in
[`bench-compare/README.md`](bench-compare/README.md)):

| 256 B closed-loop | **ramqp-broker** | RabbitMQ 4.3.1 | Artemis |
|---|---|---|---|
| p50 / p99 / p99.9 latency | **89 / 213 / 428 µs** | 251 / 519 / 777 µs | 227 / 576 / 833 µs |
| blast throughput | **326k msg/s** | 48k msg/s | 79k msg/s |
| broker memory | **~40 MiB** (incl. client) | 133 MiB | 715 MiB |

Quorum queues (every message Raft-committed; single replica): ~202k msg/s,
depth-flat to a 50k backlog, p50 ~116 µs.

Performance is the product for this broker — the targets, hot-path rules, and
benchmark-as-merge-gate policy are `broker.md` §3.

---

## Architecture

```
ramqp (client)
  PUBLIC API     Connection · Session · Producer · Consumer        (ramqp/src/api)
  RESILIENCE     supervisor · reconnect · replay · pool            (ramqp/src/resilience)
  DIAL + DRIVER  connect TCP/TLS/WS · client driver task · SASL    (ramqp/src/{transport,connection,sasl})

ramqp-core (shared engine)
  LINK           sender/receiver · settlement · credit · delivery  (ramqp-core/src/link)
  SESSION        begin/end · windows · registry · both polarities
                 (client attach + server accept)                   (ramqp-core/src/session)
  CONNECTION     open negotiation · mux · heartbeat                (ramqp-core/src/connection)
  TRANSPORT      single-pass frame codec · header (both orders)    (ramqp-core/src/transport)
  SASL           SCRAM math · server-side machinery                (ramqp-core/src/sasl)
  CONTRACTS      errors · ids · config · metrics/events · proto    (ramqp-core/src/{error,ids,config,observe,proto})
  CODEC + TYPES  clean-room AMQP 1.0 type system + wire codec      (ramqp-core/src/{codec,types})

ramqp-broker (the broker)
  FRONTEND       acceptor · server handshake · connection driver   (ramqp-broker/src/{broker,connection,auth})
  QUEUES         transient actors · quorum (Raft-backed) actors    (ramqp-broker/src/{queue,quorum,registry})
  CLUSTER        metadata group · per-queue groups · TCP Raft
                 transport · static-seed bootstrap                 (ramqp-broker/src/cluster)
  DAEMON         ramqp-brokerd                                     (ramqp-broker/src/bin)
```

## Tests & benchmarks

```sh
cargo test                    # whole workspace: unit + in-process integration
cargo test --all-features     # incl. TLS / WS / SCRAM / transactions (180 tests)
cargo bench --bench codec     # codec/framing micro-benchmarks

# Live-broker interop — #[ignore]d so a plain `cargo test` stays green;
# CI runs this for real against RabbitMQ 4.x and Artemis:
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/ramqp_it \
    cargo test --test broker -- --ignored --test-threads=1

# Broker latency/throughput/RSS harness (ours in-process, or any AMQP URL):
cargo run -p ramqp-bench-compare --release --bin latency
```

## Releasing

Publish order matters now that the repo is a workspace — see
[`RELEASING.md`](RELEASING.md). Short version: `ramqp-core` must be on
crates.io **before** `ramqp` 0.8.0 (the client's manifest requires it by
version); `ramqp-broker` stays unpublished until its API stabilizes;
`bench-compare` is never published.

## License

MIT.
