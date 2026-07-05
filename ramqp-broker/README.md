# ramqp-broker

A **performance-first, highly-available AMQP 1.0 broker** in Rust, built on
[`ramqp-core`](../ramqp-core) — the same clean-room protocol engine as the
[`ramqp`](https://crates.io/crates/ramqp) client.

> **Status: in development, working.** The design, performance targets, and
> phased plan live in [`broker.md`](../broker.md) at the repository root; its
> §11 checkboxes are the live status. Not yet published to crates.io (the API
> is pre-alpha).

## Working today

- **Any AMQP 1.0 client connects** — TCP acceptor, read-first server
  handshake, SASL ANONYMOUS/PLAIN behind a pluggable `Authenticator`
  (`AllowAll`, `StaticPlain` with constant-time comparison).
- **Transient queues** — `/queues/<name>` (or bare names), auto-declared:
  one lock-free actor task per queue, competing consumers with round-robin
  dispatch, credit-based flow control, accept/release/modified/reject
  settlement, redelivery on consumer failure, bounded depth (overflow →
  `rejected`, `resource-limit-exceeded`).
- **Quorum queues** — `/quorum/<name>`: each backed by its own Raft group
  (openraft). A publish is acknowledged only after the enqueue **commits to
  the replicated log** — the accepted disposition is the durability confirm.
  Pipelined commits, ready-set dispatch, snapshots + log compaction (memory
  tracks queue depth, not history). Single-replica today; multi-node
  placement and failover routing are the current work.
- **Cluster foundation** — a metadata Raft group (replicated queue catalog)
  over a real TCP inter-node transport with static-seed bootstrap; 3-node
  clusters form, replicate, and survive leader failure with re-election and
  zero committed-message loss (tested).

## Run it

```sh
cargo run -p ramqp-broker --bin ramqp-brokerd -- --listen 0.0.0.0:5672
```

`--listen` (or `RAMQP_LISTEN`) is the only knob today; `RUST_LOG` controls
tracing. Then point any client at it — e.g. the repo's example:

```sh
RAMQP_URL=amqp://localhost:5672 RAMQP_ADDRESS=/queues/demo \
    cargo run -p ramqp --example produce_consume
```

Or embed it:

```rust,no_run
use ramqp_broker::{Broker, BrokerConfig};

# async fn ex() -> std::io::Result<()> {
let bound = Broker::new(BrokerConfig::default()).bind("0.0.0.0:5672").await?;
bound.run().await
# }
```

## Numbers

Untuned first numbers vs RabbitMQ 4.3.1 and Artemis on the same machine —
2–3× lower latency at every percentile, 4–6× the throughput, at a fraction of
the footprint — with methodology and caveats in
[`bench-compare/README.md`](../bench-compare/README.md). Performance is the
product here: targets and the hot-path rules are `broker.md` §3.

`#![forbid(unsafe_code)]`. MIT license.
