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
  tracks queue depth, not history).
- **Durable queues** — `/durable/<name>` (build with `--features store-redb`,
  run with `--data-dir <path>`): on-disk via redb, group-commit fsync (the
  accepted disposition is the durability confirm), full restart recovery.
- **Clustering with leader routing** — a metadata Raft group (replicated
  queue catalog + rendezvous placement) and an **inter-node fabric**: one
  multiplexed TCP connection per peer pair carrying every group's Raft
  traffic *and* the data plane (publish/subscribe forwarding, zero-copy
  bodies, batched writes). **Any node serves any queue** — attach anywhere;
  a leader-following proxy routes to wherever the queue group's leader
  lives, and re-routes on failover. Kill the leader node mid-stream and a
  consumer on a survivor keeps receiving with zero accepted-message loss
  (tested end-to-end with the unmodified `ramqp` client).

## Run it

```sh
cargo run -p ramqp-broker --bin ramqp-brokerd -- --listen 0.0.0.0:5672
```

The daemon uses **jemalloc** as its global allocator by default: glibc's malloc
fragments its per-thread arenas under high connection open/close churn, growing
RSS and collapsing throughput over a long run (issue #23); jemalloc keeps both
flat. Build with `--no-default-features` to opt out (e.g. on a target jemalloc
doesn't support).

`RUST_LOG` controls tracing. For a cluster, give each node an id, a fabric
listen address, and the shared seed list:

```sh
ramqp-brokerd --listen 0.0.0.0:5672 \
  --node-id 1 --cluster-listen 0.0.0.0:7472 \
  --seed 1=host-a:7472 --seed 2=host-b:7472 --seed 3=host-c:7472
```

(env: `RAMQP_LISTEN`, `RAMQP_NODE_ID`, `RAMQP_CLUSTER_LISTEN`,
`RAMQP_SEEDS=1=host-a:7472,2=host-b:7472,...`.) Then point any client at any
node — e.g. the repo's example:

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

## Testing

Beyond the in-tree suite (`cargo test --all-features`), a re-runnable battery
lives in [`scripts/`](scripts) — the same checks build over build, so
regressions in correctness, memory, HA, interop, or robustness surface before a
release. It is **not** wired into CI; run it by hand:

```sh
ramqp-broker/scripts/run-all.sh --quick   # gates + full suite (fast)
ramqp-broker/scripts/run-all.sh           # + soak, chaos, interop, robustness, fuzz
```

Stages: static gates, the suite under a flake-repeat loop, a soak/leak detector
(RSS-flat + no-throughput-decay under churn), chaos (rolling `kill -9` of cluster
nodes with a zero-accepted-loss verifier, plus durable crash recovery), an
interop matrix (`ramqp` and `fe2o3-amqp` clients × ramqp-broker / RabbitMQ /
Artemis), robustness floods, and `cargo-fuzz` on the wire decoders. Performance
(`scripts/bench.sh`) and coverage (`scripts/cov.sh`) are manual and never gate.
See [`scripts/README.md`](scripts/README.md).

## Numbers

Untuned first numbers vs RabbitMQ 4.3.1 and Artemis on the same machine —
2–3× lower latency at every percentile, 4–6× the throughput, at a fraction of
the footprint — with methodology and caveats in
[`bench-compare/README.md`](../bench-compare/README.md). Performance is the
product here: targets and the hot-path rules are `broker.md` §3.

`#![forbid(unsafe_code)]`. MIT license.
