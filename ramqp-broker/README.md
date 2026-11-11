# ramqp-broker

A **performance-first, highly-available AMQP 1.0 broker** in Rust, built on
[`ramqp-core`](https://crates.io/crates/ramqp-core) — the same clean-room
protocol engine as the [`ramqp`](https://crates.io/crates/ramqp) client.

> **Status: working, pre-1.0.** The wire protocol, queue semantics, and HA
> behavior are exercised by a conformance suite, cross-client interop
> (`ramqp`, `fe2o3-amqp`, Qpid JMS, Qpid Proton), and partition/chaos tests.
> The **Rust API is still settling** (config types are `#[non_exhaustive]`);
> expect additive change across 0.x. The design, performance targets, and
> phased plan live in
> [`broker.md`](https://github.com/ZerosAndOnesLLC/rAMQP/blob/main/broker.md)
> in the repository.

## Working today

- **Any AMQP 1.0 client connects** — TCP acceptor, read-first server
  handshake, SASL ANONYMOUS/PLAIN/SCRAM-SHA-1/256/512 behind a pluggable
  `Authenticator` (`StaticUsers` — the config-file backend: SCRAM verifiers
  only, never plaintext, with per-user × vhost × address × operation
  permissions; plus `AllowAll`, `StaticPlain`, `StaticScram`).
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
cargo install ramqp-broker            # installs the ramqp-brokerd daemon
ramqp-brokerd --config /etc/ramqp/broker.toml
ramqp-brokerd --listen 127.0.0.1:5672   # quick local run, no auth
```

On a **non-loopback** listener the daemon refuses to start without users
unless you opt into an open broker with `--allow-anonymous`
(`RAMQP_ALLOW_ANONYMOUS=1`).

(or from a repo checkout:
`cargo run -p ramqp-broker --bin ramqp-brokerd -- --listen 0.0.0.0:5672`)

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

### Configuration file

`--config <path>` (or `RAMQP_CONFIG`) loads a TOML file; environment
variables and flags override it. It replaces RabbitMQ's `definitions.json`:

```toml
listen = "0.0.0.0:5672"
data_dir = "/var/lib/ramqp"           # needs --features store-redb
management_listen = "0.0.0.0:15692"
allow_plain = true                    # PLAIN is checked against the SCRAM verifier

[limits]
max_connections = 16384

[cluster]
node_id = 1
listen = "0.0.0.0:7472"
seeds = ["1=broker-0.broker:7472", "2=broker-1.broker:7472", "3=broker-2.broker:7472"]

[[users]]
name = "orders"
verifier_file = "/etc/ramqp/users/orders.scram"   # RFC 5803 line(s)

[[users]]
name = "ops"
password_file = "/etc/ramqp/users/ops"   # derived into a verifier at load
admin = true                              # management API (see below)

[[permissions]]                 # deny by default
user = "orders"                 # or "anonymous", or "*"
vhost = ""                      # default vhost; "*" = all
address = "/queues/orders.*"    # glob over the attach address
operations = ["send", "receive"]

[[policies]]                    # first match wins
prefix = "orders."
message_ttl_ms = 600000
max_length = 100000
overflow = "reject-publish"     # or "drop-head"
dead_letter = "/quorum/dead"
max_delivery_attempts = 5

[[queues]]                      # declared at startup, idempotent
address = "/quorum/orders.created"
```

Secrets stay out of the file: point `verifier_file` / `password_file` at a
mounted Kubernetes Secret. Generate a verifier with
`echo -n "$PASSWORD" | ramqp-brokerd hash-password [--mechanism SCRAM-SHA-512]`.
The daemon also shuts down gracefully on `SIGTERM`.

Or embed it (`ramqp-broker = "0.9"` in `Cargo.toml`; add the `store-redb`
feature for `/durable/*` queues):

```rust,no_run
use ramqp_broker::{Broker, BrokerConfig};

# async fn ex() -> std::io::Result<()> {
let bound = Broker::new(BrokerConfig::default()).bind("0.0.0.0:5672").await?;
bound.run().await
# }
```

## Testing

Beyond the in-tree suite (`cargo test --all-features`), a re-runnable battery
lives in
[`scripts/`](https://github.com/ZerosAndOnesLLC/rAMQP/tree/main/ramqp-broker/scripts)
— the same checks build over build, so
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
See
[`scripts/README.md`](https://github.com/ZerosAndOnesLLC/rAMQP/blob/main/ramqp-broker/scripts/README.md).

## Numbers

Untuned first numbers vs RabbitMQ 4.3.1 and Artemis on the same machine —
2–3× lower latency at every percentile, 4–6× the throughput, at a fraction of
the footprint — with methodology and caveats in
[`bench-compare/README.md`](https://github.com/ZerosAndOnesLLC/rAMQP/blob/main/bench-compare/README.md).
Performance is the product here: targets and the hot-path rules are
`broker.md` §3.

`#![forbid(unsafe_code)]`. MIT license.
