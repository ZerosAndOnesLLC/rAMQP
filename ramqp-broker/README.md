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
- **Addresses** — `/queues/<name>` (RabbitMQ 4.x's address for any queue
  type, or a bare name) reaches the queue declared under `<name>` **whatever
  its kind**, and auto-declares a transient queue only when there is none.
  `/quorum/<name>` and `/durable/<name>` name a kind explicitly (declaring
  it on first use). A name holds one kind: declaring a second kind under it
  is refused (`amqp:not-allowed` at attach, `409` in the admin API). A
  declared quorum or durable queue takes precedence over an auto-declared
  transient one (in a cluster, transient queues are node-local); should a
  quorum and a durable queue share a name from an older version,
  `/queues/<name>` refuses to guess. Authorization matches the address
  as written, so grant the form clients use — and note that a grant on
  `/queues/*` therefore reaches durable and quorum queues too (RabbitMQ 4.x
  semantics). Set `strict_queue_grants = true` to keep `/queues/` grants to
  transient queues: reaching a durable or quorum queue through `/queues/`
  then also needs a grant on its `/durable/` or `/quorum/` address.
- **Transient queues** — auto-declared at `/queues/<name>`:
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
- **Exchanges** — RabbitMQ 4.x addressing, so AMQP 1.0 clients move
  unchanged: publish to `/exchanges/<name>/<routing-key>` (or
  `/exchanges/<name>` with the key in `properties.subject`). **fanout**,
  **direct**, and **topic** (`*` / `#`) kinds; `amq.direct`, `amq.fanout`,
  `amq.topic` exist in every vhost. A publish is confirmed once **every**
  destination queue has confirmed it (rejected if any refuses); an
  unroutable message is settled **`released`** (as RabbitMQ 4.x does, so a
  producer can tell "reached no queue" from "delivered"), or goes to the
  exchange's `alternate` queue (`accepted`). Definitions persist (redb standalone, the metadata Raft group when
  clustered) and binding changes apply to already-attached producers.
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
management_token_file = "/etc/ramqp/mgmt-token"   # bearer token for admin writes
allow_plain = true                    # PLAIN is checked against the SCRAM verifier
strict_queue_grants = false           # true: /queues/* grants cover transient queues only

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
dead_letter = "/quorum/dead"     # or "/quorum/{queue}.dlq": one per queue
max_delivery_attempts = 5

[[queues]]                      # declared at startup, idempotent
address = "/quorum/orders.created"
dead_letter = "/quorum/orders.created.dlq"   # optional: this queue's own policy
max_delivery_attempts = 3                    # (any [[policies]] field)

[[exchanges]]
name = "events"
type = "topic"                  # fanout | direct | topic
alternate = "/queues/unroutable"   # optional: unroutable messages go here

[[bindings]]
exchange = "events"
queue = "/quorum/orders.created"
key = "order.#"
```

### TLS and FIPS

Build with `--features tls` (rustls on ring) or, for FIPS 140-3,
`--no-default-features --features fips,jemalloc,store-redb` (rustls **and**
SCRAM hashing on AWS-LC's FIPS module; needs CMake and Go to build). The
daemon logs `FIPS build: …` at startup and refuses to run if the module is
not active (`ramqp_broker::fips_enabled()`).

```toml
listen = ""                        # optional: TLS-only (no plain listener); or --listen none

[tls]
listen = "0.0.0.0:5671"            # amqps
cert_file = "/etc/ramqp/tls/tls.crt"
key_file = "/etc/ramqp/tls/tls.key"
client_ca_file = "/etc/ramqp/tls/ca.crt"   # optional: require client certificates (amqps only)
management = true                  # management endpoint over TLS (never requires client certs)

[cluster.tls]                      # mutual TLS on the fabric
cert_file = "/etc/ramqp/fabric/tls.crt"
key_file = "/etc/ramqp/fabric/tls.key"
ca_file = "/etc/ramqp/fabric/ca.crt"       # a DEDICATED cluster CA
```

Certificates **and CA bundles** are re-read when they change (cert-manager
rotation): new handshakes use them, and a broken file keeps the previous
material. The fabric needs its **own CA** — the daemon refuses to share it
with `client_ca_file`, since anything that CA signs could reach the fabric —
and a peer's certificate must also name one of the seed hosts (DNS or IP
SAN), so a stray certificate from the same CA is still refused. Flags:
`--tls-listen`, `--tls-cert`, `--tls-key`, `--tls-client-ca`.

### Other clients

The broker speaks plain AMQP 1.0, and its release gate runs independent
client stacks against it with logins, exchanges and TLS: Qpid JMS (Java),
Qpid Proton (Python/C), rhea (Node.js), Azure go-amqp (Go), fe2o3-amqp (Rust)
and RabbitMQ's own AMQP 1.0 client (see `tests/interop/`). Points worth
knowing when moving clients over:

- **Addresses** are RabbitMQ 4.x's: `/queues/<name>` reaches a queue of any
  kind (declare it as quorum or durable in the config file or admin API, and
  clients keep using `/queues/<name>`), `/exchanges/<name>/<key>` publishes
  through an exchange.
- **SASL**: PLAIN works for every user (checked against the stored verifier);
  a SCRAM mechanism is offered only when every user has a verifier for it, so
  clients that pick the strongest offered never lock a user out. Users created
  from a password get SHA-256 and SHA-512 verifiers.
- **TLS**: both ways the spec allows — TLS from the first byte (RabbitMQ-style
  `amqps`) and the negotiated `AMQP 2.1.0.0` header (fe2o3-amqp and other
  spec-default clients), the latter also as an in-place upgrade on the plain
  port when a certificate is configured.
- **RabbitMQ clients** (`rabbitmq-amqp-*-client`): `vhost:/` is the default
  vhost; connecting, publishing (exchange and queue addresses) and consuming
  work. Their topology management (`DeclareQueue`, `Bind`, …) goes over a
  RabbitMQ-specific `/management` protocol and is answered with `501` — declare
  topology in the config file or through the admin API instead.

### Management API

With `management_listen` set, one small HTTP/1.1 endpoint serves metrics,
inspection, and an idempotent admin API (the default vhost is `%2F`):

| Request | Purpose |
|---|---|
| `GET /metrics` | Prometheus text (connections, RSS, per-queue gauges) |
| `GET /queues` | every queue with ready / unacked / consumers |
| `GET /queues/<vhost>/<name>[?kind=]` | one queue: `{"ready", "unacked", "messages", "consumers", …}` — point KEDA's `metrics-api` scaler at `ready`. Correct on **every** node (a non-leader asks the queue's leader); `503` when unknowable, never zeros |
| `PUT /queues/<vhost>/<name>` `{"kind":"quorum"}` | declare (`transient` default); `201` created, `204` exists, `409` exists as another kind or with a different policy. Optional policy fields, as in `[[policies]]`: `dead_letter`, `max_delivery_attempts`, `message_ttl_ms`, `max_length`, `max_length_bytes`, `overflow` — the queue's own policy (`GET` shows it) |
| `DELETE /queues/<vhost>/<name>[?kind=]` | delete a queue of any kind: consumers and producers are detached with `amqp:resource-deleted`, its messages and stored state removed, and its bindings dropped. A clustered quorum queue is deleted through the metadata group — every replica tears down its Raft member and purges its log and files (a replica that is down does so when it rejoins), and a re-declare of the same name is a fresh, empty queue. Clustered deletion needs every node on a version that supports it (`503` otherwise) |
| `GET /exchanges`, `GET /exchanges/<vhost>/<name>` | definitions with bindings |
| `PUT /exchanges/<vhost>/<name>` `{"type":"topic","alternate":"/queues/x"}` | declare; `409` on different arguments |
| `DELETE /exchanges/<vhost>/<name>` | delete with its bindings |
| `PUT` / `DELETE /bindings/<vhost>/<exchange>` `{"queue":"/queues/x","key":"a.*"}` | bind / unbind |
| `GET /users` | users (`source`: `config` or `api`); never public |
| `PUT /users/<name>` `{"password":"…"}` or `{"verifier":"SCRAM-SHA-256$…"}`, optional `"admin":true` | create or replace a runtime user (`201` / `204`). A password becomes SCRAM-SHA-256 and -512 verifiers on arrival and is never stored. Configuration users are read-only (`409`) |
| `DELETE /users/<name>` | delete a runtime user and its permissions; its open connections close (`amqp:unauthorized-access`) |
| `GET` / `PUT` / `DELETE /permissions/<vhost>/<user>` `[{"address":"/queues/jobs","operations":["receive"]}]` | a runtime user's grants in a vhost, the same address-glob × operation form as `[[permissions]]` (configuration users get theirs from the file: `409`) |

Writes always need credentials: `Authorization: Bearer <token>` from
`management_token_file` (`--management-token-file`), or HTTP Basic for a
user with `admin = true`. Reads are open unless `management_auth_reads = true`
(`/users` and `/permissions` always need credentials).

Runtime users, permissions and per-queue policies persist: in the store
(`data_dir`) standalone, in the metadata Raft group when clustered — where
they need every node on a version that supports them (`503` otherwise).
Without either they last until the broker stops. Runtime users need the
built-in user store (`[[users]]` in the config file, or
`Broker::with_runtime_users` when embedding).

A queue's own policy (on `[[queues]]`, or in `PUT /queues/...`) overrides
the prefix policies for that queue; its `dead_letter` is relative to the
queue's vhost. A dead letter whose target name already holds another kind of
queue (a consumer attached the dead-letter queue at `/queues/<name>` first,
say) goes to that queue rather than being dropped. In a prefix policy, `{queue}` in
`dead_letter` expands to each queue's name, giving every queue its own
dead-letter companion; the companions themselves are not dead-lettered by
that policy. A consumer's `rejected` (or `modified{undeliverable-here}`)
dead-letters through the queue's policy with reason `rejected`, and drops
the message only when there is no `dead_letter` target (RabbitMQ 4.x
semantics). Redeliveries after `modified{delivery-failed}` carry the
failure count in `header.delivery-count` (AMQP 1.0 §3.2.1; added to any count
the publisher set), so consumers can budget retries.

Secrets stay out of the file: point `verifier_file` / `password_file` at a
mounted Kubernetes Secret. Generate a verifier with
`echo -n "$PASSWORD" | ramqp-brokerd hash-password [--mechanism SCRAM-SHA-512]`.
The daemon also shuts down gracefully on `SIGTERM`.

Or embed it (`ramqp-broker = "0.11"` in `Cargo.toml`; add the `store-redb`
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
