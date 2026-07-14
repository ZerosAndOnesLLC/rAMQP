# rAMQP Broker — Plan (v2)

A **performance-first**, highly-available **AMQP 1.0 broker** in Rust, built by
extracting the role-agnostic engine out of the existing `ramqp` client into a
shared `ramqp-core`, then adding a server crate on top. Clean-room, no external
AMQP dependencies (same constraint as the client). Clustered from v1; single
protocol, done excellently; **fast and light before anything else**.

> **Status: building — Phases 0–9 complete; Phase 10 partially done** (fe2o3 interop, in-process fault injection, the runtime decision, the unified conformance harness, the Qpid JMS interop leg, process-level partition testing, and the tuned-incumbent/durability-parity bench re-run (provisional) are in; remaining: other external-toolchain legs (proton, our-client⇄Qpid broker), bare-metal bench numbers, Artemis-tuned config, and a multi-connection load sweep). See §11 checkboxes. The broker runs **clustered**:
> a 3-node cluster forms from static seeds over the inter-node fabric (one
> multiplexed TCP connection per peer pair carrying every Raft group + the
> forwarded data plane), quorum queues are declared through the replicated
> catalog with rendezvous placement, **any node serves any queue** through
> leader-following proxies, and killing the leader node mid-stream loses zero
> accepted messages — proven end-to-end with the unmodified `ramqp` client.
> **Durability is in**: `/durable/*` queues on redb with group-commit fsync,
> paged deep quorum queues (flat p50 to 1M+ deep, ~3× less RSS at 4 KiB bodies),
> TTL/max-length/dead-letter policies, and an on-disk Raft log — quorum queues
> and the catalog survive full broker restarts. Benchmarks (vs live RabbitMQ
> 4.3.1 and Artemis, incl. quorum-vs-quorum and the deep-queue matrix) are in
> `bench-compare/README.md`; CI runs the client interop suite against our own
> broker alongside RabbitMQ and Artemis. This v2 supersedes the original
> single-node plan after the scope decisions in §2.

---

## 1. Goal & non-goals

**Goal.** The **fastest, most resource-efficient AMQP 1.0 broker in existence** —
predictable tail latency, maximal throughput-per-core, and a small, bounded,
explicitly-managed memory footprint — highly available (clustered) from day one.
Performance and efficiency are **not a feature of this broker; they are the
product** (§3). Every design decision is judged first against "does this make us
measurably faster and lighter than every JVM/BEAM broker on the same hardware?"
Built in Rust for memory safety and no GC pauses; shipped as its own
crate/daemon so a client-only user never compiles broker code and vice-versa.

**Non-goals (deliberate, not "for now").**
- **Other protocols.** AMQP 0.9.1, JMS/OpenWire, MQTT, STOMP, Kafka wire — all
  out. This is **AMQP 1.0 only**, a committed strategic choice (§2). The internal
  model may be 1.0-shaped where convenient; we do **not** pay for protocol-neutral
  abstraction we may never use.
- Re-implementing the wire codec or the session/link state machines — reused
  from `ramqp-core`.
- Hand-rolling consensus — we use **openraft** (§2, §8). Consensus is not AMQP,
  so this doesn't touch the clean-room constraint.
- A management **UI** (a management *protocol*/admin API + metrics export is in
  scope; a web console is later).

**Explicitly deferred until the core is fast and stable (owner's call):**
- **Adoption / go-to-market / compatibility polish.** We chase numbers first; the
  masses come after the broker is demonstrably the fastest and rock-stable. No
  effort spent courting adoption until §3's targets are met and held.
- Beating incumbents on **feature breadth** or **ecosystem**. We win on
  performance in a narrow lane, not a feature race.
- Millions of concurrent connections **per node** (C10M). Horizontal scale via
  the cluster; per-node connection scaling is moderate initially.

**House constraints carried over:** `#![forbid(unsafe_code)]`, async-first
(tokio), no paid crates without asking, scale-for-millions / perf-critical, fix
all warnings, bump `Cargo.toml` version + `cargo check` before each commit,
update READMEs.

---

## 2. Locked decisions (the scope contract)

These are settled; everything downstream follows from them. Revisit only with
cause. **Overriding priority: performance (§3) outranks every other axis** — when
a decision trades speed/footprint against features, ergonomics, or adoption,
speed/footprint wins until the core is stable.

| # | Decision | Consequence |
|---|---|---|
| D1 | **Full production broker**, not a reference/test peer. | The long tail of broker semantics (durability, DLX, TTL, authz, management) is all in scope — but subordinate to §3. |
| D2 | **Clustered / HA from v1.** | The storage layer is a replicated state machine from the start; "in-memory first, persistence later" is out. |
| D3 | **AMQP 1.0 only**, committed. | No protocol-neutral core abstraction; the node/queue/settlement model is 1.0-native. |
| D4 | **Per-queue Raft groups + one metadata Raft group** (RabbitMQ quorum-queue model). | Each replicated queue is its own state machine; a metadata group owns membership + placement. Best fit for AMQP settlement semantics; load-balances across nodes. |
| D5 | **Use `openraft`** for consensus (not hand-rolled, not `raft-rs`). | Async-native, MIT/Apache, free. We own storage + network + the state machine + the multi-raft manager on top of it. |
| D6 | **Opt-in HA: quorum vs transient queues** (RabbitMQ model). | A queue is declared replicated (own Raft group) or transient/local (single-node, no consensus cost). Transient queues are also the single-node MVP. |
| D7 | **Isolation by crate boundary, not feature flags** (§4). | Client-only builds never compile broker code; broker builds never compile client api/resilience. |
| D8 | **Performance is the product** (§3). | Explicit latency/throughput/footprint targets; a per-phase performance gate; no merge that regresses the tracked benchmark. |

**Positioning.** We win a lane by being **the fastest**: predictable tail latency
+ small bounded footprint + memory-safe + AMQP-1.0-native + HA + open-source.
That combination does not exist today.

- vs **JVM brokers** (Artemis, Qpid-J, Kafka): the no-GC latency argument is real
  and winnable — our strongest ground.
- vs **RabbitMQ** (Erlang/BEAM): weaker on latency (BEAM's per-process GC is
  already soft-realtime); we win on **footprint** and **throughput-per-core**.
- vs **Solace / Redpanda-tier**: they own high-perf today; we challenge on latency
  by earning it in the data path (§3), not by slogan.

---

## 3. Performance — the north star (non-negotiable)

**Performance and efficiency are the product.** Not a phase, not a "later"
concern — the axis every other decision is judged against. The no-GC advantage is
**earned by the data path, not granted by the language**: a Rust broker with
per-message `Arc` churn, allocator pressure, or unbounded buffering can jitter
worse than a tuned JVM broker. Redpanda's win came from *architecture*
(thread-per-core + zero-copy + io_uring), not merely "C++ has no GC." We earn it
deliberately.

### 3.1 Targets (provisional — calibrate on real hardware, then defend forever)

Numbers are placeholders to be pinned in Phase 4 against a **tuned** Artemis and
RabbitMQ on identical hardware; the *shape* of the targets is not negotiable.

| Dimension | Target |
|---|---|
| **Tail latency** | Beat tuned Artemis/RabbitMQ on **p99 and p99.9** by a clear margin; keep tails flat: **p99.9 / p50 ≤ ~10×** under sustained load. Tail flatness is the headline claim. |
| **Throughput** | **≥** a tuned RabbitMQ/Artemis on small-message msgs/sec **per core**; scale ~linearly with cores (share-nothing). |
| **Memory** | **Bounded, configurable RSS** — never unbounded growth under backpressure or deep queues; low idle per-connection overhead (target < tens of KB). Deep queues bounded by paging (§8). |
| **Allocation** | **Zero heap allocations on the steady-state per-message hot path** — buffers pooled/reused, `bytes::Bytes` slices for zero-copy. Allocation only on connection/queue setup. |
| **CPU** | **No locks and no shared mutable state on the message hot path** (the one-task-per-connection / per-shard actor model gives this for free). |

### 3.2 Hot-path constitution (design rules, enforced in review)

1. **Zero-copy wherever the protocol allows** — reference-counted `bytes::Bytes`
   slices, vectored writes (already in `FramedTransport`). A message body is
   copied at most once (ingest), ideally never again to the consumer.
2. **Bounded memory, always** — every buffer, queue, and channel has a cap +
   backpressure. Flow-to-disk (paging) *before* OOM, never after.
3. **No locks on the hot path** — share-nothing per connection/shard; cross-shard
   work goes through message passing, not shared mutices.
4. **Minimal allocation** — pool and reuse; avoid per-message `Arc` unless it buys
   a real zero-copy. Prefer arrays/slabs over node-per-message structures.
5. **Batch aggressively** — frame batching, group-commit fsync, Raft batch commit,
   vectored socket writes. Batching is the single biggest throughput lever.
6. **Cache-friendly data** — contiguous, index-addressed queue storage over
   pointer-chasing; keep hot metadata small and dense.
7. **Measure everything** — every hot path ships with a benchmark; a merge that
   regresses the tracked benchmark is rejected (§3.4).

### 3.3 Runtime model — the fork we optimize toward

Performance is the priority, so the runtime is a **first-class, conscious**
decision, not inherited by default. Three paths, ordered by determinism:

1. **tokio work-stealing (inherit the core engine).** Lowest friction; likely
   *enough* to beat the JVM brokers. Where we **start** — to get to measurable
   numbers fast.
2. **Sharded tokio per core (current-thread runtime pinned per core, share-nothing
   queue partitioning).** Most of the thread-per-core benefit while staying in the
   tokio ecosystem. The pragmatic **destination**.
3. **Full thread-per-core share-nothing (glommio / monoio / io_uring).**
   Redpanda-tier determinism; hardest model; the **escalation** if p99.9 demands it.

**Standing rule:** architect the queue/dispatch layer **shard-partitioned from
day one** so moving 1 → 2 → 3 is cheap, and let the benchmark — not taste —
decide when to escalate. io_uring and monoio/glommio are on the table the moment
the numbers justify them.

### 3.4 Performance as a gate (not a phase)

- **Continuous benchmark harness** in `bench-compare` (already a dev-only member
  with `fe2o3-amqp`): p50/p99/p99.9 tail latency + RSS-under-load, run against
  **tuned** Artemis and RabbitMQ, **results committed in-repo**. Stands up in
  Phase 4 and runs from then on.
- **CI regression guard:** every hot path has a bench; a commit that regresses the
  tracked numbers beyond a threshold **blocks the merge**. Performance is defended
  continuously, not audited once.
- **The receipts are the moat.** Our audience adopts a flame graph, not a slogan;
  "no GC" is a hypothesis until the p99.9 numbers prove it. Reproducible,
  published benchmarks vs tuned incumbents are a product deliverable.

---

## 4. Crate structure

Three crates in one workspace, **isolation by crate boundary, not feature flags** (D7):

```
ramqp-core    role-agnostic engine: codec, types, framing, session/link
              state machines, negotiate, sasl primitives, txn types, observe
ramqp         the CLIENT (api + resilience) on top of core   → published, unchanged name
ramqp-broker  the BROKER (listener, server handshake, queues/store, cluster) on top of core
```

- Client-only user: `ramqp = "*"` → pulls `ramqp-core` only. Broker never compiled.
- Broker operator: `ramqp-broker = "*"` → pulls `ramqp-core` only. Client api/resilience never compiled.
- **Clustering lives inside `ramqp-broker`** (openraft + multi-raft manager +
  metadata/queue state machines) as modules first. Extract a `ramqp-cluster`
  crate only if it earns its keep — don't abstract preemptively.
- Federation/shovel (broker dialing an upstream): depends on **both**; they share one compiled `ramqp-core`.
- Optional later: a `ramqp` **facade** re-exporting the broker behind a `broker` feature (the sqlx UX). Cosmetic; decide last.

Why not one crate with a `broker` feature: Cargo features unify across the whole
dep graph, so any crate enabling `ramqp/broker` would force the broker into every
`ramqp` consumer in that build. Separate crate names can't be unified in.

---

## 5. Target workspace layout

Recommended: **virtual workspace root** at `~/dev/rAMQP` (sqlx-style). The crate
*name* `ramqp` is preserved, so crates.io users are unaffected; only the repo
path moves to `ramqp/`.

```
~/dev/rAMQP/
  Cargo.toml            # [workspace] virtual root, members = [...]
  broker.md             # this file
  ramqp-core/
    Cargo.toml          # name = "ramqp-core"
    src/...
  ramqp/                # the published client (moved into a subdir)
    Cargo.toml          # name = "ramqp", depends on ramqp-core
    src/...
  ramqp-broker/
    Cargo.toml          # name = "ramqp-broker", depends on ramqp-core, openraft
    src/lib.rs
    src/cluster/...            # openraft glue, multi-raft manager, metadata + queue SMs
    src/bin/ramqp-brokerd.rs   # the daemon
  bench-compare/        # dev-only member; fe2o3 peer + the §3.4 latency/RSS harness
```

Lower-disruption alternative: keep `ramqp` as the **root package** and add
`ramqp-core` / `ramqp-broker` as path members (manifest stays at repo root). Same
crate names; pick this if moving the client into a subdir is undesirable.

> **Open item (decide before Phase 0):** virtual-root vs root-package (churn vs
> tidiness on a *published* crate). The empty `~/dev/rAMQP-Broker/` dir referenced
> in v1 is dead — current cwd is already `~/dev/rAMQP` (the workspace). Remove it.

---

## 6. Extraction map — what moves where

Verified against the source. `split` = the module is partly role-neutral and
partly client-directional; the table says how to cut it.

| Current (`ramqp/src/...`) | → Destination | Notes |
|---|---|---|
| `codec/*` | **core** | Pure encode/decode. As-is. |
| `types/*` (definitions, messaging, performatives, sasl, mod) | **core** | All wire types: `Role`, settle modes, error model, `Source`/`Target`, `DeliveryState`, every performative, SASL frame types. Broker needs `Source`/`Target` for addressing. |
| `transport/frame.rs` | **core** | `FramedTransport<S>` is generic over `S: IoStream`; fully symmetric. As-is. Zero-copy/vectored write path lives here — perf-critical (§3). |
| `transport/header.rs` | **core** | `ProtocolHeader` (`AMQP`/`TLS`/`SASL` consts, `read`/`write`/`from_bytes`). `negotiate()` is client-shaped (write-then-read) — keep it; broker calls `read` then `write` itself. |
| `transport/mod.rs` | **split** | `IoStream` trait + `Address`/`Scheme` parsing → **core**. `connect*()`, `Transport` enum (client TLS/WS variants), `connect_tls` → **client**. Broker adds its own acceptor + server `Transport`. |
| `transport/tls.rs` | **split** | Client uses `tokio_rustls::client::TlsStream`. → **client**. Broker writes a `server::TlsStream` acceptor (mirror). |
| `transport/ws.rs` | **split** | `WsByteStream` codec → **core/shared**; `connect_ws` → **client**; broker adds WS upgrade/accept. |
| `connection/negotiate.rs` | **core** | `build_open`/`reconcile` (min-of-both, symmetric)/`close_to_error`. As-is. |
| `connection/mux.rs` | **core** | `ChannelAllocator`, `RemoteChannelMap`. Symmetric. |
| `connection/heartbeat.rs` | **core** | Idle-timeout. Symmetric. |
| `connection/driver.rs` | **client** | The client connection actor (client polarity — see §7). Broker writes its own driver; shared inner loop may be lifted to a core `Connection` engine later. |
| `session/*` (state, window, registry, mod) | **core** | Session state machine + flow windows + handle allocators. ~90% role-neutral; gains additive server-side `accept_*` methods (§7). |
| `link/*` (sender, receiver, credit, delivery, settlement, resume, mod) | **core** | Link state holders; both roles already present. As-is. |
| `sasl/mod.rs` | **split** | `ScramMechanism` math (h/hmac/pbkdf2) → **core** (behind `scram`). `SaslProfile`, client `negotiate()`, `ScramClient` → **client**. Broker writes the server flow + `ScramServer` + credential verification. SASL *frame* types already in `types/sasl.rs` → core. |
| `proto/mod.rs` | **core** | `DriverCommand`/`SessionEvent`/`LinkEvent`/`IncomingDelivery` vocabulary. Shared; the broker may add command variants. |
| `ids.rs` | **core** | `ContainerId`/`ChannelId`/`SessionId`/`DeliveryId`/`Handle`. |
| `error.rs` | **split (mostly core)** | Protocol errors → core. Confirm no client-only variants leak; client re-exports. |
| `config/mod.rs` | **split** | → **core**: `container_id`, `hostname`, `max_frame_size`, `channel_max`, `idle_timeout`, `SessionConfig`, `CreditMode`, link settle-modes/`max_message_size`. → **client**: `ReconnectConfig`, `connect_timeout`, `command_buffer`, `reconnect`, `max_outbox`, presets. Broker gets its own config type. |
| `observe/*` | **core** | `Metrics` trait + `EventBus`. Broker wants metrics too — but metrics must be cheap on the hot path (§3). |
| `txn/*` | **split** | Txn *types* → **core**. Client controller → **client**. Broker **coordinator** (`amqp:coordinator` target) is net-new → **broker**. |
| `api/*` | **client** | The client's public handles. Stays. |
| `resilience/*` | **client** | Reconnect/pool. Stays. |

To keep the **published client's public API stable**, `ramqp/src/lib.rs`
re-exports the moved items from core (`pub use ramqp_core::{codec, types, ...}`),
so existing `use ramqp::...` paths keep working. A `tests/public_api.rs` (or
`cargo semver-checks` in CI) locks the re-export surface so a missed `pub use`
fails our build, not a downstream user's.

---

## 7. Reuse assessment — role-neutral vs net-new "server polarity"

The engine is largely symmetric (verified against source). The genuinely new
protocol work is the **server polarity of establishment** plus broker semantics
(which AMQP 1.0 does not define at all).

**Reusable as-is (role-neutral, verified):**
- Codec + all wire types.
- `FramedTransport` framing/batching/vectored writes; `ProtocolHeader` read/write.
- `reconcile()` open negotiation; channel mux; heartbeat.
- `Session`: windows, link registry, `on_transfer` (assembly), `flush_sender`
  (multi-frame send), `on_disposition`, `on_flow`, `send_disposition`,
  `grant_credit`, detach. A broker delivering to a consumer uses the *same*
  `flush_sender`; receiving from a producer uses the *same* `on_transfer`.

**Net-new server polarity (the real protocol work), verified against source:**
1. **Transport establishment.** `transport/mod.rs` is dial-only; no
   `TcpListener`/accept, no server TLS. Broker adds an acceptor + server
   `Transport` + `server::TlsStream`/WS-upgrade.
2. **Header sequencing.** `Driver::open` (`driver.rs:77`) writes `open` *then*
   reads the peer's. A server must **read first, then reply**. Broker drives
   `ProtocolHeader::read`/`write` + `reconcile` in server order.
3. **SASL direction.** Only `ScramClient` + a client `negotiate()` exist. Broker
   needs: advertise mechanisms → read `sasl-init` → verify credentials → send
   outcome, plus a `ScramServer` (server-first/server-final). Hash math reused.
4. **Peer-initiated session begin.** `route_session_frame` (`driver.rs:452`)
   *rejects* a peer `begin` (requires `remote-channel` set + local channel in
   `BeginSent`; else `ProtocolViolation`). Broker must **accept** a peer `begin`,
   allocate a local channel, reply. → add `Session::accept_peer_begin` (additive).
5. **Peer-initiated link attach.** `Session::on_peer_attach` (`state.rs:231`)
   only binds a response to a link *we* opened (`link_handles.get(name)`, returns
   if absent). Broker must **accept** a peer `attach`: create the mirror endpoint
   (peer `receiver`→broker `SenderLink`; peer `sender`→broker `ReceiverLink`),
   resolve `source`/`target` to a node, reply, and (broker-sender) grant credit.
   → add `Session::accept_peer_attach` (additive).

Items 4–5 are **additive methods on the core `Session`** — they do not alter the
client's code paths.

---

## 8. Cluster architecture (net-new — the defining subsystem)

D2+D4+D5+D6 assemble into one design. The unifying insight:

> **A replicated queue is a Raft state machine. The Raft log *is* the write-ahead
> journal. Group-commit fsync becomes "commit a Raft batch." Snapshots are log
> compaction. Paging is how the state machine sheds memory.**

This collapses what v1 treated as three separate subsystems (memory/paging,
durability, replication) into one — but it means the store must be a Raft state
machine **from the start**; you cannot build a naive in-memory queue and bolt
replication on later.

```
                 AMQP frontend  (any node accepts any connection)
                        │  attach /queues/foo
                        ▼
                 route to foo's Raft LEADER  ──►  internal forwarding fabric
                        │                          (cross-node RPC if leader
                        ▼                           is elsewhere)
   ┌─────────────── per-queue Raft group (foo) ───────────────┐
   │  replica set of N nodes; log entries = enqueue / settle   │
   │  state machine = queue contents + unacked map             │
   │  openraft snapshots = compaction; paging = memory relief  │
   └───────────────────────────────────────────────────────────┘

   metadata Raft group (all nodes): membership · queue catalog ·
   placement (queue→replica set) · policies · queue type (quorum|transient)
```

**Replication model (D4).** Per-queue Raft groups + one metadata group. Log
entries for a queue are `enqueue(msg)` / `settle(delivery_id, outcome)`; the
state machine is queue contents + the unacked map. Maps cleanly onto AMQP 1.0
settlement (accept/reject/release/modify are just state-machine commands) and
load-balances (different queues lead on different nodes).

**Queue types (D6).** Declared **quorum** (own Raft group, replicated, durable)
or **transient** (single-node, no consensus, cheap). Exclusive/auto-delete/temp
links default to transient. Transient queues are the Phase-4 single-node MVP and
a real shipping feature — not throwaway scaffolding.

**openraft + the multi-raft manager (D5, our biggest cluster build item).**
openraft runs **one** group per `RaftCore`; we run thousands (one per quorum
queue). Naively that's a per-queue tick/heartbeat/storage handle → heartbeat
storm + tokio-task explosion. We own a **multi-raft manager** that shares one
network transport, batches ticks/heartbeats across groups, and multiplexes log
storage — the way TiKV's multi-raft and RabbitMQ's Ra do. This is why RabbitMQ
caps practical quorum-queue counts; we design for it explicitly. **This layer is
also perf-critical** — batching (§3.2) is what keeps thousands of groups cheap.

**Leader routing / internal forwarding fabric.** Any node accepts the AMQP
connection, but a queue's data lives on its leader's replica set. AMQP 1.0 has no
native redirect, so we proxy internally (RabbitMQ's approach): the accepting node
forwards the session's transfers/dispositions to the leader — a server-internal
RPC/routing layer between the AMQP session and the queue leader. Its per-hop cost
is on the hot path, so it must be zero-copy and batched (§3.2).

**Cluster formation / bootstrap.** Node discovery + initial metadata-group
formation: static seed list first; pluggable discovery (DNS/k8s) later. Default
replication factor (3), placement policy, and rebalancing on node add/remove live
in the metadata group.

> **#1 design risk — deep queues (a performance risk).** Per-queue Raft is
> *weakest exactly where our "fast at large workloads" headline is strongest*:
> deep backlogs. The naive model replicates every byte through consensus and holds
> the queue in the state machine's memory (RabbitMQ quorum queues were historically
> memory-hungry and discouraged for very long queues). Mitigation is a deliberate
> Phase-7 design choice tied to §3's memory target: a **paged/segmented state
> machine** (spill message bodies to disk, keep only indices resident) and/or
> **splitting bulk payload off the consensus path** (log carries references; a
> separate streaming replication moves bodies). Get this wrong and the demo that
> sells the broker becomes the benchmark that sinks it.

---

## 9. Broker semantics (net-new; AMQP 1.0 leaves these undefined)

- **Node/address model.** Resolve `attach.source`/`target` to broker nodes. Pick
  an address convention (RabbitMQ-4.x-style `/queues/<name>`, `/exchanges/...`, or
  a simpler flat namespace) — decide early; document it. Encodes queue type
  (quorum|transient) at declaration.
- **Queues / topics.** Ordered queues; optional pub/sub (topic) fan-out. Queue
  storage is contiguous/index-addressed for cache-friendliness (§3.2).
- **Message store.** For **quorum** queues: the Raft state machine + snapshots
  (§8). For **transient-durable** queues: a local durable store (append log +
  index). Candidate free embedded substrate: `redb` (safest; `sled` in long-term
  limbo, `fjall` newer). No paid crates. SQL metadata store, if ever, uses **sqlx
  migrations** per house rules.
- **Dispatch / fan-out.** Deliver enqueued messages to attached consumer
  (broker-sender) links honoring link credit + session windows (reuses
  `flush_sender`); round-robin/competing-consumer per queue. Zero-copy from store
  to socket where the protocol allows (§3.2).
- **Settlement & redelivery.** Map consumer dispositions
  (accepted/rejected/released/modified) to ack/requeue/drop; redelivery counters.
  For quorum queues these are Raft-committed state transitions.
- **Dead-lettering, TTL, max-length** policies.
- **Flow-control policy.** Credit issuance strategy, memory/backpressure caps,
  per-connection/queue resource limits (slow-loris/abuse protection). Backpressure
  is the mechanism that keeps memory bounded (§3.1).
- **Transactions.** The `amqp:coordinator` target + txn coordinator
  (commit/rollback of enqueues/dequeues), made cluster-aware.
- **Auth/authz backend.** Pluggable credential verification (PLAIN/EXTERNAL/SCRAM)
  + per-address permissions. SCRAM-server needs a credential store abstraction.
- **Management/admin.** Declare/delete/inspect queues; Prometheus metrics export;
  admin API. Metrics collection must stay off the hot path (§3.2).

**Daemon:** `src/bin/ramqp-brokerd.rs` + a broker `Config` (listen addrs, TLS,
auth source, storage backend, cluster seeds/replication defaults, policies).

---

## 10. Feature matrix

Features are per-crate and orthogonal (backends, not "client vs broker"):

| Feature | core | client | broker |
|---|---|---|---|
| `rustls` / `native-tls` | — | client TLS connect | **server TLS accept** |
| `ws` | WS codec | client WS connect | **server WS upgrade** |
| `scram` | hash math | `ScramClient` | **`ScramServer` + verify** |
| `transaction` | txn types | controller | **coordinator (cluster-aware)** |
| `cluster` (openraft) | — | — | **broker-only: multi-raft, metadata + queue SMs** |
| persistence (`store-redb`, …) | — | — | **broker-only (transient-durable)** |
| `io-uring` (runtime escalation) | — | — | **broker-only, latency-max path (§3.3)** |

---

## 11. Phased execution plan

Working-plan rules: one sub-phase at a time, `cargo check` + version bump +
commit after each, update READMEs on significant change. Mark `[x]` when done.
**Phases 0–4 are independent of clustering** — near-term work is stable.

> **Performance gate (from Phase 4 on):** no sub-phase is "done" if it regresses
> the tracked `bench-compare` numbers (§3.4); every new hot path ships with a
> bench. This gate outranks feature completeness.

### Phase 0 — Workspace scaffolding ✅
- [x] Decide virtual-root vs root-package layout (§5) → **virtual root**; removed dead `~/dev/rAMQP-Broker`.
- [x] Create the `[workspace]` (virtual root, `default-members` excludes `bench-compare`); moved client to `ramqp/`; added empty `ramqp-core` + `ramqp-broker` (0.1.0) members; client builds + 102 unit tests pass.
- [x] CI: `default-members` builds/tests the three real crates; `bench-compare` (fe2o3) stays out of default builds; `release.yml` now publishes `-p ramqp`.

### Phase 1 — Extract `ramqp-core` (mechanical, behavior-preserving) ✅
Executed in true bottom-up dependency order (the checklist order below had
`error`/`config` too late — `transport`/`session`/`link` depend on them):
codec/types/ids/observe → error/config → transport split → proto →
link/session → negotiate/mux/heartbeat → txn/sasl splits.
- [x] Move `codec`, `types`, `ids`, `observe` to core. Client re-exports (incl. the `#[macro_export]` `amqp_composite!`).
- [x] Move `error` (wholesale) + `config` to core. **Deviation:** `config` moved wholesale, not field-split — splitting `ConnectionConfig`'s public fields would break the client API the gate protects; broker gets its own config type anyway.
- [x] Move `transport/frame.rs` + `header.rs` + `IoStream`/`Scheme`/`Address` to core (`connect*`/`Transport`/`TlsConfig`/`tls`/`ws` stay client-side).
- [x] Move `proto/mod.rs` to core.
- [x] Move `session/*` and `link/*` to core (CreditMode `#[non_exhaustive]` matches in client became if-let — cross-crate exhaustiveness).
- [x] Move `connection/negotiate.rs`, `mux.rs`, `heartbeat.rs` to core; client keeps `driver`.
- [x] Split `txn` (wire types/helpers → core behind `transaction`; `TransactionController` stays) and `sasl` (SCRAM math/saslprep/nonce/ct_eq → core behind `scram`, + new `unescape_username` for Phase 2; `SaslProfile`/`ScramClient`/`negotiate()` stay). Crypto deps moved client→core.
- [x] `tests/public_api.rs` locks the full pre-0.8 re-export surface (compile-time, both feature sets).
- [x] **Gate:** full suite green (default + --all-features), clippy/fmt/docs clean, benches compile; client **0.8.0**, `ramqp-core` **0.1.0**. READMEs + CHANGELOG updated.

### Phase 2 — Server-side primitives in core (additive) ✅
- [x] `Session::accept_peer_begin` (maps immediately from a peer begin; returns our responding begin; handle allocation bounded by min of both handle-max values) + `Session::knows_link` for driver routing.
- [x] `Session::accept_peer_attach` (mirror endpoint, responding attach echoing caller-resolved source/target, initial-delivery-count adoption/declaration per spec §2.6.7, optional initial credit flow; duplicate→ProtocolViolation, exhaustion→Capacity) + `SenderLink::accepted`/`ReceiverLink::accepted`.
- [x] `header::accept` (read-first, echo-or-counteroffer per §2.2); `sasl::server` with `parse_plain_response` + `ScramServer`/`ScramVerifier` (RFC 5802 server side, verifier-based storage — no plaintext; channel-binding demands rejected; credential store itself deferred to Phase 9).
- [x] Tests: 5 server-session, 2 header-accept pairing, 7 SASL-server (incl. the RFC 5802 server vector), and 2 client⇄core SCRAM interlock tests over the real `negotiate()` (mutual auth + wrong-password rejection). Client untouched. Core → **0.2.0**.

### Phase 3 — Broker skeleton (`ramqp-broker`) ✅
- [x] TCP acceptor (`Broker::bind` → `BoundBroker::run`), one owning task per connection (client's lock-free actor model), TCP_NODELAY, watch-channel graceful shutdown, `serve_stream` for in-process transports. (Queue/dispatch shard-partitioning lands with the queue layer in Phase 4 — carried there.)
- [x] Server handshake: `header::accept` read-first (SASL required unless the authenticator allows unauthenticated), server SASL (ANONYMOUS/PLAIN via pluggable `Authenticator`; `AllowAll` + constant-time `StaticPlain`), read-first `open` with the client's frame-size-floor validation, `reconcile`, heartbeat. Handshake bounded by `connect_timeout` (slow-loris guard).
- [x] Broker connection driver: biased select loop (reads → link/session event drains → heartbeat → shutdown), inbound routing into core `Session` via `accept_peer_begin`/`accept_peer_attach`/`handle_link_frame`; duplicate-open/unknown-channel/SASL-after-open → connection errors; rejected attach → session end with `resource-limit-exceeded`. **Decision (was open): standalone broker driver, not a shared core `Connection` engine** — revisit only if Phase 4+ duplication proves costly.
- [x] **Smoke test green:** the unmodified `ramqp` client over loopback TCP — ANONYMOUS + PLAIN (good/bad/unoffered credentials), begin/attach producer+consumer, session end/reopen, graceful close, 16 concurrent connections.

### Phase 4 — Single-node MVP (transient queues) + establish the benchmark ✅
- [x] Address→queue resolution (`/queues/<name>` + bare names, auto-declare) + in-memory transient queues: **one owning actor task per queue** (lock-free; deliberately the shape the Phase-6 Raft state machine needs — Publish/Settle are the future log commands). Bounded depth: overflow → `rejected` (resource-limit-exceeded).
- [x] Producer path: peer sender → `ReceiverLink` → handle-attributed delivery events → queue Publish (bounded mailbox back-pressures the producer) → disposition acks; batched credit replenishment. Bodies stay `Bytes` end-to-end (refcount only).
- [x] Consumer path: peer receiver → `SenderLink` → queue Subscribe; peer flow credit → queue demand; round-robin dispatch via per-connection command channel → `send_transfer` (credit + windows enforced by core).
- [x] Settlement: per-dispatch outcome futures → accepted→ack, released→requeue, modified→requeue(+failure count), rejected→drop; settle-owner verification; unsubscribe/detach/teardown requeue unacked. Competing consumers round-robin.
- [x] **Two systemic bugs found & fixed under load:** (1) queue⇄connection bounded-channel deadlock — resolved by channel orientation (queue→conn unbounded-but-credit-bounded; conn→queue bounded for producer backpressure; wait-for graph now acyclic); (2) **core bug:** a pure session flow (no handle) never re-flushed window-stalled senders — any dispatch-driven peer stalled at exactly `incoming-window` transfers. Fixed in core + regression tests (core unit + 50k blast).
- [x] §3.4 harness stood up (`bench-compare/latency`: closed-loop p50/p90/p99/p99.9 + blast throughput + RSS, in-process or any URL) and **first numbers recorded vs live RabbitMQ 4.3.1 and Artemis on this machine** (untuned defaults; see bench-compare/README): **p50 89µs vs 251/227µs, p99.9 428µs vs 777/833µs, 326k msg/s vs 48k/79k, ~40MiB vs 133/715MiB.** Deferred: tuned-incumbent isolated runs; CI regression-guard wiring (needs a perf-stable runner — harness is the tooling).
- [x] End-to-end: client `produce_consume` example (now env-configurable) runs against `ramqp-brokerd` (new daemon bin); 8 e2e integration tests + blast regression. Commit.

### Phase 5 — Cluster foundation ✅ (catalog→queue wiring folded into Phase 6)
- [x] `openraft` 0.9.24 integration: `MetaTypeConfig` (declare_raft_types), in-memory `RaftStorage` (log/vote/snapshot/apply via the Adaptor), and an in-process `Router` network — the seam where the TCP inter-node transport slots in next. (Multi-raft manager comes with per-queue groups in Phase 6.)
- [x] **Metadata Raft group**: replicated queue catalog (`MetaCommand::Create/DeleteQueue`, `QueueSpec{quorum|transient, replicas}`, idempotent apply); membership via openraft (add_learner/change_membership).
- [x] **TCP inter-node transport** (`cluster::tcp`): length-prefixed serde_json RPC (JSON control plane — the binary codec + connection sharing arrive with the Phase 6 multi-raft manager), lazily-reconnecting per-peer clients, `serve_raft` acceptor; 3-node cluster forms and replicates over real sockets.
- [x] **Static-seed bootstrap** (`cluster::bootstrap`): `ClusterConfig{node_id, raft_listen, seeds}`; lowest seed id initializes with retry-until-quorum (already-initialized → no-op, restart/race-safe); `ClusterHandle::await_membership`. Test: 3 nodes bootstrap concurrently from the same seed list, agree on a leader, converge. (Queue-declaration wiring through the catalog lands with Phase 6 quorum queues, where the catalog gains its consumer.)
- [x] Tests: single-node group applies/deletes; **3-node cluster forms, catalog replicates to every node; leader kill → re-election → post-failover writes converge on survivors**; learner joins and catches up. (Node-*restart* durability needs the on-disk log — Phase 7.) Bench unchanged: the cluster layer is not yet on any message path.

### Phase 6 — Quorum queues ✅
- [x] Per-queue Raft state machine (enqueue/settle log entries + unacked map).
- [x] Quorum-vs-transient declaration wired through the address model (`/quorum/<name>`).
- [x] Snapshots / log-compaction (bincode snapshots; `LogsSinceLast` policy; built off-lock, off the async worker).
- [x] **Leader routing + internal forwarding fabric** (any node serves any queue), zero-copy + batched. Landed as: the **fabric** (`cluster/fabric.rs`) — one multiplexed TCP connection per peer pair, correlation-id RPC (no head-of-line blocking, cancel-safe), bincode Raft payloads, raw-`Bytes`-tail message bodies (never serde'd), batched single-flush writes — carrying every group's Raft traffic (shared-transport half of the multi-raft manager) **and** the data plane; the **cluster node** (`cluster/node.rs`) — catalog-driven declaration with deterministic rendezvous placement recorded in the catalog, StartGroup fanout + lazy member heal, `MetaWrite` leader forwarding, leader-side publish/subscribe bridging onto unmodified queue actors (per-connection ordered forwarder preserves producer FIFO); and the **proxy** (`proxy.rs`) — a leader-following local actor speaking the queue-mailbox protocol, so the connection driver is untouched; it re-resolves on failover, migrates subscriptions (re-arming outstanding demand), and retries in-flight publishes (epoch-guarded rebinds). Quorum actors exit on demotion (a follower never dispatches). Clustering is opt-in via `BrokerConfig::cluster` / daemon `--node-id/--cluster-listen/--seed`; standalone brokers are unchanged.
- [x] Test: produce to a quorum queue, **kill the leader mid-stream**, consumer continues, zero loss — proven at three scopes: single-replica group (`leader_death_loses_no_committed_message`), 3-node fabric level (`killing_the_leader_node_loses_no_accepted_message`), and client-facing e2e with the unmodified `ramqp` client (`kill_leader_mid_stream_loses_nothing`, plus `any_node_serves_any_queue`).
- [x] **Bench: quorum-queue tail latency/throughput vs RabbitMQ quorum queues** (bench-compare/README): 3-node cluster over real TCP — p50 288µs leader-local / 427µs via follower (one fabric hop ≈ +140µs), p99.9 < 1ms, 140–165k msg/s vs RabbitMQ 4.3.1 quorum 2234µs p50 / 39k msg/s — with the honest caveat that our log is in-memory vs RabbitMQ's fsync (durability parity is the Phase 7 re-run) while our leg replicated ×3 vs RabbitMQ's single member. The bench also flushed out a real bug — close-time settlement drains defeated by tokio's cooperative budget requeued acked messages as duplicates on every busy close — fixed + regression-tested. Commit.

### Phase 7 — Durability & deep-queue scaling ✅ (the §8 #1 risk)
- [x] Durable-local store for non-replicated durable queues: `/durable/<name>` addresses backed by **redb** behind the `store-redb` feature (`data_dir` config / brokerd `--data-dir`). One DB file per node; all writes ride a single **group-commit writer** (one fsync amortizes every publish in flight across all durable queues); a publish's accepted disposition is the on-disk durability confirm. Restart recovery = the ready-set seed from a store scan — proven by a client-facing stop-the-broker/start-a-new-one test (`durable_messages_survive_a_broker_restart`); settled messages never resurrect.
- [x] **Paged/segmented state machine** for deep replicated queues (§8 risk mitigation; defends §3.1 memory target). Landed as `cluster/paging.rs` + a paged `QueueState`: with a `data_dir`, bodies beyond a per-queue resident budget (`resident_bytes_max`, default 64 MiB) spill to append-only segment files — the index stays resident, the bytes do not; segments reclaim when fully settled; snapshot builds pin segments against concurrent reclamation, keep spilled bodies **external** (refs, so a deep queue's snapshot never materializes gigabytes), and park snapshot *blobs* on disk instead of RAM. Known limitation (documented): follower catch-up *via snapshot* for a deep paged queue is node-local-only — log-replay catch-up works; segment shipping is the follow-up, as are incremental snapshots (each build still clones+serializes the full index — the p99.9 spikes in the bench).
- [x] **Restart recovery for quorum queues** (on-disk Raft log/vote/snapshot): `SharedStore` gains a write-through `RaftLogSink` — log appends, votes, truncations, purges, and snapshot pointers are durable **before** the Raft storage call returns (Raft safety), implemented on the redb store behind `store-redb` (the same group-commit writer: one fsync amortizes across queues AND groups). Recovery seeds the store at group creation (snapshot restore + log replay by openraft; the quorum actor waits for replay before seeding its ready-set); spill segments are preserved across restarts and their live counts recomputed from the restored state. Guard rails: with a data dir configured, an unopenable store fails the attach/bind loudly instead of silently starting empty (which would shadow persisted state). Proven client-facing: standalone quorum restart, and a clustered (metadata-catalog) restart — both recover unsettled messages in FIFO order, settled ones never resurrect, and the recovered queue accepts new work. **Dead-letter, TTL, max-length policies: done** — `BrokerConfig::policies` (prefix-matched `QueuePolicy`: `message_ttl`, `max_length` + `RejectPublish`/`DropHead` overflow, `dead_letter` target, `max_delivery_attempts`), enforced in all three queue actors: lazy head-of-queue TTL expiry (quorum stamps enqueue time into the log so replicas agree; durable persists it), drop-head displacement, poison-message dead-lettering after N failed attempts; one broker-wide dead-letter router (weak-ref'd, best-effort, pre-settled republish). Client-facing tests for all four behaviors incl. quorum TTL.
- [x] **Bench: tail latency held flat as queue depth grows into the millions** (`bench-compare` `depth` bin; results in bench-compare/README): p50/p99 flat from empty to 1M deep (117→125 µs / ~250 µs); paging headline at 4 KiB bodies — **878 MiB vs 2461 MiB RSS (~3× less) at identical p50**, drain-out 248k msg/s from disk. Commit.

### Phase 8 — Transactions ✅
- [x] `amqp:coordinator` target + txn coordinator (commit/rollback), cluster-aware. Landed as: coordinator link acceptance (a sender attach targeting `Coordinator` binds a control link), `declare`/`discharge` control messages answered per spec part 4 (`declared{txn-id}` outcome; unknown-txn discharge → `rejected`), transactional **enqueues** (transfers carrying `transactional-state` stage instead of publishing) and transactional **settlements** (consumer dispositions carrying `transactional-state` stage their acks). Commit publishes every staged enqueue through its queue's own confirm — a Raft commit for quorum queues, an fsync for durable ones, which is exactly what makes the coordinator **cluster-aware** — then applies staged settlements; rollback drops enqueues and requeues settlements; a dropped connection rolls back implicitly (local-transactions scope). Staging is bounded (64 txns/connection, 10k ops/txn). Client grows `Consumer::settle_in_txn`; core grows `declared_state`/`txn_state`. Six client-facing tests. Commit.

### Phase 9 — Auth, limits, management ✅
- [x] Pluggable authn/authz + per-address permissions; SCRAM credential store. Landed as: the `Authenticator` trait grows `scram_verifier` (verifier-based storage — salted+iterated, no plaintext at rest) and `authorize(identity, vhost, address, operation)`; the SASL server flow speaks SCRAM-SHA-1/-256/-512 (full RFC 5802 exchange incl. the server-final signature in the outcome's additional-data, mutual auth against the unmodified client); `StaticScram` is the built-in verifier store. Every link attach is authorized BEFORE queue resolution (an unauthorized attach cannot even auto-declare); refusals are link-level (`unauthorized-access`, session survives); the transaction coordinator authorizes as `$coordinator`.
- [x] Resource limits, backpressure, slow-loris guards, multi-tenant vhosts — connection cap (`max_connections`), auto-declare cap (`max_queues`), slow-loris handshake timeout, bounded queue depth/policy `max_length`, bounded txn staging, and now **vhosts**: an `open.hostname` of `vhost:<name>` namespaces every queue (name, storage, catalog, policies, permissions) as `<vhost>/<name>` — same address, different vhost, different queue (tested). Dead-letter targets compose across the scheme (a per-vhost policy addresses `/queues/<vhost>/dead`).
- [x] Management/admin API + Prometheus metrics export (off the hot path): `BrokerConfig::management_listen` / brokerd `--management-listen` serves a dependency-free HTTP endpoint — `GET /metrics` (Prometheus text: connections, RSS, per-queue ready/unacked/consumer gauges) and `GET /queues` (JSON inspection). All queue stats are collected at scrape time by asking the actors (a `Stats` mailbox message) — nothing on the message path. Queue delete + a richer admin protocol ride with the management follow-up. Commit.

### Phase 10 — Interop, conformance, perf, docs 🟡 (external-toolchain legs remain)
- [~] Interop matrix: **our-client⇄our-broker is in CI** (the same `tests/broker.rs` suite that runs against RabbitMQ/Artemis, run against ramqp-broker); **`fe2o3-amqp`⇄our-broker landed** (`bench-compare/tests/fe2o3_interop.rs`: an independent AMQP 1.0 implementation exercises our handshake, links, transfers, dispositions incl. release/redelivery, and quorum queues); **Apache Qpid JMS⇄our-broker landed** (`tests/jms_interop.rs` + `tests/interop/jms/`, CI job `interop-jms`: the pure-Java Qpid JMS 2.x client round-trips through our broker); **Apache Qpid Proton (Python/C)⇄our-broker landed** (`tests/interop/proton/`, CI job `interop-proton`) — a third independent stack. The proton leg flushed out a real engine bug: `ramqp-core` keyed session links by name alone, so proton's identical sender/receiver link names (both derived from the address) collided; fixed to key by (name, role) per §2.6.1 (core 0.2.5, regression-tested). All verified locally. Remaining: our-client⇄Qpid Broker-J (reference-broker queue provisioning).
- [x] Spec conformance: core carries golden byte-vector codec tests + a spec-conformance audit; the SASL/SCRAM RFC vectors live in `ramqp-core`. **The single broker-side conformance harness landed** (`tests/conformance.rs` + the shared `tests/harness/`): one matrix across framing (header/open, directional max-frame), error-conditions (each violation asserts the *exact* `amqp:*` condition symbol — duplicate open, unmapped channel, slow-loris), flow (credit ceiling via manual-credit client), and settlement (terminal `accepted` removes the message). The old `tests/adversarial.rs` raw-socket cases were folded in (and its `error.is_some()` checks tightened to exact symbols); behavioral queue semantics stay in `produce_consume`/`quorum_queue`.
- [~] **Jepsen-style HA fault injection** — landed in-process: kill-the-leader-mid-stream zero-accepted-loss (3 scopes), **rolling leader kills to the availability boundary** (2/3 alive → recovers and accepts; 1/3 alive → quorum lost → publishes cleanly refused, never silently accepted, never hung — CP behavior verified), and **follower-loss transparency** (no refusals, no loss). The rolling-kill test flushed out a real bug: a dying node's still-open fabric connections could lazily resurrect an EMPTY group member, whose conflict replies below the leader's matched index panic openraft ("follower log reversion") — nodes now refuse member creation once stopping. **Process-level split-brain landed** (`tests/partition/run.sh` + the `partition_probe` example, CI job `partition`): a 3-node quorum cluster across real Linux network namespaces, iptables-partitioned into majority/minority — majority stays available (including an 8-connection × 5-publish concurrent **load sweep** driven through the partition), minority refuses (never silently accepts), and no committed message is lost on heal. Verified locally.
- [x] **Runtime-model escalation decision (§3.3): STAY on tokio work-stealing (revisit-on-evidence).** The benchmark decides, and the benchmark says the targets are met without escalation: p50 92µs / p99.9 260–430µs single-connection (2–3× below both incumbents at every percentile), p99.9/p50 ≈ 3–5× (target ≤10×), tails flat from empty to 1M-deep queues. Escalation triggers that reopen this: p99.9/p50 exceeding ~10× under multi-connection load, per-core throughput scaling flattening below ~linear, or a competitor benchmark demonstrating a tail gap attributable to scheduler jitter. The dispatch layer remains shard-partitioned (per-queue actors, per-connection tasks) so sharded-tokio/io_uring stays a cheap move.
- [~] Publish the full tail-latency/RSS comparison vs tuned incumbents: the published matrix (bench-compare/README) covers transient/quorum/clustered/deep-queue legs vs **default-config** RabbitMQ 4.3.1 and Artemis with honest caveats. **Tuned re-run + durability-parity quorum landed** (`bench-compare/tuned/`: `rabbitmq.conf` + one-command `run.sh`, all legs over loopback TCP for identical transport): transient p50 ≈ 2.8× below *tuned* RabbitMQ classic, and — closing the "in-memory vs fsync" caveat — our `store-redb` fsync quorum p50 ≈ 8× below RabbitMQ's fsync quorum. **Numbers are provisional (WSL2, indicative only)**; the defend-forever figures come from quiet bare metal via the same script. Remaining: bare-metal run, Artemis-tuned config, multi-connection load sweep. README + docs updated per phase.

---

## 12. Versioning

- `ramqp` (client): 0.7.2 → **0.8.0** after the core extraction (API preserved via
  re-exports; internal restructure warrants a minor bump).
- `ramqp-core`: **0.1.0** → 0.2.0 after Phase 2 server primitives.
- `ramqp-broker`: starts **0.1.0**.
- Bump the relevant `Cargo.toml` version before each commit (major=breaking,
  minor=features, patch=fixes), per house rules.

---

## 13. Testing, interop & conformance strategy

- `tests/broker.rs` currently exercises the **client** against an external broker;
  after Phase 3 it runs client⇄own-broker in-process (loopback over a duplex or
  localhost listener) — fast, no external deps.
- Interop both directions: reuse RabbitMQ 4.x / Artemis / Qpid as reference peers.
- **`bench-compare` is the performance backbone (§3.4):** `fe2o3-amqp` peer +
  continuous tail-latency + RSS harness vs tuned Artemis/RabbitMQ, results in-repo,
  CI regression guard. A product deliverable, not an afterthought.
- **HA correctness bar (Phase 10, Jepsen-style):** partition tolerance, split-brain,
  leader failover under load, no message loss/duplication beyond at-least-once.

---

## 14. Risks & open questions

Ordered by how much they threaten the plan. Status as of Phases 0–9 complete
+ Phase 10 core (👉 = still live, ✅ = retired by landed work).

1. 👉 **Squandering the no-GC advantage (the existential risk).** Per-message `Arc`
   churn, allocator pressure, or unbounded buffering makes us slower than a tuned
   JVM broker — and then we have no story. Mitigation: the §3 constitution + the
   continuous regression gate. *Standing:* first-numbers are 2–3× under both
   incumbents at every percentile and tails stay flat to 1M-deep, but the
   **tuned-incumbent + multi-connection** re-run (§3.4) is what actually retires
   this — still owed.
2. ✅ **Deep-queue tail latency** — **mitigated (Phase 7).** The paged/segmented
   state machine keeps p50 flat to 1M deep and RSS bounded (~3× win at 4 KiB
   bodies). Residual: snapshot-build stalls at depth (p99.9 spikes) → incremental
   snapshots is the follow-up; off-log payload replication is not yet needed. (§8)
3. ✅ **Runtime model fork** — **decided (Phase 10): stay on tokio work-stealing**,
   targets met without escalation (p99.9/p50 ≈ 3–5× vs the ≤10× bar). Reopens only
   on the documented triggers (multi-connection tail blow-up, sub-linear per-core
   scaling, a competitor tail gap traced to scheduler jitter). Dispatch stays
   shard-partitioned so escalation is cheap. (§3.3)
4. 👉 **Multi-raft scaling.** The shared-transport fabric (one connection per peer
   pair, all groups multiplexed) is in and correct at test scale; **batched
   ticks/heartbeats across thousands of groups** is not yet implemented — the
   thousands-of-quorum-queues load test is owed before this is retired. (§8)
5. ✅ **Leader-routing fabric** — **built (Phase 6).** Any node serves any queue via
   leader-following proxies; kill-leader zero-loss proven at three scopes. (§8)
6. 👉 **HA correctness / trust.** In-process fault injection (kill-leader,
   rolling-kill to the availability boundary, follower-loss) is green and flushed
   out three real bugs; **process-level partition/split-brain under sustained
   load** (Jepsen-grade) and production references remain the trust-earning work.
   (§13)
7. ✅ **Published-crate semver** — **held.** `tests/public_api.rs` locks the
   re-export surface; the one client addition this cycle (`Consumer::settle_in_txn`)
   is additive + feature-gated. Client stays 0.8.0.
8. ✅ **Persistence engine** — **resolved: `redb` 4.x** (one DB/node, group-commit
   writer shared by durable queues + the on-disk Raft log). (§9)
9. ✅ **Driver de-duplication** — standalone broker driver kept; the duplication
   never grew costly enough to lift a shared `Connection<S>` into core.

**New open items surfaced by the build (follow-ups, not blockers):**
- **Incremental snapshots** — a deep queue's snapshot still clones+serializes the
  full index (the p99.9 spikes in the depth bench).
- **Spill-segment shipping** — a paged quorum queue's snapshot keeps bodies
  external (node-local), so follower catch-up *via snapshot* is unsupported;
  log-replay catch-up works.
- **Fabric authentication** — inter-node RPCs are currently unauthenticated (bind
  the fabric to a trusted network); a fabric-level auth/mTLS story is owed before a
  hostile-network deployment.
- **Management surface** — read-only today (`/metrics`, `/queues`); queue
  delete + a richer admin protocol are the next slice.
