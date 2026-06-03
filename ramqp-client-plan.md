# `ramqp` — From-Scratch AMQP 1.0 Client: Phased Implementation Plan

> **Status:** Draft for execution. Designed for parallel multi-agent implementation.
> **Working crate name:** `ramqp` (placeholder — rename freely).
> **Scope:** A new, production-grade AMQP 1.0 client built clean-room on top of the existing
> encoding/type layer, fixing the resilience, performance, API, and observability gaps found in
> `fe2o3-amqp`.

---

## 0. How to read and execute this plan

This document is structured so work can be **fanned out to many agents at once**. The mechanism is
**contract-first development**:

1. **Phase 0 freezes the shared contracts** (error model, traits, message types, config, metrics
   hooks). Nothing else starts until these compile and are reviewed.
2. Once contracts are frozen, **each work package (WP) builds against interfaces, not against other
   agents' code.** Agents only touch the files listed in their WP.
3. Every WP has explicit **dependencies**, **owned files**, **interface to satisfy**, and
   **acceptance criteria**. An agent should be able to execute a WP with only: this doc + the frozen
   contracts + the referenced spec sections.

Each WP is tagged:

- `WP-<phase>.<n>` — stable ID, cite it in agent prompts.
- **Deps:** other WP IDs that must be merged first.
- **Parallel-safe with:** WPs that touch disjoint files and can run concurrently.
- **Owned files:** the ONLY files this WP may create/modify (prevents merge collisions).
- **Suggested agent:** rough specialization hint.

> ⚠️ **Rule for agents:** Do not modify files outside your WP's "Owned files" list. If you need a
> change to a shared contract, stop and flag it — contract changes are coordinated, not unilateral.

---

## 1. Goals & non-goals

### Goals
- **Resilience by default:** automatic reconnect, transparent session/link re-establishment,
  in-flight delivery replay, connection pooling.
- **Performance:** single-pass framing, no locks on the hot path, lazy/zero-copy receive by default,
  frame/disposition batching.
- **Ergonomics:** flat, classified error model with cause chains; stable handles; graceful lifecycle;
  config presets.
- **Observability:** pluggable metrics, a connection-state event stream, and tracing spans that don't
  require a log feature to surface health.

### Non-goals (v1)
- AMQP 1.0 **broker/acceptor** side (server). Client only.
- Full **transaction** coordinator support (stub the types; defer behind a feature flag).
- Management (`fe2o3-amqp-management`) and CBS layers — separate follow-on crates that depend on
  `ramqp`.
- Reimplementing the type/encoding layer (see Decision D-1).

---

## 2. Decision record

| ID | Decision | Default | Rationale | Reversible? |
|----|----------|---------|-----------|-------------|
| **D-1** | Encoding/type layer | **Reuse `serde_amqp`, `serde_amqp_derive`, `fe2o3-amqp-types`** as dependencies. Clean-room only the runtime/client. | These are our own fork; they are spec-heavy, tedious, and already test-covered (200+ tests). Rewriting buys nothing and adds months of risk. | Yes — swap the dep for an internal module later if true clean-room is required. |
| **D-2** | Concurrency model | **Actor model**: one owning task per connection (the "driver"), handles communicate via bounded channels. No shared mutable protocol state behind locks. | Eliminates the "RwLock-per-delivery" pattern; makes reconnect a supervisor concern; keeps the hot path lock-free. | Hard to reverse — foundational. |
| **D-3** | Receive default | **Zero-copy / lazy by default.** `recv()` yields a delivery exposing raw `Bytes`; typed deserialize is an explicit, cheap opt-in. | Inverts `fe2o3-amqp`'s eager-deserialize default; matches the perf goal. | Yes — additive typed API. |
| **D-4** | Error model | **One flat enum per public operation**, each with a `kind`, a real `source()` chain, and `is_retryable()` classification. | Replaces 39 nested enums that are hard to match on. | Yes — additive. |
| **D-5** | Runtime | **tokio only** for v1. `wasm32` support deferred to a later phase behind `cfg`. | Focus; avoid the dual-runtime tax early. | Yes — re-add the `SendBound`-style abstraction later. |
| **D-6** | MSRV / edition | **Rust 2024 edition, MSRV 1.85** (matches the just-upgraded workspace). | Consistency with the rest of the repo. | n/a |

> If you want to flip **D-1** to true clean-room, insert a "Phase E: encoding layer" before Phase 1
> and the rest of the plan is unchanged. That phase is large (~6–10 WPs) and gates everything.

---

## 3. Target architecture

### 3.1 Layering

```
┌─────────────────────────────────────────────────────────────────────┐
│  PUBLIC API  (ramqp::{Client, Connection, Session, Producer, Consumer}) │  Phase 5
│  builders + presets · graceful lifecycle · logical handles            │
├─────────────────────────────────────────────────────────────────────┤
│  RESILIENCE  supervisor · reconnect/backoff · re-attach · replay ·     │  Phase 6
│              connection pool                                          │
├─────────────────────────────────────────────────────────────────────┤
│  LINK runtime    sender/receiver state machines · settlement ·         │  Phase 4
│                  credit/flow · unsettled tracking · delivery assembly  │
├─────────────────────────────────────────────────────────────────────┤
│  SESSION runtime begin/end · windows · handle↔link map · multiplexing  │  Phase 3
├─────────────────────────────────────────────────────────────────────┤
│  CONNECTION runtime  driver task · open/close · channel mux · heartbeat │  Phase 2
├─────────────────────────────────────────────────────────────────────┤
│  TRANSPORT   TCP/TLS/WS streams · length-delimited framing ·           │  Phase 1
│              frame codec (encode/decode performatives) · SASL handshake │
├─────────────────────────────────────────────────────────────────────┤
│  CONTRACTS   error model · config · ids/newtypes · metrics trait ·     │  Phase 0
│              event types · channel message enums                       │
├─────────────────────────────────────────────────────────────────────┤
│  REUSED      serde_amqp · serde_amqp_derive · fe2o3-amqp-types  (D-1)  │  (existing)
└─────────────────────────────────────────────────────────────────────┘

OBSERVABILITY (Phase 7) is cross-cutting: emission points are wired in at every layer against the
Phase-0 metrics trait + event channel, so it does not block other layers.
```

### 3.2 The actor model (D-2) — why it kills the hot-path locks

```
  socket
    │  (raw bytes)
    ▼
┌──────────────────────────────────────────────────────────────┐
│  CONNECTION DRIVER TASK  (owns the transport, single thread)   │
│  - reads frames, routes by channel → session inboxes           │
│  - drains session outboxes → writes frames (batched)           │
│  - owns heartbeat timer                                        │
│  - owns ALL protocol state (no locks: single owner)            │
└──────────────────────────────────────────────────────────────┘
        ▲ outbox (mpsc)            │ inbox per session (mpsc)
        │                          ▼
   Producer/Consumer  ◄──────  Session state lives inside the driver
   handles (cheap clones)      User handles are thin: send command, await oneshot reply
```

Protocol mutable state (unsettled maps, windows, credit) lives **inside the single driver task** and
is mutated without locks because there is exactly one owner. User-facing `Producer`/`Consumer`
handles are cheap `Clone` structs that send command messages and await `oneshot` replies. This is the
structural fix for `fe2o3-amqp`'s `RwLock`-per-delivery and lock-held-across-deserialize patterns.

### 3.3 Logical vs physical handles (resilience foundation)

- **Physical link/session** = the AMQP attach/begin bound to one TCP connection. Dies on reconnect.
- **Logical handle** (`Producer`/`Consumer`) = what the user holds. Stable across reconnects. On
  reconnect the supervisor rebuilds the physical link, replays unsettled deliveries, and the user's
  handle keeps working (or surfaces a typed `Reconnecting` state via the event stream).

---

## 4. Workspace & module layout

```
rAMQP/
├── serde_amqp/            (reused, unchanged)
├── serde_amqp_derive/     (reused, unchanged)
├── fe2o3-amqp-types/      (reused, unchanged)
└── ramqp/                 (NEW crate)
    ├── Cargo.toml
    ├── src/
    │   ├── lib.rs
    │   ├── error.rs            ── WP-0.1  flat error model
    │   ├── ids.rs              ── WP-0.2  newtypes (ChannelId, Handle, DeliveryId, ...)
    │   ├── config/             ── WP-0.3  config structs + presets
    │   ├── observe/            ── WP-0.4  Metrics trait + Event types + NoopMetrics
    │   ├── proto/              ── WP-0.5  internal message/command enums (channel payloads)
    │   ├── transport/
    │   │   ├── mod.rs          ── WP-1.1  Stream trait + TCP
    │   │   ├── tls.rs          ── WP-1.2  rustls/native-tls
    │   │   ├── ws.rs           ── WP-1.5  websocket (later)
    │   │   ├── codec.rs        ── WP-1.3  single-pass frame encode/decode
    │   │   └── header.rs       ── WP-1.3  protocol header negotiation
    │   ├── sasl/               ── WP-1.4  SASL: ANONYMOUS/PLAIN/EXTERNAL/SCRAM
    │   ├── connection/
    │   │   ├── driver.rs       ── WP-2.1  driver task / event loop
    │   │   ├── negotiate.rs    ── WP-2.2  open/close + capability negotiation
    │   │   ├── mux.rs          ── WP-2.3  channel↔session routing
    │   │   └── heartbeat.rs    ── WP-2.4  idle-timeout / keepalive
    │   ├── session/
    │   │   ├── state.rs        ── WP-3.1  begin/end state machine
    │   │   ├── window.rs       ── WP-3.2  incoming/outgoing window flow control
    │   │   └── registry.rs     ── WP-3.3  handle↔link registry
    │   ├── link/
    │   │   ├── sender.rs       ── WP-4.1  sender link state machine
    │   │   ├── receiver.rs     ── WP-4.2  receiver link state machine
    │   │   ├── settlement.rs   ── WP-4.3  unsettled tracking (lock-free, owner-local)
    │   │   ├── credit.rs       ── WP-4.4  credit/flow control
    │   │   ├── delivery.rs     ── WP-4.5  multi-frame delivery assembly + lazy body
    │   │   └── resume.rs       ── WP-6.2  unsettled replay / link recovery
    │   ├── api/
    │   │   ├── client.rs       ── WP-5.1  top-level Client + builder/presets
    │   │   ├── connection.rs   ── WP-5.2  Connection handle
    │   │   ├── session.rs      ── WP-5.3  Session handle
    │   │   ├── producer.rs     ── WP-5.4  Producer (sender) handle + send/batch
    │   │   ├── consumer.rs     ── WP-5.5  Consumer (receiver) handle + recv/settle
    │   │   └── lifecycle.rs    ── WP-5.6  graceful drop/close
    │   ├── resilience/
    │   │   ├── supervisor.rs   ── WP-6.1  reconnect + backoff + re-establish
    │   │   └── pool.rs         ── WP-6.3  connection pool
    │   └── observe/wiring.rs   ── WP-7.x  metric/event emission points
    ├── tests/                  ── integration (testcontainers)
    └── benches/                ── criterion
```

---

## 5. Phase 0 — Contracts (THE GATE)

> **Everything depends on Phase 0. It must be merged and reviewed before any Phase ≥1 agent starts.**
> Keep these files small, dependency-light, and stable. Treat changes after freeze as breaking.

### WP-0.0 — Crate skeleton
- **Deps:** none. **Suggested agent:** generalist.
- **Owned files:** `ramqp/Cargo.toml`, `ramqp/src/lib.rs`, root `Cargo.toml` (add member).
- **Do:** create the `ramqp` crate, add to workspace members, wire deps (`serde_amqp`,
  `fe2o3-amqp-types`, `tokio`, `bytes`, `futures-util`, `tracing`, `thiserror`, `pin-project-lite`).
  Edition 2024, MSRV 1.85. Empty module tree with `pub mod` stubs.
- **Accept:** `cargo check -p ramqp` passes with empty modules.

### WP-0.1 — Flat error model (D-4)
- **Deps:** WP-0.0. **Suggested agent:** API-design-minded.
- **Owned files:** `ramqp/src/error.rs`.
- **Do:** Define one public error enum per *operation surface* (`ConnectError`, `SendError`,
  `RecvError`, `SessionError`, `LinkError`) plus a shared `ErrorKind`. Each:
  - implements `std::error::Error` with a working `source()` chain,
  - carries `kind()`, `is_retryable() -> bool`, `is_fatal() -> bool`,
  - wraps a `definitions::Error` (from `fe2o3-amqp-types`) where the peer sent one, exposed via a
    typed accessor — never opaque.
- **Interface sketch:**
  ```rust
  pub enum ErrorKind { Io, Tls, Sasl, ProtocolViolation, PeerClosed, Timeout,
                       Detached, LinkRedirect, Capacity, Settlement, Cancelled }
  pub struct SendError { kind: ErrorKind, source: Option<BoxError>, remote: Option<RemoteError> }
  impl SendError { pub fn kind(&self) -> ErrorKind; pub fn is_retryable(&self) -> bool; ... }
  ```
- **Accept:** unit tests proving `source()` chains and `is_retryable()` classification for each kind.

### WP-0.2 — Identifier newtypes
- **Deps:** WP-0.0. **Parallel-safe with:** all other Phase-0 WPs.
- **Owned files:** `ramqp/src/ids.rs`.
- **Do:** `ContainerId`, `ChannelId(u16)`, `Handle(u32)`, `DeliveryId(u32)`, `DeliveryTag(Bytes)`,
  `LinkName`, `SessionId`. Type-safe conversions; no `usize`/`u32` confusion (a `fe2o3-amqp` pain
  point). Derive `Debug/Clone/PartialEq/Eq/Hash` as appropriate.
- **Accept:** `cargo check`; doc examples compile.

### WP-0.3 — Config + presets
- **Deps:** WP-0.2. **Parallel-safe with:** WP-0.1, WP-0.4, WP-0.5.
- **Owned files:** `ramqp/src/config/*`.
- **Do:** `ConnectionConfig`, `SessionConfig`, `LinkConfig`, `ReconnectConfig`. Provide
  `Config::low_latency()` and `Config::high_throughput()` presets. Typed windows (newtypes from
  WP-0.2), `max_frame_size`, `idle_timeout`, credit mode, buffer bounds, backoff params.
- **Accept:** presets documented; values justified in comments; `cargo check`.

### WP-0.4 — Observability contracts
- **Deps:** WP-0.2. **Parallel-safe with:** WP-0.1, WP-0.3, WP-0.5.
- **Owned files:** `ramqp/src/observe/mod.rs`, `ramqp/src/observe/metrics.rs`,
  `ramqp/src/observe/event.rs`.
- **Do:**
  - `trait Metrics: Send + Sync` with counter/gauge/histogram methods for the agreed metric set
    (frames in/out, bytes, deliveries, settlements, credit, reconnects, inflight). Provide
    `NoopMetrics` default.
  - `enum ConnectionEvent { Connected, Reconnecting{attempt}, Degraded, Closed{reason} }` and a
    subscription type (`tokio::sync::broadcast` or watch).
  - These are **the only** observability dependencies other WPs import; emission points (Phase 7)
    call into them.
- **Accept:** `NoopMetrics` compiles; an example custom `Metrics` impl in a doc test.

### WP-0.5 — Internal protocol message/command enums
- **Deps:** WP-0.1, WP-0.2. **Suggested agent:** protocol-savvy. **Critical — most-shared file.**
- **Owned files:** `ramqp/src/proto/*`.
- **Do:** Define the channel payloads that flow between layers (the actor "alphabet"):
  - `DriverCmd` (user → driver): `BeginSession`, `EndSession`, `AttachLink`, `DetachLink`,
    `SendTransfer`, `Disposition`, `Flow`, `CloseConnection`.
  - `SessionInbound` / `LinkInbound`: decoded performatives + `Payload(Bytes)` routed inward.
  - `Reply` oneshot payload types for each command.
  - Re-export the performative types from `fe2o3-amqp-types` used in these enums (don't redefine).
- **Accept:** enums compile; a `proto/README` (doc comment) maps each variant to its AMQP performative.

**Phase 0 exit criteria (review gate):** all of `error/ids/config/observe/proto` compile together,
are documented, and a maintainer has signed off on the contract shapes. **Tag the commit
`contracts-v1`.** Phase ≥1 agents branch from there.

---

## 6. Phase 1 — Transport & codec

> Can start the moment `contracts-v1` is tagged. Strongly parallelizable.

### WP-1.1 — Stream abstraction + TCP
- **Deps:** Phase 0. **Parallel-safe with:** WP-1.4.
- **Owned files:** `ramqp/src/transport/mod.rs`.
- **Do:** `trait IoStream: AsyncRead + AsyncWrite + Unpin + Send`; TCP impl; connect-by-URL parsing
  (`amqp://`, `amqps://`). Bounded read/write buffers.
- **Accept:** unit test connecting to a local TCP echo; URL parse tests.

### WP-1.2 — TLS
- **Deps:** WP-1.1. **Parallel-safe with:** WP-1.3, WP-1.4.
- **Owned files:** `ramqp/src/transport/tls.rs`.
- **Do:** `rustls` (default) + optional `native-tls`, feature-gated. `amqps://` wiring,
  webpki-roots default trust.
- **Accept:** TLS handshake test against a known endpoint in CI (or a local self-signed fixture).

### WP-1.3 — Single-pass frame codec + header (PERF-critical)
- **Deps:** Phase 0, WP-1.1. **Suggested agent:** perf-minded + protocol-savvy.
- **Owned files:** `ramqp/src/transport/codec.rs`, `ramqp/src/transport/header.rs`.
- **Do:**
  - Length-prefixed AMQP frame read/write. Decode: read length, deserialize performative via
    `serde_amqp`, expose remaining body as **`Bytes` via `split()`** (zero-copy).
  - **Single-pass encode (fixes `fe2o3-amqp` frames/amqp.rs:80-153):** compute the transfer/body
    split from the negotiated `max_frame_size` *once*; never re-serialize the performative to probe
    size. Document the algorithm.
  - Protocol header (`AMQP\x00\x01\x00\x00`) + SASL header negotiation handshake.
  - Encoder writes into a reusable `BytesMut` with a capacity hint from negotiated frame size (no
    unbounded `Vec::new()` per frame).
- **Accept:** round-trip encode/decode tests for every frame type; a test asserting a multi-frame
  message is encoded with exactly one performative serialization per frame (guard against regression);
  fuzz/property test for decode robustness on truncated input.

### WP-1.4 — SASL handshake
- **Deps:** Phase 0, WP-1.1. **Parallel-safe with:** WP-1.2, WP-1.3.
- **Owned files:** `ramqp/src/sasl/*`.
- **Do:** SASL state machine: mechanisms negotiation, `init/challenge/response/outcome`. Mechanisms:
  ANONYMOUS, PLAIN, EXTERNAL, and SCRAM-SHA-1/256/512 (reuse the crypto approach from
  `fe2o3-amqp/src/auth/scram` — same crates: `hmac`, `sha1`, `sha2`, `pbkdf2`, `stringprep`).
- **Accept:** PLAIN + ANONYMOUS unit tests; SCRAM client vectors (port the existing passing test
  vectors from `fe2o3-amqp/src/auth/scram/mod.rs` tests).

### WP-1.5 — WebSocket transport *(deferrable to post-v1)*
- **Deps:** WP-1.1. **Owned files:** `ramqp/src/transport/ws.rs`.
- **Do:** `tokio-tungstenite` 0.29 binding; map ws messages ↔ AMQP byte stream. Learn from
  `fe2o3-amqp-ws`'s error mapping.
- **Accept:** connects to an AMQP-over-WS endpoint; binary framing test.

---

## 7. Phase 2 — Connection runtime

> The driver task is the heart of D-2. WP-2.1 is on the critical path; 2.2–2.4 layer onto it.

### WP-2.1 — Driver task / event loop
- **Deps:** Phase 1 (WP-1.1, WP-1.3). **Suggested agent:** senior/concurrency.
- **Owned files:** `ramqp/src/connection/driver.rs`.
- **Do:** the single owning task: `tokio::select!` over {inbound frames, user command channel,
  heartbeat tick, shutdown}. Owns transport, routes inbound frames by channel to session inboxes,
  drains outbound. **No locks** — all state owner-local. Batches outbound frames before flush (PERF).
- **Accept:** a loopback test (driver talks to a mock peer over a duplex pipe) exchanging open/close;
  shutdown is clean (no leaked task).

### WP-2.2 — Open/close negotiation
- **Deps:** WP-2.1. **Parallel-safe with:** WP-2.3, WP-2.4.
- **Owned files:** `ramqp/src/connection/negotiate.rs`.
- **Do:** send/await `Open`; capability + `max_frame_size` + `channel_max` + `idle_timeout`
  negotiation (use min-of-both rules); graceful `Close` with error propagation; map peer `Close{error}`
  into the WP-0.1 error model.
- **Accept:** negotiation unit tests incl. mismatched frame sizes; peer-error propagation test.

### WP-2.3 — Channel multiplexing / session routing
- **Deps:** WP-2.1. **Parallel-safe with:** WP-2.2, WP-2.4.
- **Owned files:** `ramqp/src/connection/mux.rs`.
- **Do:** allocate outgoing channels, map incoming channel→session, enforce `channel_max`, slab-style
  allocation. Clean teardown on session end.
- **Accept:** alloc/free tests; channel-exhaustion returns typed `Capacity` error.

### WP-2.4 — Heartbeat
- **Deps:** WP-2.1. **Parallel-safe with:** WP-2.2, WP-2.3.
- **Owned files:** `ramqp/src/connection/heartbeat.rs`.
- **Do:** automatic empty-frame keepalive on negotiated interval; detect peer idle-timeout violation →
  typed `Timeout` error that the supervisor (Phase 6) treats as reconnectable.
- **Accept:** timer fires empty frames; simulated peer silence triggers timeout.

---

## 8. Phase 3 — Session runtime

### WP-3.1 — Session state machine
- **Deps:** WP-2.1, WP-2.3. **Owned files:** `ramqp/src/session/state.rs`.
- **Do:** begin/end state machine inside the driver; lifecycle from `BEGIN` to `END`; map peer
  `End{error}`.
- **Accept:** begin→end happy path over mock peer; double-end / illegal-state tests.

### WP-3.2 — Flow-control windows
- **Deps:** WP-3.1. **Parallel-safe with:** WP-3.3. **Suggested agent:** protocol-savvy.
- **Owned files:** `ramqp/src/session/window.rs`.
- **Do:** incoming/outgoing window tracking, `next-outgoing-id`, remote-incoming-window respect,
  `Flow` emission/handling at the session level.
- **Accept:** window-accounting unit tests against AMQP examples; back-pressure when window is zero.

### WP-3.3 — Handle↔link registry
- **Deps:** WP-3.1. **Parallel-safe with:** WP-3.2.
- **Owned files:** `ramqp/src/session/registry.rs`.
- **Do:** map link `Handle`→link state; `handle_max` enforcement; dispatch inbound link performatives.
- **Accept:** attach/detach registry tests; handle-exhaustion typed error.

---

## 9. Phase 4 — Link runtime

> The richest phase. Sender and receiver state machines can be built by separate agents in parallel
> once settlement/credit/delivery primitives (4.3–4.5) exist. Consider building 4.3–4.5 first, then
> 4.1 and 4.2 concurrently.

### WP-4.3 — Settlement / unsettled tracking (PERF + correctness core)
- **Deps:** WP-3.1. **Suggested agent:** senior. **Build before 4.1/4.2.**
- **Owned files:** `ramqp/src/link/settlement.rs`.
- **Do:** owner-local (no-lock, D-2) unsettled map; settlement modes (settled/unsettled, first/second);
  delivery-state transitions. Designed so it can be **snapshotted for replay** (Phase 6).
- **Accept:** state-transition table tests covering the AMQP settlement matrix; snapshot/restore test.

### WP-4.4 — Credit / flow control (link level)
- **Deps:** WP-3.2. **Parallel-safe with:** WP-4.3, WP-4.5.
- **Owned files:** `ramqp/src/link/credit.rs`.
- **Do:** link credit, drain, auto vs manual credit modes exposed via config (WP-0.3); credit as a
  first-class, runtime-tunable value (fix `fe2o3-amqp`'s awkward attach-time-only credit).
- **Accept:** credit accounting tests; drain semantics; auto-refill threshold test.

### WP-4.5 — Delivery assembly + lazy body (PERF, D-3)
- **Deps:** WP-1.3. **Parallel-safe with:** WP-4.3, WP-4.4.
- **Owned files:** `ramqp/src/link/delivery.rs`.
- **Do:** assemble multi-frame transfers into one delivery holding **`Bytes`** (zero-copy). Expose a
  `Delivery` that yields raw bytes by default and a `.decode::<T>()` opt-in (uses `serde_amqp`).
  Outbound: accept pre-encoded `Bytes` or `impl Serialize`.
- **Accept:** multi-frame reassembly tests; a benchmark comparing lazy vs eager receive.

### WP-4.1 — Sender link state machine
- **Deps:** WP-4.3, WP-4.4, WP-4.5. **Parallel-safe with:** WP-4.2.
- **Owned files:** `ramqp/src/link/sender.rs`.
- **Do:** attach/detach; `Transfer` emission honoring credit + window; settlement of outgoing
  deliveries; map peer disposition → outcome.
- **Accept:** attach→send→settle over mock peer; credit-exhaustion back-pressure; detach handling.

### WP-4.2 — Receiver link state machine
- **Deps:** WP-4.3, WP-4.4, WP-4.5. **Parallel-safe with:** WP-4.1.
- **Owned files:** `ramqp/src/link/receiver.rs`.
- **Do:** attach/detach; credit issuance; inbound transfer → delivery; disposition emission
  (accept/reject/release/modify); settlement.
- **Accept:** attach→recv→accept over mock peer; partial-credit + drain tests.

---

## 10. Phase 5 — Public API

> Layered on a working runtime. Handle WPs are parallel once the client/builder (5.1) lands.

### WP-5.1 — Client + builder + presets
- **Deps:** Phase 4. **Owned files:** `ramqp/src/api/client.rs`.
- **Do:** top-level `Client`/`Connection::open` entry, builder using presets (WP-0.3), spawns the
  driver task, returns a `Connection` handle.
- **Accept:** end-to-end open against a broker (testcontainers) in an integration test.

### WP-5.2 / 5.3 — Connection & Session handles
- **Deps:** WP-5.1. **Parallel-safe with:** each other.
- **Owned files:** `ramqp/src/api/connection.rs`, `ramqp/src/api/session.rs`.
- **Do:** thin command-sending handles; `begin_session`, etc. Stable, `Clone`-able, `Send`-able,
  storable in structs.
- **Accept:** doc-tested usage; handles survive being moved into a spawned task.

### WP-5.4 — Producer handle (send + batch)
- **Deps:** WP-5.3, WP-4.1. **Parallel-safe with:** WP-5.5.
- **Owned files:** `ramqp/src/api/producer.rs`.
- **Do:** `send`, `send_batch` (PERF: coalesced transfers), settlement-awaiting `send` vs
  fire-and-forget; outcome surfacing via the flat error model.
- **Accept:** send one / send many; batch path verified to coalesce; integration test against broker.

### WP-5.5 — Consumer handle (recv + settle)
- **Deps:** WP-5.3, WP-4.2. **Parallel-safe with:** WP-5.4.
- **Owned files:** `ramqp/src/api/consumer.rs`.
- **Do:** `recv()` → lazy `Delivery` (D-3); `accept/reject/release/modify`; a `Stream` adapter;
  batched dispositions (PERF).
- **Accept:** recv + each settlement outcome; `Stream` impl tested; disposition-batching test.

### WP-5.6 — Graceful lifecycle
- **Deps:** WP-5.2–5.5. **Owned files:** `ramqp/src/api/lifecycle.rs`.
- **Do:** ergonomic `close()` (awaits clean shutdown) **and** a `Drop` that initiates graceful detach
  without blocking, draining pending settlements via the driver (fixes `fe2o3-amqp`'s
  drop-doesn't-await wart).
- **Accept:** drop-without-close still detaches cleanly (observed on mock peer); explicit close awaits.

---

## 11. Phase 6 — Resilience

> The headline feature set. Depends on a working Phase 5 and the snapshot-able settlement (WP-4.3).

### WP-6.1 — Supervisor: reconnect + backoff + re-establish
- **Deps:** Phase 5. **Suggested agent:** senior/concurrency.
- **Owned files:** `ramqp/src/resilience/supervisor.rs`.
- **Do:** wrap the driver in a supervisor that, on reconnectable failure (per `is_retryable`),
  reconnects with jittered exponential backoff, re-opens, re-begins sessions, re-attaches links, and
  emits `ConnectionEvent` transitions (WP-0.4). Bounded outbound buffering during the gap with
  back-pressure.
- **Accept:** kill the mock peer mid-stream → client transparently reconnects and resumes; event
  stream shows `Reconnecting`→`Connected`; outbound buffer bounds enforced.

### WP-6.2 — Link recovery / unsettled replay
- **Deps:** WP-6.1, WP-4.3. **Owned files:** `ramqp/src/link/resume.rs`.
- **Do:** on re-attach, exchange unsettled maps and replay/resolve in-flight deliveries per the AMQP
  resume rules (the logic `fe2o3-amqp` has in `link/resumption.rs` but only runs manually — here it
  runs automatically under the supervisor). Resend/resume/restate/abort decision matrix.
- **Accept:** a delivery in-flight at disconnect is correctly resolved after reconnect (no loss, no
  dup beyond at-least-once contract); covers the resume decision matrix with table tests.

### WP-6.3 — Connection pool
- **Deps:** WP-6.1. **Owned files:** `ramqp/src/resilience/pool.rs`.
- **Do:** a pool of supervised connections with health-aware checkout; session multiplexing as a
  first-class API; configurable size + acquisition timeout.
- **Accept:** pool hands out healthy connections; a dead connection is replaced transparently;
  acquisition timeout returns a typed error.

---

## 12. Phase 7 — Observability wiring (cross-cutting)

> Can begin as soon as the relevant layer exists; not a blocker for anything. Assign as a dedicated
> track once Phase 2 lands, extending per layer.

### WP-7.1 — Metric emission points
- **Deps:** WP-0.4 + the layer being instrumented. **Owned files:** `ramqp/src/observe/wiring.rs`
  (+ minimal, reviewed call-site additions — coordinate, since these touch other WPs' files).
- **Do:** call the `Metrics` trait at agreed points: frames/bytes in/out, deliveries, settlements,
  credit gauges, inflight gauges, reconnect counters, latency histograms for send-to-settle.
- **Accept:** a test `Metrics` impl observes expected counter movements through a full send/recv cycle.

### WP-7.2 — Event stream + health
- **Deps:** WP-0.4, WP-6.1. **Owned files:** within `observe/` + supervisor hooks.
- **Do:** publish `ConnectionEvent`s; expose a `subscribe()`; surface health without requiring the
  tracing feature.
- **Accept:** subscriber receives the full lifecycle event sequence in an integration test.

### WP-7.3 — Tracing spans
- **Deps:** layers exist. **Do:** `#[instrument]` spans on connect/attach/send/recv with structured
  fields; ensure spans carry ids (WP-0.2). **Accept:** spans present and structured under a test
  subscriber.

---

## 13. Phase 8 — Verification, conformance, hardening

### WP-8.1 — Broker integration suite
- **Deps:** Phase 5. **Owned files:** `ramqp/tests/*`.
- **Do:** testcontainers (0.27 API — see `fe2o3-amqp/tests/common.rs` for the current pattern) against
  ActiveMQ Artemis + RabbitMQ AMQP 1.0; full send/recv/settle/detach/reconnect matrix.
- **Accept:** suite green in CI (gated to run where Docker is available).

### WP-8.2 — Benchmarks
- **Deps:** Phase 4/5. **Owned files:** `ramqp/benches/*`.
- **Do:** criterion benches: throughput (msgs/s), latency (send-to-settle), lazy-vs-eager receive,
  single-pass-vs-multipass framing. Compare against `fe2o3-amqp` as a baseline.
- **Accept:** benches run; a short results note vs baseline committed.

### WP-8.3 — Fault injection
- **Deps:** Phase 6. **Do:** a mock peer that can drop/delay/corrupt; tests for reconnect, replay,
  timeout, partial frames. **Accept:** resilience holds under each injected fault.

### WP-8.4 — Docs, examples, README
- **Deps:** Phase 5. **Do:** crate docs, runnable examples (mirror the `examples/` style in the repo),
  README with the quick-start. **Accept:** `cargo test --doc` green; examples compile.

---

## 14. Agent dispatch guide (parallel waves)

> Each wave starts only after the previous wave's gating WPs merge. Within a wave, all listed WPs are
> file-disjoint and run concurrently.

| Wave | WPs (parallel) | Gate to start | Approx. concurrency |
|------|----------------|---------------|----------------------|
| **W0** | WP-0.0 → then **0.1, 0.2, 0.3, 0.4, 0.5** | none → 0.0 merged | 1, then up to 5 |
| **W1** | 1.1, 1.4 ‖ then 1.2, 1.3 | `contracts-v1` tag | up to 4 |
| **W2** | 2.1 → then 2.2, 2.3, 2.4 | Phase 1 merged | 1, then 3 |
| **W3** | 3.1 → then 3.2, 3.3 | WP-2.1+2.3 | 1, then 2 |
| **W4** | 4.3, 4.4, 4.5 → then 4.1, 4.2 | Phase 3 | 3, then 2 |
| **W5** | 5.1 → then 5.2, 5.3 → 5.4, 5.5 → 5.6 | Phase 4 | up to 2–3 |
| **W6** | 6.1 → 6.2 → 6.3 | Phase 5 | mostly serial |
| **W7** | 7.1, 7.2, 7.3 | runs alongside W2+ | dedicated track |
| **W8** | 8.1, 8.2, 8.3, 8.4 | Phase 5 (8.3 after 6) | up to 4 |

**Critical path:** WP-0.0 → 0.5 → 1.3 → 2.1 → 3.1 → 4.3 → 4.1/4.2 → 5.1 → 6.1 → 6.2. Optimize agent
attention here; everything else is slack that fills the parallel waves.

### Standing rules for every agent
1. Branch from the latest merged tag; never edit outside your WP's **Owned files**.
2. Satisfy the **interface** from Phase 0 exactly; if it's wrong, raise a contract-change flag — don't
   fork the contract.
3. Land with tests meeting the **Accept** criteria; `cargo check` + `cargo test -p ramqp` clean; no new
   warnings (repo rule).
4. Bump `ramqp` crate version per the repo's versioning rule on each merge.
5. Mock the peer with a duplex pipe for unit tests; reserve broker (testcontainers) for integration.

---

## 15. Definition of done (v1)

- Connect (TCP+TLS, SASL PLAIN/ANONYMOUS/EXTERNAL/SCRAM) → begin session → attach producer/consumer →
  send/recv/settle → graceful close, against both Artemis and RabbitMQ.
- Survive a mid-stream connection drop with **automatic reconnect + transparent re-attach +
  in-flight replay** (no message loss within the at-least-once contract).
- **Zero locks on the per-message path**; single-pass framing; lazy receive default; batched
  send/disposition paths — all benchmarked against the `fe2o3-amqp` baseline.
- Flat error model with `source()` chains and retry classification across all public ops.
- Pluggable metrics + connection-event subscription, usable without the tracing feature.
- Integration suite, benches, fault-injection suite, and docs/examples all green in CI.

---

## 16. Risk register

| Risk | Impact | Mitigation |
|------|--------|------------|
| Contract churn after Phase 0 | Stalls every parallel agent | Heavy review at the Phase-0 gate; treat changes as breaking + coordinated. |
| AMQP settlement/resume subtlety | Correctness bugs in replay | Port `fe2o3-amqp`'s resume decision matrix as the reference; table-test every case (WP-6.2). |
| Actor model back-pressure design | Deadlock / unbounded memory | Bounded channels everywhere; explicit back-pressure tests (WP-3.2, WP-6.1). |
| Single-pass framing edge cases | Wire incompatibility | Round-trip + truncation fuzz tests (WP-1.3); interop tests against two brokers. |
| Reusing vs clean-room (D-1) revisited late | Large rework | Keep encoding behind a thin internal boundary so it can be swapped without touching runtime. |
| Broker behavioral differences | Flaky integration | Run the full matrix against both brokers in CI from Phase 5 onward. |
```
