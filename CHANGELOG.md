# Changelog

All notable changes to ramqp will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [ramqp-broker 0.11.0] - 2026-11-11

Broker-only release: quorum-queue deletion. `ramqp` (0.8.3) and
`ramqp-core` (0.2.7) are unchanged.

### Added
- **Quorum-queue deletion** (`DELETE /queues/<vhost>/<name>?kind=quorum`),
  standalone and clustered. Each quorum queue name now has an
  *incarnation* in the metadata group, bumped by a delete; the queue's
  Raft group id — its fabric address and on-disk storage — is the name for
  incarnation 0 (so every existing queue is unchanged) and `<name>//<n>`
  after a delete. A re-declared queue therefore never shares a log, a
  snapshot, spill files, or Raft traffic with the deleted one.
  - Clustered: the delete commits through the metadata group; every node
    then stops its member of the old group, detaches the consumers and
    producers attached through it (`amqp:resource-deleted`), and purges
    the group's Raft log, snapshot, and spill/snapshot files. A node that
    was down does the same when it catches up; a straggling Raft RPC for
    a deleted group never resurrects it.
  - Standalone: the next incarnation is recorded durably *before* the old
    group is purged, so a crash mid-delete can never revive the queue; a
    startup pass finishes any interrupted purge.
  - The deleted queue's bindings are removed.
  - Gated like exchanges: refused (`503`) until every cluster node supports
    it. Metadata snapshots keep the 0.10.0 encoding until a queue is
    deleted. New fabric request `StartGroupAt` (sent only for re-declared
    queues); existing request tags are unchanged.

## [ramqp-broker 0.10.0] - 2026-11-11

`ramqp-broker` **0.10.0** + `ramqp-core` **0.2.7** (issue #37: replace
RabbitMQ 4.x on FIPS-mode Kubernetes). `ramqp` stays at 0.8.3 — no client
code changed; it picks up `ramqp-core` 0.2.7 through its `^0.2` range (the
session-window fix below). Breaking daemon/feature changes are listed under
**Changed**.

### Added
- **Server TLS** (issue #37 §4; features `tls` — rustls/ring — or `fips`): an
  `amqps` listener (`BrokerConfig::tls_listen` + `TlsServerConfig`), the
  management endpoint over TLS (`management_tls`), optional client-certificate
  verification (mTLS), and certificate **hot reload** from disk (a failed
  reload keeps the old certificate). Optional **mutual TLS on the cluster
  fabric** (`ClusterMemberConfig::tls`). `[tls]` config section and
  `--tls-*` flags.
- **FIPS build** (`fips`): rustls on AWS-LC's FIPS 140-3 provider and SCRAM
  hashing on the same module via the new `ramqp-core` feature **`scram-fips`**
  (SHA/HMAC/PBKDF2 and nonce randomness on AWS-LC; no RustCrypto, no ring).
  `ramqp_broker::fips_enabled()`; the daemon refuses to start a `fips` build
  whose module is inactive. A FIPS CI job runs both crates' suites. RFC 7677
  SCRAM-SHA-256 known-answer test added (runs on both backends).
- **Admin API** on the management listener (issue #37 §3): `PUT`/`DELETE`
  for queues, exchanges, and bindings (idempotent: `201` created, `204`
  already so, `409` different arguments), `GET /queues/<vhost>/<name>` for
  one queue's ready/unacked/consumers (KEDA `metrics-api`), and
  `GET /exchanges[/<vhost>/<name>]`. Writes need a bearer token
  (`BrokerConfig::management_token`, `management_token_file`,
  `--management-token-file`) or Basic credentials of an admin user
  (`Authenticator::management_login`); `management_auth_reads` protects
  reads too. Bounded request bodies, still no HTTP dependency.
- **Queue deletion** for transient and durable queues: consumers are
  detached with `amqp:resource-deleted`, producers on their next publish,
  and a durable queue's rows (and name) are purged. Quorum-queue deletion
  answers `501` for now.
- `SecretString` (redacted `Debug`) for secrets in `BrokerConfig`.
- **Exchange routing** (issue #37 §1) with RabbitMQ 4.x AMQP 1.0 addressing:
  producers publish to `/exchanges/<name>/<routing-key>` or `/exchanges/<name>`
  (routing key from `properties.subject`). Kinds: fanout, direct, topic
  (`*`/`#`); `amq.direct`/`amq.fanout`/`amq.topic` built in per vhost.
  Bindings route to transient, quorum, or durable queues; a queue matched by
  several bindings gets one copy. Unroutable messages are accepted and
  dropped, or routed to an optional `alternate` queue. Unsettled publishes
  are confirmed after every destination confirms (rejected if any refuses).
  Transactional publishes stage one enqueue per destination. Definitions
  persist in redb (standalone) or the metadata Raft group (clustered, synced
  to every node's routing view), and changes apply to attached producers.
  `Broker::declare_exchange` / `delete_exchange` / `bind_queue` /
  `unbind_queue`; `[[exchanges]]` and `[[bindings]]` config-file sections.
- **`ramqp-brokerd` config file** (`--config` / `RAMQP_CONFIG`, TOML; issue
  #37 §2): listeners, data dir, management listener, limits, cluster
  membership, **users** (RFC 5803 SCRAM verifiers from a file or inline, or
  a password file derived into a verifier at load — never plaintext at
  rest), **permissions** (user × vhost × address glob × send/receive,
  deny-by-default), **policies** per vhost + name prefix (every
  `QueuePolicy` field), and **pre-declared queues** (idempotent across
  restarts). Flags and env vars override the file.
- `StaticUsers` authenticator (the config-file backend): SCRAM-SHA-1/256/512
  per stored verifier, PLAIN checked against the same verifier, opt-in
  ANONYMOUS, admin flag, `Permission` grants. `auth::parse_scram_verifier` /
  `format_scram_verifier` / `derive_scram_verifier` helpers.
- `ramqp-brokerd hash-password` prints a verifier line from a password on
  stdin.
- `ramqp-brokerd --management-listen` flag (previously env-only) and
  `--allow-anonymous`; the daemon now also shuts down gracefully on
  `SIGTERM`.
- `Broker::declare_queue` and `BoundBroker::broker`.

### Client interoperability (before release)
- **Feature interop matrix** in the release gate: Qpid JMS, Qpid Proton,
  rhea, Azure go-amqp and RabbitMQ's own AMQP 1.0 Go client against a broker
  with logins, exchanges and TLS; fe2o3-amqp covers the same in-process.
- SCRAM mechanisms are offered only when every user has a verifier for them
  (strongest-first clients such as Qpid JMS could otherwise lock a user out);
  password users get SCRAM-SHA-256 and SCRAM-SHA-512 verifiers.
- TLS: the spec's negotiated `AMQP 2.1.0.0` header is accepted on the amqps
  listener and as an upgrade on the plain listener (fe2o3-amqp's default).
- RabbitMQ clients: `vhost:/` maps to the default vhost; `/management` links
  are accepted and every request gets a correlated `501` reply (declare via
  the config file or admin API) instead of a timeout.
- `ramqp-core`: a link we detach or refuse keeps its handle until the peer's
  answering detach (AMQP 1.0 §2.6), instead of reusing it at once.

### Fixed (review of the issue #37 work, before release)
- Exchange publishes whose destination queue cannot be resolved are now
  **rejected** (nothing published, nothing cached) instead of accepted while
  silently skipping that queue; a deleted exchange **detaches** its producers
  (`amqp:resource-deleted`) instead of accepting and dropping.
- Deleting a queue also removes the bindings to it, and answers everything
  queued behind the delete (publishes refused, racing subscribes detached)
  so no producer waits forever or leaks credit.
- `GET /queues/<vhost>/<name>` follows a clustered queue to its leader and
  returns `503` (never zeros) when stats are unknowable; node-local
  `/metrics` and `/queues` omit queues another node leads.
- Fabric mutual TLS has its own `[cluster.tls]` section and CA (refused if
  it equals the amqps client CA), checks that a peer certificate names a
  seed host, and management TLS never demands client certificates.
- Client CA and cluster CA bundles reload on change, like certificates.
- Startup topology from the config file is declared before the listener
  accepts; configuration conflicts stop the daemon instead of retrying
  forever.
- Exchange metadata commands are proposed only once every cluster node
  reports support (new fabric capability probe); metadata snapshots keep
  the 0.9.x encoding until exchanges are used.
- PLAIN and management Basic password checks (PBKDF2) run on the blocking
  pool, bounded to one per CPU; verifier iterations are capped at 1,000,000;
  the unknown-user decoy matches real users' parameters; failed management
  logins are slowed.
- Management requests: 3 s head deadline, TLS handshake bound, admin writes
  run to completion even if the request times out, and the connection is
  drained after the response so early `401`/`413` replies are not reset.
- Exchange definitions persist one row per exchange; an unreadable row is
  moved aside (`corrupt:`) instead of blocking every exchange.
- Fan-out confirms are awaited in the connection (no task per message);
  routing keys over 255 bytes are unroutable; multi-destination
  transactional staging is all-or-nothing; `$`-prefixed queue names are
  reserved.
- `ramqp-core`: a transfer for an already-detached link still consumes the
  session window (no next-incoming-id drift).
- TLS-only mode: an empty plain listen address (`listen = ""`,
  `--listen none`) binds only the amqps listener.
- docs.rs builds without the `fips` feature (it needs CMake and Go).

### Changed
- `ramqp-broker`'s SCRAM backend is now a feature: **`rustcrypto`** (on by
  default) or `fips`. Builds using `--no-default-features` must add
  `rustcrypto` (or `fips`); a build with neither fails with a clear error.
  Publishing needs `ramqp-core` released first (the `scram-fips` feature).
- The metadata group's state gains exchange definitions; snapshots written
  by 0.9.x still restore. **Upgrade every cluster node before declaring
  exchanges** — an older node cannot apply the new metadata command.
- **Breaking (daemon):** `ramqp-brokerd` refuses to start on a non-loopback
  listener with no users configured unless `--allow-anonymous`
  (`RAMQP_ALLOW_ANONYMOUS=1`, `allow_anonymous = true`) is given. It used to
  log a warning and run open.

## [ramqp-broker 0.9.5] - 2026-10-05

Broker-only patch release; `ramqp` (0.8.3) and `ramqp-core` (0.2.6) are
unchanged (their dependency ranges are already at the latest stable majors,
so republishing them would ship identical packages).

### Changed
- **Dependency refresh.** Regenerated `Cargo.lock` with the latest compatible
  releases (tokio 1.53.2, uuid 1.27.0, mio 1.2.4, libc 0.2.190, cc 1.6.0,
  lazy_static 1.5.1, and other patch updates). The lockfile ships inside the
  `ramqp-broker` package, so `cargo install ramqp-broker --locked` now builds
  the daemon against these versions. No manifest, code, or API changes.
- Verified live on the refreshed lockfile: full workspace suite
  (271 tests), RabbitMQ 4.x + ActiveMQ Artemis client interop (6/6 each),
  TLS + WebSocket round-trips, and the Qpid JMS, Qpid Proton, and
  fe2o3-amqp third-party interop legs against the broker.
- Dev-only: the unpublished `bench-compare` harness picks up
  `fe2o3-amqp` 0.18.2.

## [ramqp-broker 0.9.4] - 2026-09-26

Broker-only patch release; `ramqp` (0.8.3) and `ramqp-core` (0.2.6) are
unchanged (their dependency ranges are already at the latest stable majors,
so republishing them would ship identical packages).

### Changed
- **Dependency refresh.** Regenerated `Cargo.lock` with the latest compatible
  releases (redb 4.3.0, uuid 1.26.1, thiserror 2.0.21, rust_decimal 1.43.0,
  smallvec 1.16.2, clap 4.6.7, zerocopy 0.8.59, and other patch updates). The
  lockfile ships inside the `ramqp-broker` package, so
  `cargo install ramqp-broker --locked` now builds the daemon against these
  versions. No manifest, code, or API changes.
- Verified live on the refreshed lockfile: full workspace suite
  (274 tests), RabbitMQ 4.x + ActiveMQ Artemis client interop (7/7 each),
  TLS + WebSocket round-trips, and the Qpid JMS, Qpid Proton, and
  fe2o3-amqp third-party interop legs against the broker.
- Dev-only: the unpublished `bench-compare` harness moved to
  `fe2o3-amqp` 0.18 (its interop leg passes against the broker unchanged).

## [ramqp-broker 0.9.3] - 2026-08-26

Broker-only patch release; `ramqp` (0.8.3) and `ramqp-core` (0.2.6) are
unchanged (their dependency ranges are already at the latest stable majors,
so republishing them would ship identical packages).

### Changed
- **Dependency refresh.** Regenerated `Cargo.lock` with the latest compatible
  releases (redb 4.2.0, thiserror 2.0.20, uuid 1.25.0, ordered-float 5.5.0,
  futures 0.3.34, clap 4.6.6, zerocopy 0.8.56, and other patch updates). The
  lockfile ships inside the `ramqp-broker` package, so
  `cargo install ramqp-broker --locked` now builds the daemon against these
  versions. No manifest, code, or API changes.
- Verified live on the refreshed lockfile: full workspace suite
  (274 tests), RabbitMQ 4.3.1 + ActiveMQ Artemis client interop (6/6 each),
  TLS + WebSocket round-trips, and the Qpid JMS, Qpid Proton, and
  fe2o3-amqp third-party interop legs against the broker.
- Dev-only: the unpublished `bench-compare` harness moved to
  `fe2o3-amqp` 0.17 (its interop leg passes against the broker unchanged).

## [0.8.3] - 2026-07-31

Client-side dependency-range refresh: `ramqp` 0.8.3 + `ramqp-core` 0.2.6.
No code or API changes; the public API remains compile-locked by
`tests/public_api.rs`. `ramqp-broker` (0.9.2) is unchanged apart from
syncing its `ramqp-core` pin.

### Changed
- **`base64` 0.22 → 0.23** in both `ramqp` (scram feature) and `ramqp-core`
  (scram feature).
- **`tokio-tungstenite` 0.29 → 0.30** in `ramqp` (ws feature). This also
  drops the duplicate `sha1 0.10` that tungstenite 0.29 pulled alongside
  the SCRAM stack's `sha1 0.11`.
- Verified live against RabbitMQ 4.3.1: 6/6 broker interop tests and the
  WebSocket bridge round-trip on the new tungstenite.

## [ramqp-broker 0.9.2] - 2026-07-31

Broker-only patch release; `ramqp` (0.8.2) and `ramqp-core` (0.2.5) are
unchanged.

### Changed
- **Dependency refresh.** Regenerated `Cargo.lock` with the latest compatible
  releases (clap 4.6.5, clap_builder 4.6.5, displaydoc 0.2.7, hybrid-array
  0.4.14, tokio-macros 2.7.2). The lockfile ships inside the `ramqp-broker`
  package, so `cargo install ramqp-broker --locked` now builds the daemon
  against these versions. No manifest, code, or API changes.

## [ramqp-broker 0.9.1] - 2026-07-28

Broker-only patch release; `ramqp` (0.8.2) and `ramqp-core` (0.2.5) are
unchanged.

### Changed
- **Dependency refresh.** Raised the broker's minimums to `openraft` 0.9.25,
  `serde` 1.0.229, and `serde_json` 1.0.151, and regenerated `Cargo.lock`
  (tokio 1.53.1, futures 0.3.33, thiserror 2.0.19, and other patch updates).
  The lockfile ships inside the `ramqp-broker` package, so
  `cargo install ramqp-broker --locked` now builds the daemon against these
  versions. No code or API changes.

## [0.8.2] - 2026-07-17

### Added
- **`ramqp-broker` publishes to crates.io, first release 0.9.0.** The broker
  (transient + Raft-replicated quorum + redb-durable queues, clustering with
  leader routing, policies, transactions, management endpoint) ships as a
  library crate and the `ramqp-brokerd` daemon (`cargo install ramqp-broker`).
  Its config types (`BrokerConfig`, `QueuePolicy`, `OverflowBehavior`,
  `ClusterMemberConfig`) are now `#[non_exhaustive]` — construct via
  `Default`/`new` and set fields — so future knobs arrive without build
  breaks. Pre-1.0: the Rust API may still change additively across 0.x.

### Fixed
- **`ramqp-core` (0.2.5): a session now keys links by (name, role), not name
  alone.** AMQP 1.0 §2.6.1 identifies a link by container-id + name + *role*, so
  a peer may open a sender and a receiver that share a link name on one session.
  The engine previously keyed by name only, so the second same-named attach was
  misrouted and dropped — breaking Apache Qpid Proton, whose default link names
  are derived from the address (identical for both directions). Found by the new
  broker↔proton interop leg; regression-tested in `ramqp-core`. The `ramqp`
  client is unaffected in practice (it generates unique link names), but ships
  the corrected engine.

## [0.8.1] - 2026-07-13

### Security
- Replaced the unmaintained `rustls-pemfile` crate (RUSTSEC-2025-0134) with the
  PEM parsing built into `rustls-pki-types` (already present via `rustls`, and
  the crate that owns the `CertificateDer`/`PrivateKeyDer` types we return). No
  public API or behavior change; the `rustls` feature no longer pulls
  `rustls-pemfile`. (#21)

## [0.8.0] - 2026-07-13

**No `Cargo.toml` or code changes needed to upgrade**: `ramqp = "0.8"` is a
drop-in replacement for 0.7 — every public path, feature name, and type is
unchanged (compile-time-locked by `tests/public_api.rs`). `ramqp-core` arrives
as an ordinary transitive dependency. See `RELEASING.md` for the maintainer
side (publish order: `ramqp-core` first).

### Changed
- **Workspace restructure: the role-agnostic engine moved into `ramqp-core`.**
  The codec, wire types, ids, config, errors, observability, framing/header
  transport layer, session/link state machines, open-negotiation/mux/heartbeat,
  transaction wire types, and the SCRAM math now live in the new `ramqp-core`
  crate (0.2.1), shared with the new `ramqp-broker`. `ramqp` re-exports
  everything, so **all existing `ramqp::...` paths keep working**.
- The `scram` and `transaction` features now delegate to the corresponding
  `ramqp-core` features; the SCRAM crypto dependencies moved to `ramqp-core`.
- `proto::LinkEvent` variants and `proto::IncomingDelivery` now carry the
  link's local `handle` (attribution for multi-link consumers of the internal
  event vocabulary, e.g. a broker). Only affects code that pattern-matches
  these low-level internals exhaustively; the `Consumer`/`Producer` API is
  untouched.
- The `produce_consume` example accepts `RAMQP_URL` / `RAMQP_ADDRESS`
  overrides (the URL was hardcoded).

### Fixed
- **Senders stalled by session-window exhaustion now resume on a handleless
  (session-level) `flow`.** Previously only flows carrying a link handle
  re-ran the send path; a peer replenishing just its incoming-window could
  leave queued messages sitting in the outbox until an unrelated event
  flushed them. Found by driving 50k-message bursts through `ramqp-broker`;
  regression-tested at both the session and end-to-end level.

### Added (workspace)
- **`ramqp-core` 0.2.1** — the engine as its own crate, including net-new
  server polarity: `Session::accept_peer_begin`/`accept_peer_attach`,
  read-first `header::accept`, and a SASL server side (PLAIN parsing + an
  RFC 5802 `ScramServer` with verifier-based credential storage, validated
  byte-for-byte against the RFC vectors and against `ramqp`'s own client).
- **`ramqp-broker` 0.2.0 (unpublished)** — the broker: transient + quorum
  (Raft-replicated) queues, and **clustering with leader routing**: a
  multiplexed inter-node fabric (shared per-peer TCP for all Raft groups +
  the forwarded data plane), catalog-driven quorum declaration with
  rendezvous placement, leader-following proxies so any node serves any
  queue, and zero accepted-message loss across a mid-stream leader kill
  (tested e2e with the unmodified client). Daemon grows
  `--node-id/--cluster-listen/--seed`. See the README and `broker.md`.

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
  Actions CI (check / test / fmt / clippy `-D warnings` / docs) and a
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
