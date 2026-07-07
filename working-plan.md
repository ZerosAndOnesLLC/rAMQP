# Issue #19 remediation — working plan

Fixing **all 49 findings** from https://github.com/ZerosAndOnesLLC/rAMQP/issues/19 on
`feat/phase6-forwarding-fabric` (PR #18). Decisions made by user 2026-07-07:

- Scope: **everything** (4 Critical, 10 High, 18 Medium, 17 Low)
- CRIT-2: **warn + document** (startup warning for non-loopback fabric bind + docs; auth/TLS deferred)
- Landing: this branch, **one commit per finding** (or per natural group of same-code findings),
  commit message references the ID(s)

Process per finding: re-verify against code → fix → test → `cargo check` (no warnings) →
bump `ramqp-broker/Cargo.toml` patch version → commit.

## Phase A — silent data-loss / correctness (triage group 1)

- [x] CRIT-1 — partial txn commit breaks atomicity — fixed: two-phase commit (Reserve/Unreserve/PublishReserved across all 4 actors + fabric), detached discharge task, honest `Partial` outcome (0.8.2)
- [x] CRIT-3 — meta group purges log but never persists snapshot — fixed: SnapshotPersist::{Inline,File} sink API; inline blobs stored in redb atomic with pointer; regression test (0.8.3)
- [x] CRIT-4 — snapshot blobs + spill segments never fsync'd — fixed: write_blob_durably (file+dir fsync), Spill::sync_all before pointer persists (0.8.4)
- [x] HIGH-8 — install_snapshot delete-before-persist — fixed: old blob deleted only after new pointer durable; failed blob write degrades to inline (0.8.4)
- [x] HIGH-9 — recover_raft silent degrade — fixed: dangling pointer/missing blob now refuses to start (0.8.4)
- [x] HIGH-1 — install_snapshot loses paging — fixed: in-place restore into existing state + persistent spill identity rejects foreign External refs loudly (0.8.5)
- [x] HIGH-2 — proxy rebind sub leak — fixed: teardown_downstream before rebind + try_bind partial-failure cleanup; regression test (0.8.6)
- [x] HIGH-3 — TxnDone misroute — fixed: TxnDone carries SessionId, outcome dropped unless the session at the channel is still the same (0.8.7)
- [x] HIGH-5 — dropped txn settle — fixed: refused settles requeue immediately; cap refusal marks txn rollback-only → discharge Rejected; unit + integration tests (0.8.8)
- [x] MED-12 — volatile durable DLX — fixed: DeadLetter.confirm; source Remove ordered after copy's fate; durable→durable DLX test (0.8.9)

Phase A COMPLETE (2026-07-07): CRIT-1/3/4, HIGH-1/2/3/5/8/9, MED-12, LOW-10 — versions 0.8.2→0.8.9.

## Phase B — isolation / DoS (fix or warn+document per decisions)

- [x] CRIT-2 — fabric port: warned (bootstrap non-loopback warning) + documented (ClusterMemberConfig Security section, brokerd docs) per user decision (0.8.10)
- [x] HIGH-4 — staged txn bytes — fixed: MAX_STAGED_BYTES 64MiB/connection, refusal poisons txn, discharge frees budget (0.8.10)
- [x] HIGH-6 — vhost/queue key collision — fixed: vhost validated at open, client names reject '/'+control chars, internal DLX path keeps qualified names; unit+integration tests (0.8.11)
- [x] HIGH-7 — clustered in-memory durability — fixed: loud bootstrap warning + data_dir docs (kept for tests/ephemeral) (0.8.12)
- [x] HIGH-10 — default authz — fixed: with_user_vhosts grants on StaticPlain/StaticScram, sharp-edge trait docs; unit+client tests (0.8.12)
- [x] MED-6 — byte bounds — fixed: max_queue_bytes (1GiB default) + max_length_bytes across transient/durable/quorum actors (0.8.13)
- [x] MED-17 — mgmt DoS — fixed: 10s request deadline + 64-conn semaphore (0.8.14)
- [x] MED-18 — metric injection — fixed: proper Prometheus/JSON escaping incl. control chars (0.8.14)

Phase B COMPLETE (2026-07-07): CRIT-2 (warn+doc), HIGH-4/6/7/10, MED-6/17/18 — versions 0.8.10→0.8.14.

## Phase C — remaining Mediums  ✅ COMPLETE (0.8.15→0.8.20)

- [x] MED-1 — quorum stale delivery-limit — fixed: leader-local exact failure map (0.8.15)
- [x] MED-2 — registry init-cell race — fixed: orphaned-cell guard under map lock (0.8.16)
- [x] MED-3 — DLX self-cycle — fixed: self-target dead-lettering disabled + warned (0.8.16)
- [x] MED-4 — clustered DLX locality — fixed: warn on node-local target + policy docs (0.8.17)
- [x] MED-5 — TTL wall clock — documented on message_ttl (0.8.17)
- [x] MED-7 — missed demotion — fixed: term-change exit (0.8.18)
- [x] MED-8 — stop/heal TOCTOU — fixed: re-check stopping under groups lock (0.8.19)
- [x] MED-9 — dead consumer in-flights — fixed: requeue all across quorum/transient/durable (0.8.18)
- [x] MED-10 — no dial timeout — fixed: 5s timeout, dial outside mutex (0.8.19)
- [x] MED-11 — partial ready seed — fixed: exit for respawn on replay timeout (0.8.18)
- [x] MED-13 — coordinator stall — fixed: zero-wait drain on control path (0.8.20)
- [x] MED-14 — coordinator detach txn leak — fixed: take_all rollback on detach (0.8.20)
- [x] MED-15 — txn_results starvation — fixed: self-drain arm (execution already detached) (0.8.20)
- [x] MED-16 — SCRAM oracle — fixed: deterministic decoy verifier for unknown users (0.8.20)

## Phase D — Lows  ✅ COMPLETE (0.8.21→0.8.26)

- [x] LOW-1 — dead-letter re-ready — fixed: readd_on_failure flag (0.8.21)
- [x] LOW-2 — FIFO poll-order fragility — documented (0.8.21)
- [x] LOW-3 — open_sub leak — fixed: close_sub on deserialize error (0.8.21)
- [x] LOW-4 — bootstrap Fatal arm — fixed (0.8.22)
- [x] LOW-5 — OpenSub teardown race — fixed: closed flag on leader subs (0.8.22)
- [x] LOW-6 — current-segment leak — fixed: reclaim on roll (0.8.21)
- [x] LOW-7 — proxy unbind — fixed: handle_msg returns keep-running (0.8.23)
- [x] LOW-8 — zombie consumer attach — fixed: end session on actor death (0.8.23)
- [x] LOW-9 — FNV dir collision — fixed: SHA-256 directory tags (0.8.25)
- [x] LOW-10 — snapshot-id reset — fixed in Phase A (0.8.4)
- [x] LOW-11 — silent settle unpersist — fixed: warn on batch abort (0.8.25)
- [x] LOW-12 — global max_queues — fixed: max_queues_per_vhost (0.8.26)
- [x] LOW-13 — knows_link authz — documented + regression test (0.8.26)
- [x] LOW-14 — declare slot leak — fixed: session check first (0.8.23)
- [x] LOW-15 — txn error fidelity — fixed: TransactionError domain + global-id reject (core 0.2.4 / broker 0.8.24)
- [x] LOW-16 — snapshot pin leak — fixed: finish_snapshot always called (0.8.22)
- [x] LOW-17 — PLAIN plaintext/timing — documented (0.8.25)

## Session log

- 2026-07-07: plan created; starting Phase A / CRIT-1.
- 2026-07-07: ALL 49 findings complete. ramqp-broker 0.8.1->0.8.26, ramqp-core 0.2.3->0.2.4.
  One commit (or small group) per finding, each with a regression test where testable.
