# ramqp-bench-compare

A fair, reproducible benchmark harness comparing **ramqp** against
**fe2o3-amqp** over a live AMQP 1.0 broker, plus an isolation experiment for
diagnosing the receive path.

This is a workspace member but is **excluded from the published `ramqp`
crate** — its `fe2o3-amqp` dependency never ships with the library. Build it
explicitly:

```sh
cargo build -p ramqp-bench-compare --release
```

## Binaries

| bin         | what it measures |
|-------------|------------------|
| `rig`       | The rigorous comparison: matched credit windows, warmup, multiple body sizes, N trials, median/min/max. Per client via `BENCH_CLIENT`. |
| `bench`     | Quick head-to-head send + recv throughput (one or both clients). |
| `confirm`   | Isolation experiment: ramqp `recv()` only vs `recv()`+`accept()`, to pin settlement cost. |
| `wscompare` | ramqp receive throughput over the transport in `AMQP_URL` (`amqp://` = TCP, `ws://` = WebSocket), one long-lived connection — for comparing the WS vs TCP transport on the same broker. |
| `probe`     | Minimal connectivity check with per-step timeouts (handy when bringing up a new broker/transport). |
| `wsproxy`   | Transparent WebSocket→TCP proxy, to benchmark AMQP-over-WebSocket against a broker that only speaks TCP (e.g. RabbitMQ). |

## Fairness controls (in `rig`)

- **Matched credit windows** — both clients use `Auto(1000)`. (Defaults differ:
  fe2o3 = 200, ramqp = 1000; leaving them at defaults flatters ramqp and makes
  fe2o3 look noisy.)
- **Warmup trial discarded** before timing.
- **Steady-state receive** — prefill the queue with N, then time draining
  exactly N (`recv` + settle). Send is not timed.
- **Multiple body sizes / trials**, broker restarted between clients.

## Running

```sh
# RabbitMQ 4.x (native AMQP 1.0); declare the queue first.
docker run -d --rm --name rabbit -p 5672:5672 -p 15672:15672 rabbitmq:4-management
curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/ramqp_it \
  -H content-type:application/json -d '{"durable":true}'

export AMQP_URL=amqp://guest:guest@localhost:5672 AMQP_ADDRESS=/queues/ramqp_it

BENCH_CLIENT=ramqp                cargo run -p ramqp-bench-compare --release --bin rig
RAMQP_BATCH=100 BENCH_CLIENT=ramqp cargo run -p ramqp-bench-compare --release --bin rig
BENCH_CLIENT=fe2o3                cargo run -p ramqp-bench-compare --release --bin rig
```

Env: `BENCH_CLIENT` (`ramqp`|`fe2o3`), `RAMQP_BATCH` (>1 = batched `accept_through`).

## WebSocket vs TCP

RabbitMQ doesn't expose AMQP 1.0 over WebSocket, but **ActiveMQ Artemis does**
natively (its AMQP acceptor auto-detects the WS upgrade and negotiates the
`amqp` subprotocol). Run `wscompare` over both schemes against the same broker:

```sh
docker run -d --name artemis -e ARTEMIS_USER=guest -e ARTEMIS_PASSWORD=guest \
  -p 5682:5672 -p 8162:8161 apache/activemq-artemis:latest
# Artemis needs an anycast queue (store-and-forward) for prefill-then-drain:
docker exec artemis /var/lib/artemis-instance/bin/artemis queue create \
  --name ramqp_it --address ramqp_it --anycast --durable \
  --auto-create-address --preserve-on-no-consumers --user guest --password guest --silent

export AMQP_ADDRESS=ramqp_it
AMQP_URL=amqp://guest:guest@localhost:5682 cargo run -p ramqp-bench-compare --release --bin wscompare
AMQP_URL=ws://guest:guest@localhost:5682   cargo run -p ramqp-bench-compare --release --bin wscompare
```

For a same-broker isolation against a TCP-only broker (e.g. RabbitMQ), front it
with `wsproxy` and point `wscompare` at `ws://127.0.0.1:5673`:

```sh
WS_LISTEN=127.0.0.1:5673 WS_UPSTREAM=127.0.0.1:5672 \
  cargo run -p ramqp-bench-compare --release --bin wsproxy &
```

**Finding (Artemis, directional, high variance):** the WS transport is **on par
with TCP** — within ~15%, ahead at 64 B and slightly behind at 1–8 KB (the
masking/copy cost scales with payload). The WS layer is not a meaningful
throughput penalty. Note Artemis throttles rapid AMQP-over-*TCP* reconnects
(handshake timeouts under the per-trial-reconnect `rig`), while its WS path stays
healthy — a broker-side quirk, not a client one (the same `rig` is fine on
RabbitMQ); `wscompare` sidesteps it with one long-lived connection.

## Results (ramqp 0.7.2 vs fe2o3-amqp 0.15.1, RabbitMQ 4.x, recv msg/s median)

| body | ramqp (per-msg) | fe2o3 (per-msg) | ramqp (batched) |
|------|----------------:|----------------:|----------------:|
| 64 B |         188,000 |         177,000 |     **263,000** |
| 1 KB |         120,000 |         119,000 |     **150,000** |
| 8 KB |          42,000 |          42,000 |      **54,000** |

**Takeaways:**

- On the identical per-message path, ramqp is at **parity or slightly ahead** of
  the mature incumbent — helped by two 0.7.2 receive-path optimizations: removing
  a per-frame `clock_gettime` from heartbeat bookkeeping, and a read-preferring
  driver loop that coalesces per-message settlements into one write.
- Using ramqp's **batched ranged settlement** (`accept_through`, which fe2o3's
  public API lacks) makes it **~1.3–1.4x faster** across body sizes.
- Send throughput is a tie for both — it is bound by per-message broker
  settlement, not the client.
- Profiling shows the remaining per-message cost is ~80% async-runtime overhead
  (scheduler, cross-thread task wakeups, mpsc) rather than ramqp's own codec or
  transport — further gains require structural changes (e.g. batched recv/send
  APIs, fewer task hops), not micro-optimization.

Numbers are from a single-node RabbitMQ on WSL2 and are sensitive to broker
credit/prefetch dynamics; treat them as directional. Re-run on your own broker.

## First broker numbers — ramqp-broker Phase 4 (2026-07-05)

The `latency` bin (broker.md §3.4 harness): closed-loop e2e latency
(produce-settled → consume → accept, one in flight, 5000 samples), then a
50k-message blast-and-drain for throughput. Same machine, same harness, same
`ramqp` client stack on both legs for every target; 256 B payloads.

| 256 B, defaults | **ramqp-broker** | RabbitMQ 4.3.1 | Artemis (JVM) |
|---|---|---|---|
| p50 latency        | **89 µs**   | 251 µs | 227 µs |
| p90                | **123 µs**  | 323 µs | 326 µs |
| p99                | **213 µs**  | 519 µs | 576 µs |
| p99.9              | **428 µs**  | 777 µs | 833 µs |
| max                | **683 µs**  | 2119 µs | 1299 µs |
| blast throughput   | **326k msg/s** | 48k msg/s | 79k msg/s |
| broker memory      | ~40 MiB¹    | 133 MiB² | 715 MiB² |

¹ whole-process RSS *including* the client and harness (in-process broker).
² `docker stats` container memory, idle-adjacent.

**Read honestly:** first smoke numbers, not a rigorous claim. Incumbents run
untuned defaults in docker (loopback + docker NAT hop) vs our in-process
loopback; single queue, single connection, WSL2; RabbitMQ used a durable
classic queue (4.x forbids transient non-exclusive by default) though
messages were non-persistent. The tuned, isolated, multi-load comparison is
the standing §3.4 deliverable. What these numbers do establish: the
architecture is in the right latency class from day one — every percentile
2–3× below both incumbents and 4–6× their throughput on identical hardware.

Run it yourself:

```sh
cargo run -p ramqp-bench-compare --release --bin latency                  # ours, in-process
LAT_URL=amqp://guest:guest@localhost:5672 LAT_ADDRESS=/queues/bench-lat \
    cargo run -p ramqp-bench-compare --release --bin latency              # any broker
```

## Quorum-queue numbers — ramqp-broker Phase 6 (2026-07-06)

Same harness, same machine, 256 B payloads: replicated (`/quorum/*`) queues
after the Phase 6 forwarding fabric landed. The 3-node cluster runs three
`ramqp-brokerd` processes over real loopback TCP (fabric + Raft replication,
replication factor 3); "via follower" attaches the client to a node that does
NOT lead the queue's Raft group, so every message crosses the internal
forwarding fabric to the leader and back.

| 256 B, defaults | transient (in-proc) | quorum ×1 (in-proc) | **quorum ×3, leader node** | **quorum ×3, via follower** | RabbitMQ 4.3.1 quorum |
|---|---|---|---|---|---|
| p50 latency      | 92 µs  | 120 µs | **288 µs** | **427 µs** | 2234 µs |
| p90              | 118 µs | 143 µs | **347 µs** | **489 µs** | 2639 µs |
| p99              | 162 µs | 245 µs | **429 µs** | **626 µs** | 3963 µs |
| p99.9            | 259 µs | 396 µs | **633 µs** | **815 µs** | 7496 µs |
| blast throughput | 286k msg/s | 157k msg/s | **141k msg/s** | **157–165k msg/s** | 39k msg/s |
| broker memory    | 28 MiB¹ | 218 MiB¹ | 451 MiB leader / ~190 MiB per follower² | — | 226 MiB³ |

¹ whole-process RSS including client+harness (in-process broker).
² per-`ramqp-brokerd` VmRSS after three consecutive 50k blasts; dominated by
the in-memory Raft log + snapshot copies (high-water, not steady-state — the
on-disk log and paged state machine are Phase 7).
³ `docker stats` container memory after the run.

**Read honestly:** the headline asymmetry — our quorum queue commits its Raft
log **in memory** while RabbitMQ's quorum queue fsyncs to disk — makes the
latency gap partly a durability gap; the disk-backed comparison is redo-work
for Phase 7. Also: RabbitMQ ran as a single-node cluster (its quorum queue
had one member, no replication RTT) while ours replicated to 3 members over
loopback TCP — that asymmetry favors *RabbitMQ*. Untuned defaults, WSL2,
docker NAT on the RabbitMQ leg. What the numbers do establish: the
consensus-per-publish path (commit-backed accepts) plus the forwarding fabric
keeps tails flat — p99.9/p50 ≈ 2× on every leg (§3.1 target ≤10×) — and one
fabric hop costs ~140 µs p50 on this machine.

Found by this bench (worth the price of admission): a broker-side bug where
the close-time settlement drain polled tokio oneshots with an exhausted
cooperative budget, silently requeuing hundreds of already-acked messages per
busy connection close (duplicates for the next consumer). Fixed + regression-
tested (`blast::close_after_blast_leaves_nothing_to_redeliver`).

## Deep-queue numbers — paged state machine, Phase 7 (2026-07-07)

The `depth` bin: publish→accepted (closed-loop) latency and process RSS as a
`/quorum/*` queue fills — the broker.md §8 #1-risk defense. In-process broker
(RSS includes client + harness); paging = `DEPTH_DATA_DIR` set (64 MiB
resident-body budget; bodies beyond it spill to segment files; snapshot blobs
park on disk).

**Tail flatness (256 B bodies, 0 → 1M deep, paged):**

| depth | p50 | p99 | p99.9 | RSS |
|---|---|---|---|---|
| 2k    | 117 µs | 197 µs | 316 µs  | 19 MiB |
| 102k  | 117 µs | 244 µs | 5.0 ms  | 167 MiB |
| 502k  | 122 µs | 222 µs | 3.9 ms  | 619 MiB |
| 1.00M | 125 µs | 254 µs | 5.3 ms  | 1320 MiB |

p50/p99 are **flat from empty to a million deep**; drain-out of the deep
queue runs 274k msg/s. The p99.9/max spikes are snapshot builds (each one
clones + serializes the full index; cadence is every 50k applies) —
incremental snapshots are the standing follow-up.

**The paging headline (4 KiB bodies × 252k ≈ 1 GiB of payload):**

| | paged | unpaged |
|---|---|---|
| RSS at full depth | **878 MiB** | 2461 MiB |
| final RSS after drain | **908 MiB** | 3040 MiB |
| p50 at full depth | 124 µs | 126 µs |
| drain throughput | 248k msg/s | 274k msg/s |

~3x less memory at identical p50, for a ~10% drain-throughput cost — the
flow-to-disk trade §3.1 asks for. At 256 B bodies the *index* (BTreeMap +
ready-set + raft bookkeeping) dominates RSS, so paging buys proportionally
less; body-size scaling is exactly the point of the split.

Known limitation (documented in the code): a paged queue's snapshot keeps
spilled bodies **external** (node-local refs) — follower catch-up *via
snapshot* for a deep paged queue is not yet supported (log-replay catch-up
is); segment shipping is the follow-up.

## Tuned-incumbent re-run — Phase 10 (provisional)

The Phase 4/6 tables above ran RabbitMQ at stock defaults; broker.md §3.4 asks
for a re-run against a **tuned** incumbent, and for the quorum leg to be
**durability-parity** (our quorum fsyncs its Raft log via `store-redb`, matched
against RabbitMQ's fsync-backed quorum queue — closing the "in-memory vs fsync"
caveat that made the Phase 6 quorum gap partly a durability gap). Every leg here
runs **over loopback TCP against a broker process**, so the transport path is
identical for all rows (the Phase 4 table compared ours in-process vs RabbitMQ
in docker; this is fairer).

Closed-loop e2e latency, µs, 20 000 samples ([`tuned/rabbitmq.conf`](tuned/rabbitmq.conf);
tuned Artemis = NIO journal + autotune off). Representative run:

| leg | p50 | p99 | p99.9 |
|---|--:|--:|--:|
| ramqp-broker transient            | 93.7 | 263.0 | 340.4 |
| RabbitMQ 4.x classic (**tuned**)  | 249.1 | 467.4 | 650.6 |
| Artemis (**tuned**, NIO)          | 292.1 | 704.2 | 1132.5 |
| ramqp-broker quorum (`store-redb`, fsync) | 304.3 | 665.3 | 866.9 |
| RabbitMQ 4.x quorum (fsync)       | 2260.5 | 3834.4 | 7303.2 |

The headline holds under tuning and at durability parity: transient p50 ≈ 2.7×
below tuned RabbitMQ classic and ≈ 3× below tuned Artemis, and — the point of
the re-run — our **fsync-backed quorum** is ≈ 7× below RabbitMQ's fsync quorum,
so the Phase 6 gap was *not* merely a durability artifact.

> **⚠️ PROVISIONAL — indicative only.** These numbers were taken on a
> shared/virtualized box (WSL2), not quiet bare metal, so they are directional,
> not the "defend-forever" figures broker.md §3.4 requires. Reproduce (and
> generate the real numbers on isolated hardware) with the one command:
>
> ```sh
> bench-compare/tuned/run.sh    # stands up tuned RabbitMQ (5673) + Artemis (5674), runs all legs
> ```
>
> Remaining §3.4 items: the bare-metal run, and a latency-under-N-concurrent-
> connections sweep (this table is single-connection closed-loop).
