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

| bin       | what it measures |
|-----------|------------------|
| `rig`     | The rigorous comparison: matched credit windows, warmup, multiple body sizes, N trials, median/min/max. Per client via `BENCH_CLIENT`. |
| `bench`   | Quick head-to-head send + recv throughput (one or both clients). |
| `confirm` | Isolation experiment: ramqp `recv()` only vs `recv()`+`accept()`, to pin settlement cost. |

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
