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

## Results (ramqp 0.7.1 vs fe2o3-amqp 0.15.1, RabbitMQ 4.x, recv msg/s median)

| body | ramqp (per-msg) | fe2o3 (per-msg) | ramqp (batched) |
|------|----------------:|----------------:|----------------:|
| 64 B |         186,000 |         189,000 |     **229,000** |
| 1 KB |         115,000 |         126,000 |     **157,000** |
| 8 KB |          40,000 |          40,000 |      **52,000** |

**Takeaways:**

- On the identical per-message path, ramqp is at **parity** with the mature
  incumbent (within a few %).
- Using ramqp's **batched ranged settlement** (`accept_through`, which fe2o3's
  public API lacks) makes it **~1.2–1.3x faster** across body sizes.
- Send throughput is a tie for both — it is bound by per-message broker
  settlement, not the client.

Numbers are from a single-node RabbitMQ on WSL2 and are sensitive to broker
credit/prefetch dynamics; treat them as directional. Re-run on your own broker.
