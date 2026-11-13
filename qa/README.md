# Pre-release QA

`qa/prerelease.sh` runs everything worth running before a release, in one
command, and ends with a pass/fail table. `ramqp-qa` (this crate, never
published) is the tool behind the stages that existing tests don't cover.

```sh
qa/prerelease.sh                    # quick profile, ~30-40 min
qa/prerelease.sh --full             # release profile, ~2-3 h (longer fuzz/soak/chaos + partition)
qa/prerelease.sh --only diff,bench  # some stages
qa/prerelease.sh --skip interop
qa/prerelease.sh --update-baseline  # record this machine's benchmark baseline
```

Results go to `target/prerelease/<timestamp>/`: one log per stage, every
workload's JSON verdict, benchmark results, resource samples, and
`summary.txt`. The exit status is non-zero if any stage failed.

## Stages

| Stage | What it proves | Fails when |
|---|---|---|
| `gates` | fmt, clippy on every feature set, docs, every test suite (all-features, default, FIPS, `scram-fips`, the fe2o3 leg), the 3-crate publish dry-run | any of them fails |
| `fuzz` | the `ramqp-core` decoders survive arbitrary input (`ramqp-core/fuzz`, time-boxed) | a crash or timeout |
| `interop` | the 5-client-stack matrix, the Qpid JMS and Proton legs, and the `ramqp` client against RabbitMQ 4.x, ActiveMQ Artemis and `ramqp-brokerd` | any leg fails |
| `diff` | the same protocol scenarios against `ramqp-brokerd`, RabbitMQ 4.x and Artemis (outcomes, redelivery and `delivery-count`, sections preserved, 1 MiB messages, TTL, `first-acquirer`, …) | the broker behaves differently from the reference and `qa/known-differences.json` doesn't explain why |
| `guarantees` | workloads on transient, durable and quorum queues: steady consumers; consumers leaving every way a real one does (detach, drop, end session, close); frames coalesced into one read; connections cut mid-flight | an acknowledged message is lost, an accepted message is redelivered (except across a cut connection), or per-producer order breaks (steady consumers only) |
| `chaos` | `kill -9` the broker repeatedly under load and restart it on the same data | an acknowledged message on a durable or quorum queue is lost (transient: only that nothing hangs) |
| `bench` | throughput and latency percentiles for fixed scenarios, closed loop and fixed rate | throughput drops more than 15%, or p99 latency rises more than 50% (and by over 0.5 ms), against `qa/baselines/<host>.json` |
| `soak` | mixed load on every queue kind for a long time while sampling the broker's memory, file descriptors and threads | a workload fails, or memory or descriptors keep growing after warm-up |
| `partition` | the netns split-brain test for a 3-node cluster (`ramqp-broker/tests/partition`) | it fails; needs passwordless `sudo`; `--full` only |

A stage is **skipped** (not failed) when a tool it needs is missing; the
summary says which.

## Requirements

Rust stable and nightly (fuzzing), `cargo-fuzz`, Docker (RabbitMQ and
Artemis containers, the Proton leg), Java, Node, Go, OpenSSL, `jq`, `curl`.
FIPS builds also need CMake. The partition stage needs passwordless `sudo`.

The `diff` and `interop` stages reuse containers named `ramqp-rabbit`
(AMQP on 5672, management on 15672) and `ramqp-artemis` (AMQP on 5673), or
create them; containers the script starts are stopped at the end.

## Tuning

Every duration has an environment override:

| Variable | quick | full |
|---|---|---|
| `QA_FUZZ_SECS` (per target) | 60 | 600 |
| `QA_GUARANTEE_SECS` (per workload) | 20 | 120 |
| `QA_CHAOS_KILLS` (every `QA_CHAOS_INTERVAL`=6 s) | 3 | 10 |
| `QA_SOAK_SECS` | 300 | 3600 |
| `QA_BENCH_MESSAGES` | 100 000 | 300 000 |
| `QA_PARTITION` | 0 | 1 |

Also `QA_PORT_BASE` (5750), `QA_MAX_THROUGHPUT_DROP` (15),
`QA_MAX_P99_RISE` (50), and the container names and ports
(`QA_RABBIT_*`, `QA_ARTEMIS_*`).

## Baselines

Benchmarks are only comparable on the same machine, so each host has its own
baseline: `qa/baselines/<hostname>.json`. The first `bench` run on a host
writes it; `--update-baseline` replaces it (after a deliberate performance
change). Commit it so the next release compares against it.

## Known differences

`qa/known-differences.json` lists behaviour where `ramqp-brokerd` differs
from another broker on purpose, with the reason:

```json
{ "rabbitmq": { "missing_queue_attach": "by design: …" } }
```

Add an entry only for a deliberate difference; a gap belongs in an issue.

## `ramqp-qa` on its own

Each subcommand works against any AMQP 1.0 broker (`--help` on each):

- `workload`: concurrent produce/consume with consumer churn and a delivery
  guarantee check;
- `proxy`: a TCP proxy that coalesces frames, adds latency and cuts
  connections;
- `bench` and `compare`: throughput and latency, and the baseline check;
- `diff`: the cross-broker scenarios (`--list-queues` prints the queue
  names to declare first);
- `resources`: memory, file-descriptor and thread sampling of a process.

```sh
cargo build --release -p ramqp-qa
target/release/ramqp-qa workload --url amqp://guest:guest@localhost:5672 \
    --address /queues/my-test --secs 60 --exit-every 50 --no-order-check
```
