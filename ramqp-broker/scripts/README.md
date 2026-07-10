# ramqp-broker test scripts

A consistent, re-runnable battery for the broker — the same checks build over
build, so regressions in correctness, memory, HA, interop, or robustness are
caught before a crates.io release. **None of these run in CI** (yet); they are
run by hand or via `run-all.sh`.

Every script sources [`lib.sh`](lib.sh) (logging, PASS/FAIL accounting, brokerd
spawn/teardown, port + RSS helpers), prints a clear per-check pass/fail, exits
non-zero on any failure, and writes artifacts (logs, metrics, spawned data
dirs) to a timestamped `out/<ts>/` directory (gitignored).

## Quick start

```sh
cd <repo root>
ramqp-broker/scripts/run-all.sh --quick     # gates + full test suite (fast)
ramqp-broker/scripts/run-all.sh             # the default battery
ramqp-broker/scripts/run-all.sh chaos soak  # a specific subset, in order
```

## The stages

| # | Script | What it proves | Needs |
|---|--------|----------------|-------|
| 0 | `00-gates.sh` | fmt, clippy `-D warnings`, `cargo check --all-features`, docs, `cargo audit`, `cargo deny` all clean | — |
| 1 | `10-suite.sh` | the full ~150-test suite passes on **both** feature sets, and stays green across a **flake-repeat loop** (the coop-budget requeue bug only surfaced on repeated runs) | nextest |
| 2 | `20-soak.sh` | sustained + churny load for N minutes leaves broker RSS **flat** and throughput **non-degrading** — the leak/degradation detector | — |
| 3 | `30-chaos.sh` | a 3-node cluster with rolling leader/follower **kills + restarts** loses **zero accepted messages** and recovers; durable data survives restart | store-redb |
| 4 | `40-interop.sh` | the `ramqp` client interop suite passes against ramqp-broker, **RabbitMQ**, and **Artemis**; the independent **fe2o3-amqp** client interops with ramqp-broker | docker |
| 5 | `50-robust.sh` | the broker stays **live and responsive** under connection floods, slow-loris, and malformed/oversized-frame floods against a live daemon | — |
| 6 | `60-fuzz.sh` | the untrusted-wire decoders (`decode_frame`, `Value`) survive bounded **fuzzing** with no panic/hang | cargo-fuzz (nightly) |

## Manual-only (never in `run-all`, never in CI)

| Script | Purpose |
|--------|---------|
| `bench.sh` | latency / depth / throughput harness; captures timestamped JSON and diffs against a committed `baseline.json` to flag perf drift. **Bench is never automated.** |
| `cov.sh` | `cargo llvm-cov` coverage report for the broker + core, to find untested paths. |

## Tuning knobs (env)

| Var | Default | Used by |
|-----|---------|---------|
| `RAMQP_OUT` | `out/<timestamp>` | all (share one dir across a run) |
| `RAMQP_SUITE_REPEAT` | 3 | suite flake loop |
| `RAMQP_SOAK_SECS` | 120 | soak |
| `RAMQP_SOAK_PAIRS` | 8 | soak concurrency |
| `RAMQP_CHAOS_ROUNDS` | 4 | chaos kill/restart rounds |
| `RAMQP_CHAOS_N` | 20000 | chaos messages to verify |
| `RAMQP_ROBUST_SECS` | 20 | robustness flood duration |
| `RAMQP_FUZZ_SECS` | 60 | per-target fuzz time |

## Driver binaries

Load/chaos/robustness drivers that can't be expressed in bash live as **example
binaries** in [`../examples/`](../examples) (`loadgen`, `chaos`, `robust`), so
they are compiled by the normal `cargo check --all-targets` and never rot.
