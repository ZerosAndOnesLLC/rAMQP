#!/usr/bin/env bash
# MANUAL performance runner — NOT part of run-all, NEVER in CI. Benchmarks must
# not gate anything (numbers move with the machine); this is for a human to spot
# drift build-over-build.
#
# Runs the `latency` bin several trials against the in-process ramqp-broker (or
# an external one via LAT_URL), medians the metrics, and diffs a committed
# baseline. `--save-baseline` stores the current run as the new baseline (commit
# it so drift shows up in git).
#
#   scripts/bench.sh                    # run + compare to baseline
#   scripts/bench.sh --save-baseline    # run + store baseline
#   LAT_URL=amqp://host:5672 scripts/bench.sh   # against an external broker
#
# Knobs: BENCH_TRIALS (5) + latency-bin knobs (LAT_N, LAT_LAT_N, LAT_PAYLOAD, ...).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "manual perf bench (not a gate)"

TRIALS="${BENCH_TRIALS:-5}"
BASELINE="$SCRIPTS_DIR/bench-baseline.json"
SAVE=0
[[ "${1:-}" == "--save-baseline" ]] && SAVE=1

info "building latency bin"
cargo build --release -q -p ramqp-bench-compare --bin latency
LAT="$ROOT/target/release/latency"

LOG="$RAMQP_OUT/bench-latency.log"
: >"$LOG"
for t in $(seq 1 "$TRIALS"); do
  info "trial $t/$TRIALS"
  "$LAT" 2>&1 | tee -a "$LOG"
done

METRICS="$RAMQP_OUT/bench-metrics.json"
python3 "$SCRIPTS_DIR/bench_stats.py" parse "$LOG" >"$METRICS"
section "median over $TRIALS trials"
cat "$METRICS"

if [[ $SAVE -eq 1 ]]; then
  cp "$METRICS" "$BASELINE"
  ok "baseline saved → $BASELINE (commit it to track perf over builds)"
elif [[ -f "$BASELINE" ]]; then
  section "vs baseline"
  python3 "$SCRIPTS_DIR/bench_stats.py" compare "$METRICS" "$BASELINE" \
    || warn "perf drift beyond tolerance (see table above)"
else
  warn "no baseline yet — run 'scripts/bench.sh --save-baseline' to create one"
fi
