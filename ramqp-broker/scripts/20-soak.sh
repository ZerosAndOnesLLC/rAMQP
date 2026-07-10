#!/usr/bin/env bash
# Stage 2 — soak / leak detector.
#
# Runs a separate brokerd process under sustained, churny load for N minutes and
# samples the BROKER's RSS the whole time. Two failure modes it catches:
#   * a memory leak — RSS climbs instead of holding flat under bounded depth;
#   * throughput decay — the class of regression that exposed the close-time
#     settlement-drain bug (repeated busy connection closes degrading the broker).
# Connection churn is on by default so the close path is exercised hard.
#
# Knobs: RAMQP_SOAK_SECS (120) RAMQP_SOAK_PAIRS (8) RAMQP_SOAK_CHURN (500).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 2: soak / leak"
require_cmd python3 || { finish; exit 1; }

SECS="${RAMQP_SOAK_SECS:-120}"
PAIRS="${RAMQP_SOAK_PAIRS:-8}"
CHURN="${RAMQP_SOAK_CHURN:-500}"

build_brokerd >/dev/null
info "building loadgen"
cargo build --release -q -p ramqp-broker --example loadgen
LOADGEN="$ROOT/target/release/examples/loadgen"

PORT="$(free_port)"
spawn_brokerd soak-broker -- --listen "127.0.0.1:$PORT"
if ! wait_port "$PORT" 30; then
  fail "soak: broker never came up on :$PORT"
  finish; exit 1
fi
ok "broker up on :$PORT (pid $BROKERD_PID), soaking ${SECS}s with churn=$CHURN"

# Background RSS sampler.
RSSF="$RAMQP_OUT/soak-rss.tsv"
: >"$RSSF"
( while kill -0 "$BROKERD_PID" 2>/dev/null; do
    printf '%s\t%s\n' "$(date +%s)" "$(rss_kib "$BROKERD_PID")" >>"$RSSF"
    sleep 2
  done ) &
sampler=$!

LOADLOG="$RAMQP_OUT/soak-loadgen.log"
LOAD_URL="amqp://127.0.0.1:$PORT" LOAD_ADDRESS="/queues/soak" \
  LOAD_SECS="$SECS" LOAD_PAIRS="$PAIRS" LOAD_CHURN="$CHURN" LOAD_REPORT_SECS=5 \
  "$LOADGEN" 2>&1 | tee "$LOADLOG"

kill "$sampler" 2>/dev/null || true

# --- evaluate --------------------------------------------------------------
eval "$(python3 "$SCRIPTS_DIR/analyze_soak.py" "$RSSF" "$LOADLOG")"
info "RSS: early=${RSS_EARLY_KIB}KiB late=${RSS_LATE_KIB}KiB leak=${RSS_LEAK_KIB}KiB peak=${RSS_PEAK_KIB}KiB (${SAMPLES_RSS} samples)"
info "throughput: early=${RATE_EARLY} late=${RATE_LATE} msg/s (${SAMPLES_RATE} samples)"

assert "soak: RSS sampler collected data" "[ ${SAMPLES_RSS:-0} -ge 5 ]"

# Leak: fail only if RSS grew BOTH >50 MiB absolute AND >25% relative (avoids
# flagging normal allocator/steady-state jitter).
if [ "${RSS_LEAK_KIB:-0}" -gt 51200 ] && [ $(( RSS_LATE_KIB * 100 )) -gt $(( RSS_EARLY_KIB * 125 )) ]; then
  fail "soak: broker RSS grew ${RSS_LEAK_KIB}KiB (${RSS_EARLY_KIB}→${RSS_LATE_KIB}) — possible leak"
else
  pass "soak: broker RSS stayed flat under sustained churn"
fi

# Throughput decay: late window must hold >=60% of the early window.
if [ "${RATE_EARLY:-0}" -gt 0 ] && [ $(( RATE_LATE * 100 )) -lt $(( RATE_EARLY * 60 )) ]; then
  fail "soak: throughput decayed ${RATE_EARLY}→${RATE_LATE} msg/s (>40% drop) — degradation under long run"
else
  pass "soak: throughput held steady (no degradation)"
fi

check "soak: no broker panics" no_panics soak-broker

finish
