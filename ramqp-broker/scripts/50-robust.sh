#!/usr/bin/env bash
# Stage 5 — robustness / DoS resilience against a LIVE broker daemon.
#
# The `robust` driver hits the broker with connection floods, slow-loris,
# garbage bytes, and malformed frames, and after every wave round-trips a real
# message with the ramqp client. The broker must stay live and responsive
# throughout — adversarial peers get reaped, they never wedge the accept loop,
# exhaust fds, or crash the process.
#
# Knobs: RAMQP_ROBUST_SECS (20, total across 4 waves) RAMQP_ROBUST_CONNS (200).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 5: robustness"

SECS="${RAMQP_ROBUST_SECS:-20}"
CONNS="${RAMQP_ROBUST_CONNS:-200}"

build_brokerd >/dev/null
info "building robust driver"
cargo build --release -q -p ramqp-broker --example robust
ROBUST="$ROOT/target/release/examples/robust"

PORT="$(free_port)"
spawn_brokerd robust-broker -- --listen "127.0.0.1:$PORT"
if ! wait_port "$PORT" 30; then
  fail "robust: broker never came up"; finish; exit 1
fi
ok "broker up on :$PORT (pid $BROKERD_PID)"

check "robust: broker stays live under floods / slow-loris / malformed frames" \
  env "ROBUST_URL=amqp://127.0.0.1:$PORT" "ROBUST_SECS=$SECS" "ROBUST_CONNS=$CONNS" "$ROBUST"

assert "robust: broker process still alive after all attacks" "kill -0 $BROKERD_PID 2>/dev/null"
check "robust: no broker panics" no_panics robust-broker

finish
