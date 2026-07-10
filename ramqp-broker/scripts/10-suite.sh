#!/usr/bin/env bash
# Stage 1 — the full test suite, both feature sets, plus a flake-repeat loop.
#
# The broker's nastiest bug so far (the close-time settlement drain requeuing
# acked messages) only showed up on *repeated* runs against a long-lived
# process. Deterministic-looking suites still hide races — so we run the broker
# suite several times and require every pass green. nextest gives fast parallel
# runs and a clean per-test report; we fall back to `cargo test` if it's absent.
#
# Knobs: RAMQP_SUITE_REPEAT (default 3).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 1: test suite + flake loop"

REPEAT="${RAMQP_SUITE_REPEAT:-3}"
LOG="$RAMQP_OUT/suite"

if command -v cargo-nextest >/dev/null 2>&1; then
  RUN=(cargo nextest run)
  info "using cargo-nextest"
else
  RUN=(cargo test)
  warn "cargo-nextest not installed; using 'cargo test' (slower, no per-test retry report)"
fi

# Full workspace (default-members), default features then all features. The
# all-features pass is the one that exercises store-redb (durable queues).
check "workspace suite — default features" \
  "${RUN[@]}"

check "workspace suite — all features" \
  "${RUN[@]}" --all-features

# nextest does not run doctests; cargo test --doc does. The broker README and
# lib carry runnable examples, so cover them explicitly.
check "doctests — all features" \
  cargo test --doc --all-features -q

# Flake loop: hammer the broker suite specifically (the timing-sensitive part).
section "flake loop: broker suite ×$REPEAT (all features)"
flakes=0
for i in $(seq 1 "$REPEAT"); do
  if "${RUN[@]}" -p ramqp-broker --all-features >"$LOG.repeat-$i.log" 2>&1; then
    ok "broker suite pass $i/$REPEAT"
  else
    flakes=$((flakes + 1))
    err "broker suite FAILED on pass $i/$REPEAT — see $LOG.repeat-$i.log"
    tail -20 "$LOG.repeat-$i.log" || true
  fi
done
if ((flakes == 0)); then
  pass "flake loop: $REPEAT/$REPEAT green"
else
  fail "flake loop: $flakes/$REPEAT runs failed (nondeterminism or a real race)"
fi

finish
