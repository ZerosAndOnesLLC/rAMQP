#!/usr/bin/env bash
# Stage 6 — fuzz the untrusted-wire decoders (a broker parses bytes from anyone
# who can open a socket, so this is the highest-value robustness surface).
# Bounded time per target; a crash writes a reproducer under fuzz/artifacts.
#
# Needs a nightly toolchain and cargo-fuzz. Skips (warns) if either is absent.
#
# Knobs: RAMQP_FUZZ_SECS (60, per target).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 6: fuzz (decode_frame, Value)"

FUZZ_SECS="${RAMQP_FUZZ_SECS:-60}"

if ! rustup toolchain list 2>/dev/null | grep -q nightly; then
  warn "no nightly toolchain (rustup toolchain install nightly); skipping fuzz"
  finish; exit 0
fi
if ! command -v cargo-fuzz >/dev/null 2>&1; then
  warn "cargo-fuzz not installed (cargo install cargo-fuzz); skipping fuzz"
  finish; exit 0
fi

cd "$ROOT/ramqp-core"
for target in decode_frame value; do
  info "fuzzing $target for ${FUZZ_SECS}s"
  if cargo +nightly fuzz run "$target" -- \
       -max_total_time="$FUZZ_SECS" -rss_limit_mb=4096 \
       >"$RAMQP_OUT/fuzz-$target.log" 2>&1; then
    pass "fuzz: $target — no crash in ${FUZZ_SECS}s"
  else
    fail "fuzz: $target — CRASH found (repro in ramqp-core/fuzz/artifacts/$target, log: fuzz-$target.log)"
    tail -25 "$RAMQP_OUT/fuzz-$target.log" | sed 's/^/    /' || true
  fi
done
cd "$ROOT"

finish
