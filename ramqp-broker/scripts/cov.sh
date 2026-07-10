#!/usr/bin/env bash
# MANUAL coverage report — NOT in run-all, NOT in CI. Runs the broker + core
# suites under llvm-cov instrumentation once and writes a text summary and an
# HTML report to out/. Use it to find untested paths, not as a gate.
#
#   scripts/cov.sh

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "coverage (ramqp-broker + ramqp-core, all features)"

if ! command -v cargo-llvm-cov >/dev/null 2>&1; then
  warn "cargo-llvm-cov not installed (cargo install cargo-llvm-cov)"
  exit 0
fi

HTML="$RAMQP_OUT/coverage"
SUMMARY="$RAMQP_OUT/coverage-summary.txt"

info "clean previous coverage data"
cargo llvm-cov clean --workspace

info "running instrumented suites (this compiles + runs the tests once)…"
cargo llvm-cov --no-report --all-features -p ramqp-broker -p ramqp-core

info "text summary"
cargo llvm-cov report --summary-only -p ramqp-broker -p ramqp-core | tee "$SUMMARY"

info "html report"
cargo llvm-cov report --html --output-dir "$HTML" -p ramqp-broker -p ramqp-core

ok "summary: $SUMMARY"
ok "html:    $HTML/html/index.html"
