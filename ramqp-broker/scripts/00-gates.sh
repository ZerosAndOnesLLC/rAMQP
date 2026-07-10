#!/usr/bin/env bash
# Stage 0 — static gates: everything that must be clean before a release,
# independent of any running broker. Fast and deterministic.
#
#   fmt · clippy -D warnings · check --all-features · docs · audit · deny
#
# These mirror (and slightly exceed) what CI enforces on `main`; running them
# here means a working branch never surprises CI.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 0: static gates"

check "rustfmt --check" \
  cargo fmt --all -- --check

check "clippy -D warnings (all targets, all features)" \
  cargo clippy --all-targets --all-features -- -D warnings

check "cargo check (all targets, all features)" \
  cargo check --all-targets --all-features

RUSTDOCFLAGS="-D warnings" check "cargo doc (all features, no deps)" \
  cargo doc --all-features --no-deps -q

# --- supply chain ----------------------------------------------------------
# `cargo audit` and `cargo deny` both read the RustSec DB; keeping both gives an
# independent second opinion. Advisories/bans/sources are hard gates. Licenses
# is soft: the tree pulls the permissive Unicode-3.0 (url→idna→icu) which needs
# a deny.toml allowance — a config gap, not a compliance problem — so it warns
# rather than fails until that file lands.
if require_cmd cargo-audit; then
  check "cargo audit" cargo audit
else
  warn "cargo-audit not installed; skipping (cargo install cargo-audit)"
fi

if require_cmd cargo-deny; then
  check "cargo deny: advisories" cargo deny check advisories
  check "cargo deny: bans"       cargo deny check bans
  check "cargo deny: sources"    cargo deny check sources
  if cargo deny check licenses >/dev/null 2>&1; then
    pass "cargo deny: licenses"
  else
    warn "cargo deny: licenses — needs a deny.toml (Unicode-3.0 allowance); not gating"
  fi
else
  warn "cargo-deny not installed; skipping (cargo install cargo-deny)"
fi

finish
