#!/usr/bin/env bash
# Shared helpers for the ramqp-broker test scripts.
#
# Every script `source`s this. It provides: consistent logging, PASS/FAIL
# accounting, a per-run output directory, a free-port finder, brokerd
# build/spawn/teardown, port-readiness waiting, and RSS sampling. Sourcing it
# installs a cleanup trap that kills every process the script spawned.
#
# Nothing here is wired into CI — these run by hand (or via run-all.sh) so the
# broker gets the same battery of checks build over build.
#
# NOTE: deliberately NOT `set -e`. This is a test orchestrator — commands are
# *expected* to fail and are handled explicitly via check/assert/`||` and the
# pass/fail tally. `set -e` would abort mid-stage on the first failing check (or
# on a helper whose last statement returns non-zero, e.g. a `[[ ]] && cmd` that
# doesn't match), which is exactly wrong here. Keep `-u` and pipefail.

set -uo pipefail

# --- locations -------------------------------------------------------------

# Repo root, robust to being invoked from anywhere.
SCRIPTS_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(git -C "$SCRIPTS_DIR" rev-parse --show-toplevel 2>/dev/null || echo "${SCRIPTS_DIR%/ramqp-broker/scripts}")"

# One timestamped output directory per run (override with RAMQP_OUT to share
# one dir across a run-all invocation). Gitignored.
: "${RAMQP_OUT:=$SCRIPTS_DIR/out/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RAMQP_OUT"

# --- logging ---------------------------------------------------------------

if [[ -t 1 ]]; then
  _C_RESET=$'\033[0m'; _C_RED=$'\033[31m'; _C_GRN=$'\033[32m'
  _C_YEL=$'\033[33m'; _C_BLU=$'\033[34m'; _C_BOLD=$'\033[1m'
else
  _C_RESET=""; _C_RED=""; _C_GRN=""; _C_YEL=""; _C_BLU=""; _C_BOLD=""
fi

log()     { printf '%s\n' "$*"; }
info()    { printf '%s  %s%s\n' "$_C_BLU" "$*" "$_C_RESET"; }
ok()      { printf '%sok  %s%s\n' "$_C_GRN" "$*" "$_C_RESET"; }
warn()    { printf '%sWARN %s%s\n' "$_C_YEL" "$*" "$_C_RESET"; }
err()     { printf '%sERR  %s%s\n' "$_C_RED" "$*" "$_C_RESET" >&2; }
section() { printf '\n%s== %s ==%s\n' "$_C_BOLD" "$*" "$_C_RESET"; }

# --- pass/fail accounting --------------------------------------------------
#
# Each check calls pass/fail with a short label. A summary + exit code are
# emitted by `finish` (call it at the end, or let the EXIT trap do it).

_PASS=0
_FAIL=0
declare -a _FAILED_LABELS=()

pass() { _PASS=$((_PASS + 1)); ok "$*"; }
fail() { _FAIL=$((_FAIL + 1)); _FAILED_LABELS+=("$*"); err "FAIL: $*"; }

# Run a command as a named check; record pass/fail from its exit status.
# Usage: check "label" cmd args...
check() {
  local label="$1"; shift
  info "▶ $label"
  if "$@"; then pass "$label"; else fail "$label"; fi
}

# Assert a condition (already-evaluated boolean via exit status of a command).
# Usage: assert "label" '[ "$x" -gt 0 ]'  — pass the test as a string to eval.
assert() {
  local label="$1"; local expr="$2"
  if eval "$expr"; then pass "$label"; else fail "$label ($expr)"; fi
}

finish() {
  section "summary"
  log "passed: $_PASS   failed: $_FAIL   artifacts: $RAMQP_OUT"
  if ((_FAIL > 0)); then
    for l in "${_FAILED_LABELS[@]}"; do err "  - $l"; done
    return 1
  fi
  return 0
}

# --- process lifecycle -----------------------------------------------------

declare -a _SPAWNED_PIDS=()

_cleanup() {
  local pid
  for pid in "${_SPAWNED_PIDS[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill "$pid" 2>/dev/null || true
  done
  # Give them a moment, then hard-kill any stragglers.
  sleep 0.3 2>/dev/null || true
  for pid in "${_SPAWNED_PIDS[@]:-}"; do
    [[ -n "$pid" ]] || continue
    kill -9 "$pid" 2>/dev/null || true
  done
}
trap _cleanup EXIT INT TERM

# Track a pid for cleanup.
track_pid() { _SPAWNED_PIDS+=("$1"); }

# Forget a pid we deliberately killed (so cleanup doesn't warn on a reused pid).
untrack_pid() {
  local target="$1" i
  for i in "${!_SPAWNED_PIDS[@]}"; do
    [[ "${_SPAWNED_PIDS[$i]}" == "$target" ]] && unset '_SPAWNED_PIDS[$i]'
  done
  return 0 # never let a non-match's `&&` short-circuit escape as our status
}

# --- ports -----------------------------------------------------------------

# Print a currently-free localhost TCP port. Uses python for an atomic bind.
free_port() {
  python3 - <<'PY'
import socket
s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
s.bind(("127.0.0.1", 0))
print(s.getsockname()[1])
s.close()
PY
}

# Wait until a TCP port accepts connections (or timeout). Usage: wait_port PORT [SECS]
wait_port() {
  local port="$1" secs="${2:-30}" i
  for ((i = 0; i < secs * 10; i++)); do
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 0.1
  done
  return 1
}

# Wait until a TCP port STOPS accepting (a process we killed is really gone).
wait_port_down() {
  local port="$1" secs="${2:-15}" i
  for ((i = 0; i < secs * 10; i++)); do
    if ! (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then return 0; fi
    exec 3>&- 3<&- 2>/dev/null || true
    sleep 0.1
  done
  return 1
}

# --- brokerd ---------------------------------------------------------------

# Build ramqp-brokerd once. Args: optional feature list (e.g. "store-redb").
# Echoes the binary path. Caches per feature set within a run.
BROKERD=""
build_brokerd() {
  local features="${1:-}"
  local flag=() tag="plain"
  if [[ -n "$features" ]]; then flag=(--features "$features"); tag="$features"; fi
  info "building ramqp-brokerd (release, features: ${features:-none})" >&2
  cargo build --release -q -p ramqp-broker --bin ramqp-brokerd "${flag[@]}" >&2
  BROKERD="$ROOT/target/release/ramqp-brokerd"
  [[ -x "$BROKERD" ]] || { err "brokerd not built at $BROKERD"; return 1; }
  echo "$BROKERD"
}

# Spawn a brokerd, tracking its pid and logging to $RAMQP_OUT/<name>.log.
# Usage: spawn_brokerd NAME -- <brokerd args...>   (env is inherited)
# Sets the global BROKERD_PID (do NOT call in $(...) — a subshell would lose the
# tracked pid so the cleanup trap could never reap it).
BROKERD_PID=""
spawn_brokerd() {
  local name="$1"; shift
  [[ "$1" == "--" ]] && shift
  local logf="$RAMQP_OUT/$name.log"
  # Append (not truncate) so a node restarted mid-stage keeps its earlier log —
  # panic evidence from before a restart must not be lost. Names are per-stage,
  # and each run gets a fresh RAMQP_OUT, so there is no cross-run bleed.
  "$BROKERD" "$@" >>"$logf" 2>&1 &
  BROKERD_PID=$!
  track_pid "$BROKERD_PID"
}

# Grep a spawned broker's log for panics/aborts — a broker that logged one is
# unhealthy even if the stage's functional assertions passed. Usage: no_panics NAME...
no_panics() {
  local name rc=0
  for name in "$@"; do
    local logf="$RAMQP_OUT/$name.log"
    [[ -f "$logf" ]] || continue
    if grep -qiE 'panicked|thread .* panicked|RUST_BACKTRACE|assertion failed|fatal runtime' "$logf"; then
      err "$name.log contains a panic/abort:"
      grep -iE 'panicked|assertion failed|fatal runtime' "$logf" | head -5 >&2
      rc=1
    fi
  done
  return $rc
}

# --- sampling --------------------------------------------------------------

# VmRSS of a pid in KiB (empty if the process is gone).
rss_kib() {
  local pid="$1"
  awk '/^VmRSS:/{print $2}' "/proc/$pid/status" 2>/dev/null || true
}

# --- misc ------------------------------------------------------------------

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || { err "required command not found: $1"; return 1; }
}

# Is a docker container running by name?
container_up() {
  docker ps --format '{{.Names}}' 2>/dev/null | grep -qx "$1"
}

cd "$ROOT"
