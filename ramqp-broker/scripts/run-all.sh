#!/usr/bin/env bash
# Orchestrator: run the ramqp-broker test battery, one stage per script,
# sharing a single timestamped output directory. NOT wired into CI — this is
# the "run the whole thing before a release" button.
#
#   scripts/run-all.sh                # default battery (gates suite interop robust chaos soak fuzz)
#   scripts/run-all.sh --quick        # gates + suite only (fast gate)
#   scripts/run-all.sh gates suite    # an explicit subset, in order
#
# Perf (bench.sh) and coverage (cov.sh) are deliberately NOT part of this —
# they are manual, and bench must never gate anything.
#
# Duration knobs (env): RAMQP_SOAK_SECS, RAMQP_FUZZ_SECS, RAMQP_CHAOS_ROUNDS.

set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Shared output dir for every stage in this run.
export RAMQP_OUT="${RAMQP_OUT:-$HERE/out/$(date +%Y%m%d-%H%M%S)}"
mkdir -p "$RAMQP_OUT"

DEFAULT_STAGES=(gates suite interop robust chaos soak fuzz)

case "${1:-}" in
  --quick) STAGES=(gates suite) ;;
  --full)  STAGES=("${DEFAULT_STAGES[@]}") ;;
  "")      STAGES=("${DEFAULT_STAGES[@]}") ;;
  *)       STAGES=("$@") ;;
esac

declare -A SCRIPT=(
  [gates]=00-gates.sh
  [suite]=10-suite.sh
  [soak]=20-soak.sh
  [chaos]=30-chaos.sh
  [interop]=40-interop.sh
  [robust]=50-robust.sh
  [fuzz]=60-fuzz.sh
)

RESULTS="$RAMQP_OUT/run-all.summary"
: >"$RESULTS"
rc=0
for stage in "${STAGES[@]}"; do
  script="${SCRIPT[$stage]:-}"
  if [[ -z "$script" ]]; then
    echo "unknown stage: $stage (known: ${!SCRIPT[*]})" >&2
    exit 2
  fi
  printf '\n\033[1m########## stage: %s ##########\033[0m\n' "$stage"
  if bash "$HERE/$script"; then
    echo "PASS  $stage" >>"$RESULTS"
  else
    echo "FAIL  $stage" >>"$RESULTS"
    rc=1
  fi
done

printf '\n\033[1m########## run-all summary ##########\033[0m\n'
cat "$RESULTS"
echo "artifacts: $RAMQP_OUT"
exit $rc
