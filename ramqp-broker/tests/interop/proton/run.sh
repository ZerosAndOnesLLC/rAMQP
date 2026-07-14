#!/usr/bin/env bash
# Apache Qpid Proton (Python) interop against ramqp-broker — the proton leg of
# broker.md Phase 10, alongside the fe2o3-amqp (Rust) and Qpid JMS (Java) legs.
#
#   ramqp-broker/tests/interop/proton/run.sh
#
# Proton needs a native library, so this runs the client one of two ways:
#   - HOST: if `python3 -c "import proton"` works (CI installs python3-qpid-proton
#     via apt), the broker binds loopback and proton runs on the host.
#   - DOCKER fallback: otherwise (e.g. a dev box where we can't touch system
#     packages) the broker binds 0.0.0.0 and proton runs in an ubuntu container
#     that apt-installs python3-qpid-proton and reaches the host via host-gateway.
#
# Requires cargo, and either host python3-qpid-proton or docker.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)" # proton -> interop -> tests -> ramqp-broker -> root
BRK="$ROOT/target/debug/ramqp-brokerd"
SCRIPT="$HERE/proton_interop.py"
ADDRESS="/queues/proton-interop"
declare -a PIDS=()

cleanup() {
    set +e
    for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
    pkill -f "$BRK" 2>/dev/null
}
trap cleanup EXIT

echo ">> building ramqp-brokerd ..."
cargo build -p ramqp-broker --bin ramqp-brokerd >/dev/null

if python3 -c "import proton" >/dev/null 2>&1; then
    echo ">> proton found on host — running host path ..."
    RAMQP_LISTEN=127.0.0.1:5680 "$BRK" >/tmp/ramqp-brokerd-proton.log 2>&1 &
    PIDS+=($!)
    sleep 2
    out="$(python3 "$SCRIPT" amqp://127.0.0.1:5680 "$ADDRESS")"
elif command -v docker >/dev/null 2>&1; then
    echo ">> proton not on host — running docker fallback ..."
    RAMQP_LISTEN=0.0.0.0:5680 "$BRK" >/tmp/ramqp-brokerd-proton.log 2>&1 &
    PIDS+=($!)
    sleep 2
    out="$(docker run --rm --add-host=host.docker.internal:host-gateway \
        -v "$SCRIPT:/proton_interop.py:ro" ubuntu:24.04 \
        bash -c "apt-get update -qq >/dev/null 2>&1 && \
                 DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-qpid-proton >/dev/null 2>&1 && \
                 python3 /proton_interop.py amqp://host.docker.internal:5680 $ADDRESS")"
else
    echo "!! neither host python3-qpid-proton nor docker available — cannot run proton interop" >&2
    exit 2
fi

echo "$out"
case "$out" in
*INTEROP_OK*) echo ">> proton interop PASSED" ;;
*) echo "!! proton interop FAILED" >&2; exit 1 ;;
esac
