#!/usr/bin/env bash
# Qpid Proton leg of the feature matrix (run by ../matrix/run.sh): host
# python3-qpid-proton if present, else an ubuntu container on the host
# network (the fixture broker listens on loopback only).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if python3 -c "import proton" >/dev/null 2>&1; then
    exec python3 "$HERE/proton_features.py"
fi
command -v docker >/dev/null || { echo "!! need python3-qpid-proton or docker" >&2; exit 2; }
exec docker run --rm --network host \
    -e MATRIX_HOST -e MATRIX_PLAIN_PORT -e MATRIX_TLS_PORT -e MATRIX_USER -e MATRIX_PASS \
    -e MATRIX_CA=/matrix/ca.crt \
    -v "$MATRIX_CA:/matrix/ca.crt:ro" -v "$HERE/proton_features.py:/proton_features.py:ro" \
    ubuntu:24.04 bash -c "apt-get update -qq >/dev/null 2>&1 && \
        DEBIAN_FRONTEND=noninteractive apt-get install -y -qq python3-qpid-proton >/dev/null 2>&1 && \
        python3 /proton_features.py"
