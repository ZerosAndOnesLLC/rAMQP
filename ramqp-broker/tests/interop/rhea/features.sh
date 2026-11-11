#!/usr/bin/env bash
# rhea (Node.js) leg of the feature matrix (run by ../matrix/run.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RHEA_VERSION="${RHEA_VERSION:-3.0.5}"
DIR="$MATRIX_WORK/rhea"
mkdir -p "$DIR"
cp "$HERE/features.js" "$DIR/"
( cd "$DIR" && [ -d node_modules/rhea ] || npm install --silent --no-audit --no-fund "rhea@$RHEA_VERSION" >/dev/null )
cd "$DIR" && exec node features.js
