#!/usr/bin/env bash
# Azure go-amqp (Go) leg of the feature matrix (run by ../matrix/run.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$HERE" && exec go run .
