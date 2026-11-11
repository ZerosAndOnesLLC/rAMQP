#!/usr/bin/env bash
# RabbitMQ AMQP 1.0 Go client leg of the feature matrix (../matrix/run.sh).
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# The client logs at INFO by default; keep the leg's output to its verdicts.
cd "$HERE" && go run . 2> >(grep -vE ' (INFO|DEBUG|WARN) ' >&2)
