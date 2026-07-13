#!/usr/bin/env bash
# Tuned-incumbent comparison for broker.md §3.4 / Phase 10: re-run the closed-
# loop latency bench against a *tuned* RabbitMQ (rabbitmq.conf here) instead of
# stock defaults, plus a durability-parity quorum leg (our store-redb quorum vs
# RabbitMQ's fsync-backed quorum queue).
#
# Fairness note: unlike the original Phase 4 table (ours in-process vs RabbitMQ
# in docker), EVERY leg here runs over loopback TCP against a broker PROCESS, so
# the transport path is identical for all rows.
#
#   bench-compare/tuned/run.sh
#
# Emits a Markdown table to stdout and to $OUT (default bench-compare/tuned/
# results.md). Requires docker + cargo. Numbers from a shared/virtualized box
# (e.g. WSL2) are INDICATIVE ONLY — see the caveat the script prints.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"
OUT="${OUT:-$HERE/results.md}"
LAT_N="${LAT_N:-20000}"
RABBIT_IMAGE="${RABBIT_IMAGE:-rabbitmq:4-management}"
BRK="$ROOT/target/release/ramqp-brokerd"
LAT="$ROOT/target/release/latency"

OURS_PORT=5680
OURS_Q_PORT=5681
# Non-default host ports so a pre-existing dev `rabbit` on 5672/15672 is untouched.
RABBIT_PORT="${RABBIT_PORT:-5673}"
RABBIT_MGMT_PORT="${RABBIT_MGMT_PORT:-15673}"
declare -a PIDS=()
DATA_DIR="$(mktemp -d)"

cleanup() {
    set +e
    for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
    pkill -f "$BRK" 2>/dev/null
    docker rm -f rabbit-tuned >/dev/null 2>&1
    rm -rf "$DATA_DIR"
}
trap cleanup EXIT

echo ">> building release bench + brokerd (store-redb) ..."
cargo build -p ramqp-bench-compare --release --bin latency >/dev/null
cargo build -p ramqp-broker --release --bin ramqp-brokerd --features store-redb >/dev/null

# --- tuned RabbitMQ ------------------------------------------------------
echo ">> starting tuned RabbitMQ ($RABBIT_IMAGE) ..."
docker rm -f rabbit-tuned >/dev/null 2>&1 || true
docker run -d --name rabbit-tuned \
    -p $RABBIT_PORT:5672 -p $RABBIT_MGMT_PORT:15672 \
    -v "$HERE/rabbitmq.conf:/etc/rabbitmq/conf.d/10-tuned.conf:ro" \
    "$RABBIT_IMAGE" >/dev/null
echo ">> waiting for RabbitMQ management ..."
for _ in $(seq 1 60); do
    if curl -fsS -u guest:guest http://localhost:$RABBIT_MGMT_PORT/api/overview >/dev/null 2>&1; then break; fi
    sleep 2
done
curl -fsS -u guest:guest -X PUT http://localhost:$RABBIT_MGMT_PORT/api/queues/%2F/rq_classic \
    -H content-type:application/json -d '{"durable":true}' >/dev/null
curl -fsS -u guest:guest -X PUT http://localhost:$RABBIT_MGMT_PORT/api/queues/%2F/rq_quorum \
    -H content-type:application/json -d '{"durable":true,"arguments":{"x-queue-type":"quorum"}}' >/dev/null
echo ">> RabbitMQ queues declared (rq_classic, rq_quorum)."

# --- our broker: transient + durable single-node quorum ------------------
echo ">> starting ramqp-brokerd (transient) on :$OURS_PORT ..."
RAMQP_LISTEN=127.0.0.1:$OURS_PORT "$BRK" >"$DATA_DIR/ours.log" 2>&1 &
PIDS+=($!)
echo ">> starting ramqp-brokerd (durable single-node quorum, store-redb) on :$OURS_Q_PORT ..."
RAMQP_LISTEN=127.0.0.1:$OURS_Q_PORT \
    RAMQP_NODE_ID=1 RAMQP_CLUSTER_LISTEN=127.0.0.1:7481 RAMQP_SEEDS=1=127.0.0.1:7481 \
    RAMQP_DATA_DIR="$DATA_DIR/redb" "$BRK" >"$DATA_DIR/ours-q.log" 2>&1 &
PIDS+=($!)
sleep 5

# --- run one latency leg, extract p50/p99/p99.9 --------------------------
# usage: leg <label> <url|-> <address>   ('-' url means in-process, unused here)
run_leg() {
    local label="$1" url="$2" addr="$3" line
    if line="$(LAT_URL="$url" LAT_ADDRESS="$addr" LAT_N="$LAT_N" "$LAT" 2>/dev/null \
        | grep -E 'latency .* p50')"; then
        local p50 p99 p999
        p50="$(echo "$line" | sed -n 's/.*p50 \([0-9.]*\).*/\1/p')"
        p99="$(echo "$line" | sed -n 's/.*p99 \([0-9.]*\).*p99\.9.*/\1/p')"
        p999="$(echo "$line" | sed -n 's/.*p99\.9 \([0-9.]*\).*/\1/p')"
        printf '| %-34s | %8s | %8s | %8s |\n' "$label" "${p50:-?}" "${p99:-?}" "${p999:-?}"
    else
        printf '| %-34s | %8s | %8s | %8s |\n' "$label" "n/a" "n/a" "n/a"
    fi
}

echo ">> running latency legs ($LAT_N samples each) ..."
{
    echo "## Tuned-incumbent latency (provisional)"
    echo
    echo "Closed-loop e2e latency, µs — all legs over loopback TCP, $LAT_N samples."
    echo
    echo "| leg | p50 | p99 | p99.9 |"
    echo "|---|--:|--:|--:|"
    run_leg "ramqp-broker transient"            "amqp://127.0.0.1:$OURS_PORT"   "/queues/bench"
    run_leg "RabbitMQ 4.x classic (tuned)"      "amqp://guest:guest@127.0.0.1:$RABBIT_PORT" "/queues/rq_classic"
    run_leg "ramqp-broker quorum (store-redb)"  "amqp://127.0.0.1:$OURS_Q_PORT" "/quorum/bench"
    run_leg "RabbitMQ 4.x quorum (fsync)"       "amqp://guest:guest@127.0.0.1:$RABBIT_PORT" "/queues/rq_quorum"
} | tee "$OUT"

echo
echo ">> results written to $OUT"
echo "!! PROVISIONAL: numbers from a shared/virtualized box are indicative only;"
echo "!! the defend-forever figures come from quiet bare metal (same script)."
