#!/usr/bin/env bash
# Pre-release QA for ramqp, ramqp-core and ramqp-broker.
#
#   qa/prerelease.sh                 # quick profile (~30-40 min), every stage but partition
#   qa/prerelease.sh --full          # release profile (~2-3 h): longer fuzz/soak/chaos + partition
#   qa/prerelease.sh --only bench,diff
#   qa/prerelease.sh --skip fuzz,interop
#   qa/prerelease.sh --update-baseline   # record this machine's benchmark baseline
#   qa/prerelease.sh --list
#
# Stages (in order):
#   gates       fmt, clippy (all feature sets), docs, every test suite, publish dry-run
#   fuzz        the ramqp-core cargo-fuzz targets, time-boxed (nightly + cargo-fuzz)
#   interop     5-client-stack matrix, Qpid JMS + Proton legs, fe2o3 leg, and the
#               ramqp client against RabbitMQ 4.x, Artemis and ramqp-brokerd
#   diff        the same protocol scenarios against ramqp-brokerd, RabbitMQ 4.x and
#               Artemis; unexpected behavioural differences fail (qa/known-differences.json)
#   guarantees  workloads with consumer churn on every queue kind, direct and through
#               a frame-coalescing and a connection-resetting proxy; checks no loss,
#               no redelivery after accept, per-producer order
#   chaos       kill -9 the broker repeatedly under load (durable, quorum, transient);
#               no acknowledged message on a durable/quorum queue may be lost
#   bench       throughput + latency scenarios vs this machine's baseline
#               (qa/baselines/<host>.json); regressions fail
#   soak        mixed long-running load with broker memory / fd / thread sampling;
#               growth after warm-up fails
#   partition   netns split-brain test for the cluster (needs sudo; --full only)
#
# Every duration can be overridden from the environment (see PROFILE below).
# Results: target/prerelease/<timestamp>/ (one log per stage + summary.txt).
# Exit status: 0 if no stage failed.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
export CARGO_INCREMENTAL="${CARGO_INCREMENTAL:-0}" # keeps target/ from ballooning

ALL_STAGES=(gates fuzz interop diff guarantees chaos bench soak partition)
PROFILE=quick
ONLY=""
SKIP=""
UPDATE_BASELINE=0

while [ $# -gt 0 ]; do
    case "$1" in
        --quick) PROFILE=quick ;;
        --full) PROFILE=full ;;
        --only) ONLY="$2"; shift ;;
        --skip) SKIP="$2"; shift ;;
        --update-baseline) UPDATE_BASELINE=1 ;;
        --list) printf '%s\n' "${ALL_STAGES[@]}"; exit 0 ;;
        -h|--help) sed -n '2,32p' "$0"; exit 0 ;;
        *) echo "unknown argument: $1" >&2; exit 2 ;;
    esac
    shift
done

# ---------------------------------------------------------------- profile
if [ "$PROFILE" = full ]; then
    : "${QA_FUZZ_SECS:=600}" "${QA_GUARANTEE_SECS:=120}" "${QA_CHAOS_KILLS:=10}"
    : "${QA_SOAK_SECS:=3600}" "${QA_BENCH_MESSAGES:=300000}" "${QA_PARTITION:=1}"
else
    : "${QA_FUZZ_SECS:=60}" "${QA_GUARANTEE_SECS:=20}" "${QA_CHAOS_KILLS:=3}"
    : "${QA_SOAK_SECS:=300}" "${QA_BENCH_MESSAGES:=100000}" "${QA_PARTITION:=0}"
fi
: "${QA_CHAOS_INTERVAL:=6}"           # seconds between broker kills
: "${QA_PORT_BASE:=5750}"             # ramqp-brokerd / proxy ports start here
: "${QA_RABBIT_CONTAINER:=ramqp-rabbit}" "${QA_RABBIT_PORT:=5672}" "${QA_RABBIT_MGMT:=15672}"
: "${QA_ARTEMIS_CONTAINER:=ramqp-artemis}" "${QA_ARTEMIS_PORT:=5673}"
: "${QA_MAX_THROUGHPUT_DROP:=15}" "${QA_MAX_P99_RISE:=50}"

STAMP="$(date +%Y%m%d-%H%M%S)"
OUT="$ROOT/target/prerelease/$STAMP"
mkdir -p "$OUT"
QA="$ROOT/target/release/ramqp-qa"
BRK="$ROOT/target/release/ramqp-brokerd"
HOST="$(hostname -s 2>/dev/null || hostname)"
BASELINE="$ROOT/qa/baselines/$HOST.json"

PIDS=()
STARTED_CONTAINERS=()
cleanup() {
    for p in "${PIDS[@]:-}"; do
        [ -n "$p" ] && kill "$p" 2>/dev/null
    done
    for c in "${STARTED_CONTAINERS[@]:-}"; do
        [ -n "$c" ] && docker stop "$c" >/dev/null 2>&1
    done
}
trap cleanup EXIT
trap 'echo; echo "interrupted"; exit 130' INT TERM

# ---------------------------------------------------------------- helpers
log() { echo "[$(date +%H:%M:%S)] $*"; }

# `skip "<reason>"` from inside a stage: exit status 2 + the reason.
SKIP_REASON_FILE="$OUT/.skip"
skip() { echo "$1" > "$SKIP_REASON_FILE"; return 2; }

need() {
    local missing=()
    for t in "$@"; do command -v "$t" >/dev/null 2>&1 || missing+=("$t"); done
    [ ${#missing[@]} -eq 0 ] || { skip "missing: ${missing[*]}"; return 2; }
}

wait_port() { # host port secs
    local deadline=$((SECONDS + $3))
    while [ $SECONDS -lt $deadline ]; do
        (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null && return 0
        sleep 0.2
    done
    return 1
}

# start_brokerd <tag> <port> [data_dir] [mgmt_port] -> sets BROKER_PID
start_brokerd() {
    local tag=$1 port=$2 data=${3:-} mgmt=${4:-}
    local args=(--listen "127.0.0.1:$port" --allow-anonymous)
    [ -n "$data" ] && args+=(--data-dir "$data")
    [ -n "$mgmt" ] && args+=(--management-listen "127.0.0.1:$mgmt")
    "$BRK" "${args[@]}" >>"$OUT/brokerd-$tag.log" 2>&1 &
    BROKER_PID=$!
    PIDS+=("$BROKER_PID")
    wait_port 127.0.0.1 "$port" 30 || { echo "ramqp-brokerd ($tag) did not start"; return 1; }
}

stop_pid() { kill "$1" 2>/dev/null; wait "$1" 2>/dev/null; true; }

# start_proxy <port> <target_port> <proxy args...> -> sets PROXY_PID
start_proxy() {
    local port=$1 target=$2; shift 2
    "$QA" proxy --listen "127.0.0.1:$port" --target "127.0.0.1:$target" "$@" \
        >>"$OUT/proxy-$port.log" 2>&1 &
    PROXY_PID=$!
    PIDS+=("$PROXY_PID")
    wait_port 127.0.0.1 "$port" 10
}

# workload <label> <args...>: runs ramqp-qa workload, keeps its JSON summary.
workload() {
    local label=$1; shift
    log "workload $label"
    if "$QA" workload "$@" --json "$OUT/workload-$label.json" >"$OUT/workload-$label.out" 2>&1; then
        log "  pass: $(jq -c '{acknowledged,consumer_exits,lost,redelivered_after_accept,order_violations}' "$OUT/workload-$label.json")"
        return 0
    fi
    log "  FAIL: $(jq -c '.failures' "$OUT/workload-$label.json" 2>/dev/null || tail -3 "$OUT/workload-$label.out")"
    return 1
}

ensure_rabbit() {
    command -v docker >/dev/null || return 1
    if ! docker inspect "$QA_RABBIT_CONTAINER" >/dev/null 2>&1; then
        docker run -d --name "$QA_RABBIT_CONTAINER" -p "$QA_RABBIT_PORT:5672" \
            -p "$QA_RABBIT_MGMT:15672" rabbitmq:4-management >/dev/null || return 1
        STARTED_CONTAINERS+=("$QA_RABBIT_CONTAINER")
    elif [ "$(docker inspect -f '{{.State.Running}}' "$QA_RABBIT_CONTAINER")" != true ]; then
        docker start "$QA_RABBIT_CONTAINER" >/dev/null || return 1
        STARTED_CONTAINERS+=("$QA_RABBIT_CONTAINER")
    fi
    local deadline=$((SECONDS + 120))
    until docker exec "$QA_RABBIT_CONTAINER" rabbitmq-diagnostics -q check_port_connectivity >/dev/null 2>&1; do
        [ $SECONDS -lt $deadline ] || return 1
        sleep 2
    done
    until curl -sf -u guest:guest "http://localhost:$QA_RABBIT_MGMT/api/overview" >/dev/null; do
        [ $SECONDS -lt $deadline ] || return 1
        sleep 2
    done
}

rabbit_fresh_queue() { # name
    local q
    q=$(printf %s "$1" | jq -sRr @uri)
    curl -s -u guest:guest -X DELETE "http://localhost:$QA_RABBIT_MGMT/api/queues/%2F/$q" >/dev/null
    curl -sf -u guest:guest -X PUT "http://localhost:$QA_RABBIT_MGMT/api/queues/%2F/$q" \
        -H content-type:application/json -d '{"durable":true}' >/dev/null
}

ensure_artemis() {
    command -v docker >/dev/null || return 1
    if ! docker inspect "$QA_ARTEMIS_CONTAINER" >/dev/null 2>&1; then
        docker run -d --name "$QA_ARTEMIS_CONTAINER" -p "$QA_ARTEMIS_PORT:5672" -p 61616:61616 \
            -e ARTEMIS_USER=admin -e ARTEMIS_PASSWORD=admin \
            apache/activemq-artemis:latest-alpine >/dev/null || return 1
        STARTED_CONTAINERS+=("$QA_ARTEMIS_CONTAINER")
    elif [ "$(docker inspect -f '{{.State.Running}}' "$QA_ARTEMIS_CONTAINER")" != true ]; then
        docker start "$QA_ARTEMIS_CONTAINER" >/dev/null || return 1
        STARTED_CONTAINERS+=("$QA_ARTEMIS_CONTAINER")
    fi
    local deadline=$((SECONDS + 120))
    until artemis_cli queue stat --silent >/dev/null 2>&1; do
        [ $SECONDS -lt $deadline ] || return 1
        sleep 2
    done
}

artemis_cli() {
    docker exec "$QA_ARTEMIS_CONTAINER" /var/lib/artemis-instance/bin/artemis "$@" \
        --url tcp://localhost:61616 --user admin --password admin
}

artemis_fresh_queue() { # name: an ANYCAST queue (auto-created ones are MULTICAST)
    artemis_cli queue delete --name "$1" --silent >/dev/null 2>&1
    artemis_cli queue create --name "$1" --address "$1" --anycast --durable \
        --preserve-on-no-consumers --auto-create-address --silent >/dev/null 2>&1
}

# ---------------------------------------------------------------- stages
stage_gates() {
    local fail=0
    run() { log "\$ $*"; "$@" || { log "  FAILED: $*"; fail=1; }; }
    run cargo fmt --all -- --check
    run cargo clippy --workspace --all-targets --all-features -- -D warnings
    run cargo clippy --all-targets -- -D warnings
    run cargo clippy -p ramqp-broker --all-targets --features tls -- -D warnings
    if command -v cmake >/dev/null && command -v go >/dev/null; then
        run cargo clippy -p ramqp-broker --all-targets --no-default-features \
            --features fips,jemalloc,store-redb -- -D warnings
    else
        log "FIPS clippy/tests skipped: need cmake and go"
    fi
    run env RUSTDOCFLAGS="-D warnings" cargo doc --all-features --no-deps
    run cargo test --workspace --all-features
    run cargo test --workspace
    run cargo test -p ramqp-core --no-default-features --features scram-fips
    if command -v cmake >/dev/null && command -v go >/dev/null; then
        run cargo test -p ramqp-broker --no-default-features --features fips,jemalloc,store-redb
    fi
    run cargo test -p ramqp-bench-compare
    run cargo publish -p ramqp-core -p ramqp -p ramqp-broker --dry-run --allow-dirty
    return $fail
}

stage_fuzz() {
    need cargo-fuzz || return 2
    rustup toolchain list | grep -q '^nightly' || { skip "no nightly toolchain"; return 2; }
    local fail=0
    for t in $(cd ramqp-core/fuzz && cargo +nightly fuzz list 2>/dev/null); do
        log "fuzz $t for ${QA_FUZZ_SECS}s"
        if ! (cd ramqp-core && cargo +nightly fuzz run "$t" -- \
                -max_total_time="$QA_FUZZ_SECS" -rss_limit_mb=2048 -print_final_stats=1); then
            log "  CRASH: artifacts in ramqp-core/fuzz/artifacts/$t"
            fail=1
        fi
    done
    return $fail
}

stage_interop() {
    need java node go docker openssl || return 2
    local fail=0
    log "client-stack matrix (Qpid JMS, Proton, rhea, go-amqp, RabbitMQ Go)"
    ramqp-broker/tests/interop/matrix/run.sh || fail=1
    log "Qpid JMS interop"
    ramqp-broker/tests/interop/jms/run.sh || fail=1
    log "Qpid Proton interop"
    ramqp-broker/tests/interop/proton/run.sh || fail=1

    # The ramqp client against three brokers.
    local port=$((QA_PORT_BASE + 1))
    start_brokerd interop "$port" || return 1
    log "ramqp client vs ramqp-brokerd"
    RAMQP_BROKER_URL="amqp://127.0.0.1:$port" RAMQP_BROKER_ADDRESS=/queues/interop \
        RAMQP_BROKER_REFUSED_ADDRESS=/durable/nope \
        cargo test -p ramqp --test broker -- --ignored --test-threads=1 || fail=1
    stop_pid "$BROKER_PID"
    if ensure_rabbit; then
        rabbit_fresh_queue ramqp_it
        log "ramqp client vs RabbitMQ 4.x"
        RAMQP_BROKER_URL="amqp://guest:guest@localhost:$QA_RABBIT_PORT" \
            RAMQP_BROKER_ADDRESS=/queues/ramqp_it \
            RAMQP_BROKER_REFUSED_ADDRESS=/queues/qa-never-declared \
            cargo test -p ramqp --test broker -- --ignored --test-threads=1 || fail=1
    else
        log "RabbitMQ unavailable: FAIL"; fail=1
    fi
    if ensure_artemis; then
        artemis_fresh_queue ramqp.it
        log "ramqp client vs ActiveMQ Artemis"
        RAMQP_BROKER_URL="amqp://admin:admin@localhost:$QA_ARTEMIS_PORT" \
            RAMQP_BROKER_ADDRESS=ramqp.it \
            cargo test -p ramqp --test broker -- --ignored --test-threads=1 || fail=1
    else
        log "Artemis unavailable: FAIL"; fail=1
    fi
    return $fail
}

stage_diff() {
    need docker jq curl || return 2
    local port=$((QA_PORT_BASE + 2))
    start_brokerd diff "$port" || return 1
    local targets=(--target "ramqp=amqp://127.0.0.1:$port,/queues/")
    if ensure_rabbit; then
        for q in $("$QA" diff --list-queues); do rabbit_fresh_queue "$q"; done
        targets+=(--target "rabbitmq=amqp://guest:guest@localhost:$QA_RABBIT_PORT,/queues/")
    else
        log "RabbitMQ unavailable"; stop_pid "$BROKER_PID"; return 1
    fi
    if ensure_artemis; then
        for q in $("$QA" diff --list-queues); do artemis_fresh_queue "$q"; done
        targets+=(--target "artemis=amqp://admin:admin@localhost:$QA_ARTEMIS_PORT,")
    else
        log "Artemis unavailable: comparing against RabbitMQ only"
    fi
    "$QA" diff "${targets[@]}" --known qa/known-differences.json --json "$OUT/diff.json"
    local rc=$?
    stop_pid "$BROKER_PID"
    return $rc
}

stage_guarantees() {
    need jq || return 2
    local port=$((QA_PORT_BASE + 3)) coalesce=$((QA_PORT_BASE + 13)) resets=$((QA_PORT_BASE + 14))
    local data="$OUT/guarantees-data"
    mkdir -p "$data"
    start_brokerd guarantees "$port" "$data" || return 1
    local bp=$BROKER_PID
    start_proxy "$coalesce" "$port" --coalesce-ms 50 || return 1
    local p1=$PROXY_PID
    start_proxy "$resets" "$port" --reset-every-ms 1500 --delay-ms 2 --jitter-ms 5 || return 1
    local p2=$PROXY_PID
    local fail=0 s=$QA_GUARANTEE_SECS
    local common=(--producers 3 --consumers 3 --secs "$s")
    for kind in queues durable quorum; do
        local q="/$kind/qa.g.$kind"
        # Steady consumers: order is checked too.
        workload "$kind-steady" --url "amqp://127.0.0.1:$port" --address "$q.steady" \
            "${common[@]}" --release-pct 3 || fail=1
        # Consumers leaving every way a real one does (issue #50's class).
        workload "$kind-churn" --url "amqp://127.0.0.1:$port" --address "$q.churn" \
            "${common[@]}" --exit-every 25 --release-pct 3 || fail=1
        # The same, with frames coalesced into one read (kubectl port-forward).
        workload "$kind-coalesced" --url "amqp://127.0.0.1:$coalesce" --address "$q.coalesced" \
            "${common[@]}" --exit-every 25 || fail=1
        # Connections cut mid-flight: an accept can die with them, so a
        # redelivery after accept is legitimate here — loss never is.
        workload "$kind-resets" --url "amqp://127.0.0.1:$resets" --address "$q.resets" \
            --drain-url "amqp://127.0.0.1:$port" "${common[@]}" --exit-every 50 \
            --allow-dup-after-accept || fail=1
    done
    stop_pid "$p1"; stop_pid "$p2"; stop_pid "$bp"
    return $fail
}

stage_chaos() {
    need jq || return 2
    local port=$((QA_PORT_BASE + 4)) data="$OUT/chaos-data"
    mkdir -p "$data"
    start_brokerd chaos "$port" "$data" || return 1
    local secs=$((QA_CHAOS_KILLS * QA_CHAOS_INTERVAL + QA_CHAOS_INTERVAL))
    local args=(--url "amqp://127.0.0.1:$port" --producers 3 --consumers 3 --secs "$secs"
        --exit-every 40 --allow-dup-after-accept --no-order-check --drain-connect-secs 120)
    log "load on durable, quorum and transient queues; kill -9 every ${QA_CHAOS_INTERVAL}s, ${QA_CHAOS_KILLS} times"
    "$QA" workload "${args[@]}" --address /durable/qa.chaos --json "$OUT/workload-chaos-durable.json" \
        >"$OUT/workload-chaos-durable.out" 2>&1 &
    local w1=$!
    "$QA" workload "${args[@]}" --address /quorum/qa.chaos.q --json "$OUT/workload-chaos-quorum.json" \
        >"$OUT/workload-chaos-quorum.out" 2>&1 &
    local w2=$!
    # Transient messages die with the broker: only "no hang, no crash" here.
    "$QA" workload "${args[@]}" --address /queues/qa.chaos.t --allow-loss \
        --json "$OUT/workload-chaos-transient.json" >"$OUT/workload-chaos-transient.out" 2>&1 &
    local w3=$!
    PIDS+=("$w1" "$w2" "$w3")
    for i in $(seq 1 "$QA_CHAOS_KILLS"); do
        sleep "$QA_CHAOS_INTERVAL"
        log "kill -9 #$i"
        kill -9 "$BROKER_PID" 2>/dev/null; wait "$BROKER_PID" 2>/dev/null
        sleep 1
        start_brokerd chaos "$port" "$data" || { log "broker did not restart"; return 1; }
    done
    local fail=0
    for w in "$w1:durable" "$w2:quorum" "$w3:transient"; do
        local pid=${w%%:*} kind=${w##*:}
        if wait "$pid"; then
            log "  $kind: pass $(jq -c '{acknowledged,lost,redelivered_after_accept,connection_errors}' "$OUT/workload-chaos-$kind.json")"
        else
            log "  $kind: FAIL $(jq -c '.failures' "$OUT/workload-chaos-$kind.json" 2>/dev/null)"
            fail=1
        fi
    done
    stop_pid "$BROKER_PID"
    return $fail
}

stage_bench() {
    need jq || return 2
    local port=$((QA_PORT_BASE + 5)) data="$OUT/bench-data" n=$QA_BENCH_MESSAGES
    mkdir -p "$data"
    start_brokerd bench "$port" "$data" || return 1
    local url="amqp://127.0.0.1:$port" results=() fail=0
    # name | address kind | args
    local scenarios=(
        "transient-256B-1p1c|queues|--size 256 --messages $n"
        "transient-256B-4p4c|queues|--size 256 --messages $n --producers 4 --consumers 4"
        "transient-64KiB-1p1c|queues|--size 65536 --messages $((n / 20))"
        "transient-256B-rate10k|queues|--size 256 --messages $((n / 5)) --rate 10000"
        "durable-256B-1p1c|durable|--size 256 --messages $((n / 2))"
        "quorum-256B-1p1c|quorum|--size 256 --messages $((n / 2))"
    )
    for sc in "${scenarios[@]}"; do
        IFS='|' read -r name kind args <<<"$sc"
        log "bench $name"
        # shellcheck disable=SC2086
        if "$QA" bench --name "$name" --url "$url" --address "/$kind/qa.bench.$name" $args \
                --json "$OUT/bench-$name.json" >"$OUT/bench-$name.out" 2>&1; then
            results+=("$OUT/bench-$name.json")
            log "  $(jq -c '{msgs_per_sec: (.msgs_per_sec|floor), p50: .latency_us.p50, p99: .latency_us.p99}' "$OUT/bench-$name.json")"
        else
            log "  FAILED: $(tail -2 "$OUT/bench-$name.out")"
            fail=1
        fi
    done
    stop_pid "$BROKER_PID"
    [ ${#results[@]} -gt 0 ] || return 1
    jq -s . "${results[@]}" >"$OUT/bench.json"
    if [ "$UPDATE_BASELINE" = 1 ] || [ ! -f "$BASELINE" ]; then
        mkdir -p "$(dirname "$BASELINE")"
        cp "$OUT/bench.json" "$BASELINE"
        log "baseline written: $BASELINE (commit it to track this machine)"
        return $fail
    fi
    "$QA" compare --baseline "$BASELINE" --current "$OUT/bench.json" \
        --max-throughput-drop "$QA_MAX_THROUGHPUT_DROP" --max-p99-rise "$QA_MAX_P99_RISE" || fail=1
    return $fail
}

stage_soak() {
    need jq || return 2
    local port=$((QA_PORT_BASE + 6)) data="$OUT/soak-data"
    mkdir -p "$data"
    start_brokerd soak "$port" "$data" || return 1
    local load=$((QA_SOAK_SECS - 15)) url="amqp://127.0.0.1:$port"
    [ "$load" -gt 30 ] || { skip "QA_SOAK_SECS too short"; return 2; }
    log "soak ${QA_SOAK_SECS}s: transient, durable and quorum load with consumer churn"
    "$QA" resources --pid "$BROKER_PID" --secs "$QA_SOAK_SECS" --interval-ms 2000 \
        --csv "$OUT/soak-resources.csv" --json "$OUT/soak-resources.json" >"$OUT/soak-resources.out" 2>&1 &
    local r=$!
    PIDS+=("$r")
    local w=()
    for kind in queues durable quorum; do
        "$QA" workload --url "$url" --address "/$kind/qa.soak.$kind" --producers 2 --consumers 2 \
            --secs "$load" --size 1024 --rate 300 --exit-every 200 --release-pct 2 \
            --json "$OUT/workload-soak-$kind.json" >"$OUT/workload-soak-$kind.out" 2>&1 &
        w+=("$!:$kind")
        PIDS+=("$!")
    done
    local fail=0
    for x in "${w[@]}"; do
        local pid=${x%%:*} kind=${x##*:}
        if wait "$pid"; then
            log "  $kind workload: pass"
        else
            log "  $kind workload: FAIL $(jq -c '.failures' "$OUT/workload-soak-$kind.json" 2>/dev/null)"
            fail=1
        fi
    done
    if wait "$r"; then
        log "  broker resources: pass $(jq -c '{rss_kib_low_first_half,rss_kib_low_second_half,rss_growth_pct,rss_kib_peak,fds_after_warmup,fds_end}' "$OUT/soak-resources.json")"
    else
        log "  broker resources: FAIL $(jq -c '.failures' "$OUT/soak-resources.json" 2>/dev/null)"
        fail=1
    fi
    stop_pid "$BROKER_PID"
    return $fail
}

stage_partition() {
    [ "$QA_PARTITION" = 1 ] || { skip "quick profile (use --full or QA_PARTITION=1)"; return 2; }
    sudo -n true 2>/dev/null || { skip "needs passwordless sudo (netns + iptables)"; return 2; }
    ramqp-broker/tests/partition/run.sh
}

# ---------------------------------------------------------------- driver
selected() {
    local s=$1
    if [ -n "$ONLY" ]; then [[ ",$ONLY," == *",$s,"* ]] || return 1; fi
    if [ -n "$SKIP" ]; then [[ ",$SKIP," == *",$s,"* ]] && return 1; fi
    return 0
}

log "pre-release QA ($PROFILE profile) → $OUT"
log "building release binaries (ramqp-qa, ramqp-brokerd with store-redb)"
if ! cargo build --release -p ramqp-qa -p ramqp-broker --features ramqp-broker/store-redb \
        >"$OUT/build.log" 2>&1; then
    tail -20 "$OUT/build.log"
    echo "build failed"
    exit 1
fi

declare -A STATUS DURATION NOTE
for stage in "${ALL_STAGES[@]}"; do
    selected "$stage" || continue
    rm -f "$SKIP_REASON_FILE"
    log "=== $stage"
    t0=$SECONDS
    "stage_$stage" >"$OUT/$stage.log" 2>&1
    rc=$?
    DURATION[$stage]=$((SECONDS - t0))
    case $rc in
        0) STATUS[$stage]=PASS ;;
        2) STATUS[$stage]=SKIP; NOTE[$stage]="$(cat "$SKIP_REASON_FILE" 2>/dev/null)" ;;
        *) STATUS[$stage]=FAIL; NOTE[$stage]="see $OUT/$stage.log" ;;
    esac
    log "    ${STATUS[$stage]} (${DURATION[$stage]}s) ${NOTE[$stage]:-}"
done

{
    echo "pre-release QA, $PROFILE profile, $(date -Is), $(git rev-parse --short HEAD)$(git diff --quiet || echo '+dirty')"
    printf '%-11s %-5s %7s  %s\n' stage result secs note
    for stage in "${ALL_STAGES[@]}"; do
        [ -n "${STATUS[$stage]:-}" ] || continue
        printf '%-11s %-5s %7s  %s\n' "$stage" "${STATUS[$stage]}" "${DURATION[$stage]}" "${NOTE[$stage]:-}"
    done
} | tee "$OUT/summary.txt"

for stage in "${!STATUS[@]}"; do
    [ "${STATUS[$stage]}" = FAIL ] && exit 1
done
exit 0
