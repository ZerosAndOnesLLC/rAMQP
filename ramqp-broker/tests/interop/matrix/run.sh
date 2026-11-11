#!/usr/bin/env bash
# Feature interop matrix: independent AMQP 1.0 client stacks against a
# ramqp-broker configured the way a RabbitMQ 4.x replacement is — user
# logins (PLAIN + SCRAM against stored verifiers), RabbitMQ 4.x exchange
# addresses, and `amqps` — not just anonymous round trips.
#
#   ramqp-broker/tests/interop/matrix/run.sh [leg ...]   # default: all legs
#
# Legs: jms (Qpid JMS, Java), proton (Qpid Proton, Python/C), rhea (Node.js),
# goamqp (Azure go-amqp, Go), rabbitmq-go (RabbitMQ's own AMQP 1.0 Go
# client — publish/consume work; its topology management gets a clear 501).
# The Rust fe2o3-amqp leg runs in-process
# (bench-compare/tests/fe2o3_features.rs). Each leg runs the same scenarios:
#   1. login + round trip           4. exchange, key from properties.subject
#   2. wrong password refused       5. unknown exchange refused
#   3. exchange, key in the address 6. amqps (private CA, hostname checked)
#   (+ SCRAM-SHA-256 where the client supports it)
#
# Requires cargo + openssl; per leg: java/javac/keytool + curl (jms),
# python3-qpid-proton or docker (proton), node/npm (rhea), go (goamqp,
# rabbitmq-go).
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
INTEROP="$(cd "$HERE/.." && pwd)"
ROOT="$(cd "$HERE/../../../.." && pwd)"
WORK="${MATRIX_WORK:-$ROOT/target/interop/matrix}"
BRK="$ROOT/target/debug/ramqp-brokerd"
PLAIN_PORT="${MATRIX_PLAIN_PORT:-5700}"
TLS_PORT="${MATRIX_TLS_PORT:-5701}"
LEGS=("$@")
[ ${#LEGS[@]} -eq 0 ] && LEGS=(jms proton rhea goamqp rabbitmq-go)

BROKER_PID=""
cleanup() {
    set +e
    [ -n "$BROKER_PID" ] && kill "$BROKER_PID" 2>/dev/null && wait "$BROKER_PID" 2>/dev/null
}
trap cleanup EXIT

rm -rf "$WORK" && mkdir -p "$WORK/pki"
echo ">> building ramqp-brokerd (tls) ..."
cargo build -q -p ramqp-broker --features tls --bin ramqp-brokerd

echo ">> minting a throwaway CA + localhost certificate ..."
(
    cd "$WORK/pki"
    openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout ca.key -out ca.crt -days 2 -subj /CN=matrix-ca 2>/dev/null
    openssl req -newkey ec -pkeyopt ec_paramgen_curve:P-256 -nodes \
        -keyout server.key -out server.csr -subj /CN=localhost 2>/dev/null
    printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n' >ext.cnf
    openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
        -out server.crt -days 2 -extfile ext.cnf 2>/dev/null
)

echo ">> writing the fixture config ..."
printf 'interop-pw\n' >"$WORK/interop.pw"
# A second user with ONLY a SHA-256 verifier: the broker must then offer
# SCRAM-SHA-256 (not 512) so strongest-first clients still log everyone in.
echo "only256-pw" | "$BRK" hash-password --mechanism SCRAM-SHA-256 >"$WORK/only256.scram"
{
    cat <<CONF
listen = "127.0.0.1:$PLAIN_PORT"

[tls]
listen = "127.0.0.1:$TLS_PORT"
cert_file = "pki/server.crt"
key_file = "pki/server.key"

[[users]]
name = "interop"
password_file = "interop.pw"

[[users]]
name = "only256"
verifier_file = "only256.scram"

[[permissions]]
user = "*"
address = "*"
operations = ["send", "receive"]

[[exchanges]]
name = "events"
type = "topic"
CONF
    for leg in jms proton rhea goamqp rabbitmqgo; do
        printf '\n[[bindings]]\nexchange = "events"\nqueue = "/queues/%s-orders"\nkey = "%s.order.#"\n' "$leg" "$leg"
    done
} >"$WORK/broker.toml"

echo ">> starting the broker (plain :$PLAIN_PORT, amqps :$TLS_PORT) ..."
"$BRK" --config "$WORK/broker.toml" >"$WORK/broker.log" 2>&1 &
BROKER_PID=$!
for _ in $(seq 1 50); do
    (exec 3<>"/dev/tcp/127.0.0.1/$PLAIN_PORT") 2>/dev/null && { exec 3>&-; break; }
    sleep 0.2
done

export MATRIX_HOST=127.0.0.1 MATRIX_PLAIN_PORT="$PLAIN_PORT" MATRIX_TLS_PORT="$TLS_PORT"
export MATRIX_CA="$WORK/pki/ca.crt" MATRIX_USER=interop MATRIX_PASS=interop-pw
export MATRIX_USER256=only256 MATRIX_PASS256=only256-pw MATRIX_WORK="$WORK"

declare -A RESULT=()
for leg in "${LEGS[@]}"; do
    echo
    echo "=================== $leg ==================="
    if MATRIX_CLIENT="$leg" "$INTEROP/$leg/features.sh"; then
        RESULT[$leg]=PASS
    else
        RESULT[$leg]=FAIL
    fi
done

echo
echo "=================== summary ==================="
status=0
for leg in "${LEGS[@]}"; do
    echo "  ${RESULT[$leg]}  $leg"
    [ "${RESULT[$leg]}" = PASS ] || status=1
done
[ $status -eq 0 ] || { echo "(broker log: $WORK/broker.log)"; }
exit $status
