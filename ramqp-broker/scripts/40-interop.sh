#!/usr/bin/env bash
# Stage 4 — interop matrix.
#
# Proves ramqp-broker speaks real AMQP 1.0, two ways:
#   * the `ramqp` CLIENT interop suite (the same one CI runs against RabbitMQ and
#     Artemis) passes against ramqp-broker, RabbitMQ 4.x, and Artemis — so our
#     broker behaves like the brokers people deploy;
#   * an INDEPENDENT client, `fe2o3-amqp`, interops with ramqp-broker — so we are
#     not just agreeing with our own client's quirks.
#
# Each external broker leg is skipped (with a warning) if its container is down.
# Fresh per-run queues are used so pre-existing backlogs never taint results.

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 4: interop matrix"

RID="$$"
ART_BIN=/var/lib/artemis-instance/bin/artemis

# Run the ramqp client interop suite against a URL+address; log per target.
client_suite() { # url address logtag
  RAMQP_BROKER_URL="$1" RAMQP_BROKER_ADDRESS="$2" \
    cargo test -q -p ramqp --test broker -- --ignored --test-threads=1 \
    >"$RAMQP_OUT/interop-$3.log" 2>&1
}

# --- ramqp-broker ----------------------------------------------------------
build_brokerd >/dev/null
PORT="$(free_port)"
spawn_brokerd interop-broker -- --listen "127.0.0.1:$PORT"
if wait_port "$PORT" 30; then
  check "ramqp client → ramqp-broker" \
    client_suite "amqp://127.0.0.1:$PORT" "/queues/interop-$RID" ramqp-broker
else
  fail "interop: ramqp-broker never came up"
fi

# --- RabbitMQ 4.x ----------------------------------------------------------
if container_up rabbit; then
  Q="ramqp_interop_$RID"
  if curl -fsS -u guest:guest -X PUT "http://localhost:15672/api/queues/%2F/$Q" \
       -H content-type:application/json -d '{"durable":true}' >/dev/null 2>&1; then
    check "ramqp client → RabbitMQ 4.x" \
      client_suite "amqp://guest:guest@localhost:5672" "/queues/$Q" rabbitmq
    curl -fsS -u guest:guest -X DELETE "http://localhost:15672/api/queues/%2F/$Q" >/dev/null 2>&1 || true
  else
    warn "RabbitMQ mgmt API not reachable; skipping"
  fi
else
  warn "rabbit container not up; skipping RabbitMQ leg"
fi

# --- ActiveMQ Artemis ------------------------------------------------------
if container_up artemis; then
  Q="ramqp_interop_$RID"
  # Artemis auto-creates MULTICAST (drops pre-subscribe sends); pre-create an
  # ANYCAST/queue-semantics address so produce-then-consume works.
  if docker exec artemis "$ART_BIN" queue create --name "$Q" --address "$Q" \
       --anycast --durable --preserve-on-no-consumers --auto-create-address \
       --url tcp://localhost:61616 --user guest --password guest --silent >/dev/null 2>&1; then
    check "ramqp client → Artemis" \
      client_suite "amqp://guest:guest@localhost:5682" "$Q" artemis
    docker exec artemis "$ART_BIN" queue delete --name "$Q" \
      --url tcp://localhost:61616 --user guest --password guest >/dev/null 2>&1 || true
  else
    warn "could not create Artemis queue; skipping"
  fi
else
  warn "artemis container not up; skipping Artemis leg"
fi

# --- independent client (fe2o3-amqp) → ramqp-broker ------------------------
# These tests bring up ramqp-broker in-process and drive it with fe2o3-amqp.
check "fe2o3-amqp client → ramqp-broker (independent impl)" \
  cargo test -q -p ramqp-bench-compare --test fe2o3_interop

finish
