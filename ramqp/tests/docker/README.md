# Live broker interop harness

The `tests/broker.rs` (plain), `tests/tls.rs` (`amqps`) and `tests/ws.rs`
(WebSocket) suites are **env-gated** — they no-op unless their broker URL is
set, so a default `cargo test` stays green without Docker.

## RabbitMQ 4.x (plain + TLS)

```sh
docker run -d --name ramqp-rabbit -p 5672:5672 -p 5671:5671 -p 15672:15672 \
  rabbitmq:4-management

# RabbitMQ does NOT auto-create a queue from an AMQP 1.0 attach — declare first:
curl -u guest:guest -X PUT http://localhost:15672/api/queues/%2F/ramqp_it \
  -H content-type:application/json -d '{"durable":true}'

# Plain AMQP 1.0 (note the /queues/<name> address form RabbitMQ uses):
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/ramqp_it \
  cargo test --test broker -- --test-threads=1
```

### TLS (`amqps://`)

```sh
./tests/docker/gen-certs.sh                       # writes certs/{ca,server}.*
docker exec ramqp-rabbit mkdir -p /etc/rabbitmq/certs
docker cp tests/docker/certs/ca.crt     ramqp-rabbit:/etc/rabbitmq/certs/
docker cp tests/docker/certs/server.crt ramqp-rabbit:/etc/rabbitmq/certs/
docker cp tests/docker/certs/server.key ramqp-rabbit:/etc/rabbitmq/certs/
docker cp tests/docker/rabbitmq-tls.conf ramqp-rabbit:/etc/rabbitmq/rabbitmq.conf
docker restart ramqp-rabbit

RAMQP_TLS_BROKER_URL=amqps://guest:guest@localhost:5671 \
RAMQP_TLS_CA_PEM=tests/docker/certs/ca.crt \
RAMQP_TLS_ADDRESS=/queues/ramqp_it \
  cargo test --features rustls --test tls -- --test-threads=1
```

## ActiveMQ Artemis (second broker)

```sh
# Map the container's AMQP-only acceptor (5672) to host 5673 to avoid clashing
# with RabbitMQ. The multiplexed 61616 acceptor mis-detects AMQP, so prefer 5672.
docker run -d --name ramqp-artemis -p 61616:61616 -p 5673:5672 -p 8161:8161 \
  -e ARTEMIS_USER=admin -e ARTEMIS_PASSWORD=admin \
  apache/activemq-artemis:latest-alpine

# Pre-create an ANYCAST (queue-semantics) address — Artemis auto-creates
# MULTICAST by default, which drops messages sent before a subscriber exists:
docker exec ramqp-artemis /var/lib/artemis-instance/bin/artemis queue create \
  --name ramqp.it --address ramqp.it --anycast --durable \
  --preserve-on-no-consumers --auto-create-address \
  --url tcp://localhost:61616 --user admin --password admin --silent

RAMQP_BROKER_URL=amqp://admin:admin@localhost:5673 \
RAMQP_BROKER_ADDRESS=ramqp.it \
  cargo test --test broker -- --test-threads=1
```

Note: Artemis validates `terminus-expiry-policy` strictly (an empty symbol is
rejected — this is what surfaced the `Default` fix). The single-node dev
container also holds connections for a 60s TTL, so running all six tests
back-to-back can stall fresh handshakes; the suite retries connects to absorb
this. Interop itself (lifecycle, 100-message bulk, modify/redelivery) is solid.

## WebSocket (`ws://`)

Most brokers don't expose AMQP-over-WebSocket, so `tests/ws.rs` runs an
in-process WS→TCP bridge in front of a plain-AMQP broker:

```sh
RAMQP_WS_BROKER_TCP=127.0.0.1:5672 \
RAMQP_WS_ADDRESS=/queues/ramqp_it \
  cargo test --features ws --test ws -- --test-threads=1
```

## Soak

```sh
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/ramqp_soak \
RAMQP_SOAK_SECS=60 \
  cargo run --release --example soak
```
