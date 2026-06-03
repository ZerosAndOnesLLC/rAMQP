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

## ActiveMQ Artemis (second broker; AMQP + WebSocket)

```sh
docker run -d --name ramqp-artemis -p 61616:61616 -p 5445:5445 \
  -e ARTEMIS_USER=admin -e ARTEMIS_PASSWORD=admin \
  apache/activemq-artemis:latest-alpine

# Artemis auto-creates addresses; use the bare queue name.
RAMQP_BROKER_URL=amqp://admin:admin@localhost:61616 \
RAMQP_BROKER_ADDRESS=ramqp.it \
  cargo test --test broker -- --test-threads=1
```

## Soak

```sh
RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
RAMQP_BROKER_ADDRESS=/queues/ramqp_soak \
RAMQP_SOAK_SECS=60 \
  cargo run --release --example soak
```
