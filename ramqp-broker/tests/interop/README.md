# External-toolchain interop

Independent, third-party AMQP 1.0 stacks exercising **ramqp-broker** — proof
its wire behavior isn't accidentally tailored to our own `ramqp` client. This
is the "external toolchain" half of broker.md Phase 10; the Rust `fe2o3-amqp`
leg lives in [`bench-compare/tests/fe2o3_interop.rs`](../../../bench-compare/tests/fe2o3_interop.rs).

## Feature matrix (logins, exchanges, TLS)

`matrix/run.sh` starts one fixture broker the way a RabbitMQ 4.x replacement
runs — users (one from a password, one with only a SCRAM-SHA-256 verifier),
a topic exchange with bindings, plain + `amqps` listeners with a throwaway
CA — and runs every client leg against it (release gate: `interop-matrix`):

| Leg | Client | Scenarios |
|---|---|---|
| `jms` | Apache Qpid JMS 2.x (Java) | PLAIN, SCRAM (client-chosen and forced SHA-256), bad password, exchange by address key and by subject (`JMSType`), unknown exchange, amqps |
| `proton` | Apache Qpid Proton (Python/C) | PLAIN, bad password, exchange by address key and subject, unknown exchange, amqps |
| `rhea` | rhea 3.x (Node.js) | same as proton |
| `goamqp` | Azure go-amqp 1.x (Go) | same as proton |
| `rabbitmq-go` | RabbitMQ's own AMQP 1.0 Go client | connect, publish via `ExchangeAddress` and `QueueAddress`, consume, amqps; its topology management gets a `501` |
| fe2o3 | fe2o3-amqp (Rust, in-process: `bench-compare/tests/fe2o3_features.rs`) | PLAIN, SCRAM-SHA-256/512, bad password, anonymous refused, exchanges, amqps (spec TLS negotiation) |

```sh
ramqp-broker/tests/interop/matrix/run.sh            # all legs
ramqp-broker/tests/interop/matrix/run.sh jms rhea   # some
```

Findings it drove (fixed in the broker): clients that pick the strongest
SCRAM mechanism offered (Qpid JMS) need every offered mechanism to work for
every user; spec-default TLS (fe2o3) sends the `AMQP 2.1.0.0` TLS header
before the handshake; RabbitMQ clients send `vhost:/` and open `/management`
links at connect. Known client issue: fe2o3-amqp 0.18 fails the next attach
on a session right after a refused attach (reproduces against RabbitMQ too).

## Legs

| Leg | Client stack | Direction | Where | Status |
|---|---|---|---|---|
| `fe2o3-amqp` | Rust (`fe2o3-amqp`) | client → our broker | `bench-compare/tests/fe2o3_interop.rs` | ✅ landed |
| **Qpid JMS** | Java (Apache Qpid JMS 2.x, Jakarta Messaging) | client → our broker | `jms/` + `tests/jms_interop.rs` | ✅ landed |
| **Qpid Proton** | Python/C (`python3-qpid-proton`) | client → our broker | `proton/` | ✅ landed |
| our client → Qpid | `ramqp` | client → Qpid Broker-J | — | ⏸️ deferred (see note) |

**Note on our-client → Qpid Broker-J:** attempted, deferred. Our client's
reference-broker interop is already covered by the `interop-rabbitmq` and
`interop-artemis` CI jobs (the `ramqp/tests/broker.rs` suite against two
independent brokers). Adding Qpid Broker-J as a third stalled on the
`apache/qpid-broker-j` image itself, not on rAMQP: it does not substitute
`QPID_ADMIN_USER`/`QPID_ADMIN_PASSWORD` into its config (the admin account stays
the literal `${QPID_ADMIN_USER}`), its Plain auth provider offers no ANONYMOUS,
and its REST returns 405 for queue creation — so neither pre-declaring a queue
nor connecting cleanly worked without deeper image surgery. Revisit with a
purpose-built Broker-J config (a mounted `initial-config.json` with a plain
password file + `nodeAutoCreationPolicies`) if a third reference broker is
wanted.

## Qpid JMS leg

`jms/JmsInterop.java` is a minimal Qpid JMS client: connect, produce + consume
a text message on a transient queue, verify the body, print `INTEROP_OK`. The
`#[ignore]`d `jms_interop` rust test starts a loopback broker in-process and
spawns the Java client at it.

Run it (fetches the jars, compiles, runs the test):

```sh
ramqp-broker/tests/interop/jms/run.sh
```

Requires a JVM (`java`/`javac`), `curl`, `tar`, and `cargo`. CI runs the same
script in the `interop-jms` job. The qpid-jms version is pinned by
`QPID_JMS_VERSION` (default `2.10.0`); jars cache under `target/interop/`.

## Qpid Proton leg

`proton/proton_interop.py` is a minimal Apache Qpid Proton (Python) client —
the C-based proton engine, a third independent stack. Proton needs a native
library, so `proton/run.sh` runs it one of two ways:

```sh
ramqp-broker/tests/interop/proton/run.sh
```

- **host** — if `python3 -c "import proton"` works (CI installs
  `python3-qpid-proton`), the broker binds loopback and proton runs on the host;
- **docker fallback** — otherwise the broker binds `0.0.0.0` and proton runs in
  an `ubuntu` container that installs `python3-qpid-proton` and reaches the host
  via `host-gateway` (dev boxes where system packages can't be touched).

CI runs the host path in the `interop-proton` job.

> This leg is why `ramqp-core` gained the `(name, role)` link keying fix
> (session/state.rs): proton names a sender and a receiver to the same address
> identically, which the broker previously rejected — see the regression test
> `same_name_sender_and_receiver_both_attach`.
