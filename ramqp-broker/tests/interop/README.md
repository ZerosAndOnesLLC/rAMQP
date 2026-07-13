# External-toolchain interop

Independent, third-party AMQP 1.0 stacks exercising **ramqp-broker** — proof
its wire behavior isn't accidentally tailored to our own `ramqp` client. This
is the "external toolchain" half of broker.md Phase 10; the Rust `fe2o3-amqp`
leg lives in [`bench-compare/tests/fe2o3_interop.rs`](../../../bench-compare/tests/fe2o3_interop.rs).

## Legs

| Leg | Client stack | Direction | Where | Status |
|---|---|---|---|---|
| `fe2o3-amqp` | Rust (`fe2o3-amqp`) | client → our broker | `bench-compare/tests/fe2o3_interop.rs` | ✅ landed |
| **Qpid JMS** | Java (Apache Qpid JMS 2.x, Jakarta Messaging) | client → our broker | `jms/` + `tests/jms_interop.rs` | ✅ landed |
| Qpid proton | C/Python (`python-qpid-proton`) | client → our broker | — | ⏳ pending (native lib) |
| our client → Qpid | `ramqp` | client → Qpid Broker-J | — | ⏳ pending (broker provisioning) |

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
