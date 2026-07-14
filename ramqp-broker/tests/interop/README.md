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
| **Qpid Proton** | Python/C (`python3-qpid-proton`) | client → our broker | `proton/` | ✅ landed |
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
