//! External-toolchain interop: Apache Qpid JMS (a pure-Java, independent AMQP
//! 1.0 client stack) exercising OUR broker end-to-end. This is the JMS leg of
//! broker.md Phase 10, complementing `bench-compare/tests/fe2o3_interop.rs`
//! (the Rust `fe2o3-amqp` leg): a second, unrelated implementation proving our
//! broker's wire behavior isn't accidentally tailored to our own client.
//!
//! `#[ignore]`d — it needs a JVM and the qpid-jms classpath. The runner
//! `tests/interop/jms/run.sh` fetches the jars, compiles the Java client, sets
//! the env vars below, and invokes this with `--ignored`; the `interop-jms` CI
//! job does the same. The test starts a loopback broker in-process, spawns the
//! Java client against it, and asserts the round-trip.

mod harness;

use harness::*;

/// Required env (set by `tests/interop/jms/run.sh`):
/// - `QPID_JMS_CP`      — directory of qpid-jms `lib/*.jar` (used as `<dir>/*`)
/// - `QPID_JMS_CLASSES` — directory holding the compiled `JmsInterop.class`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "requires a JVM + qpid-jms classpath; run via tests/interop/jms/run.sh (CI: interop-jms)"]
async fn qpid_jms_roundtrips_through_our_broker() {
    let lib = std::env::var("QPID_JMS_CP")
        .expect("QPID_JMS_CP (qpid-jms lib dir) must be set — run tests/interop/jms/run.sh");
    let classes = std::env::var("QPID_JMS_CLASSES").expect(
        "QPID_JMS_CLASSES (compiled JmsInterop dir) must be set — run tests/interop/jms/run.sh",
    );

    let lb = loopback().await;
    let url = lb.url();

    let output = tokio::process::Command::new("java")
        .arg("-cp")
        .arg(format!("{classes}:{lib}/*"))
        .arg("JmsInterop")
        .arg(&url)
        .output()
        .await
        .expect("spawn java qpid-jms client");

    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        output.status.success() && stdout.contains("INTEROP_OK"),
        "Qpid JMS interop failed (status {:?}).\n--- stdout ---\n{stdout}\n--- stderr ---\n{stderr}",
        output.status.code()
    );
}
