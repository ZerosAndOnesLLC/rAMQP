//! Broker interop tests (WP-8.1).
//!
//! These run against a real AMQP 1.0 broker. Set `RAMQP_BROKER_URL` to enable
//! them, e.g. against ActiveMQ Artemis or RabbitMQ (AMQP 1.0 plugin):
//!
//! ```sh
//! RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 cargo test --test broker
//! ```
//!
//! When the variable is unset the tests no-op (so CI without Docker stays green).

use std::time::Duration;

use ramqp::types::messaging::{DeliveryState, Modified};
use ramqp::{Connection, Consumer, Delivery, Message, Session};

fn broker_url() -> Option<String> {
    std::env::var("RAMQP_BROKER_URL").ok()
}

/// Drain any leftover messages on `address` so a test starts from a clean queue
/// (brokers persist durable queues across runs; tests share one queue).
/// Accepts everything until a short idle gap proves the queue is empty.
async fn drain(session: &Session, address: &str) {
    if let Ok(mut c) = session.create_consumer(address).await {
        while let Ok(Ok(d)) = tokio::time::timeout(Duration::from_millis(300), c.recv()).await {
            let _ = c.accept(&d).await;
        }
        c.detach().await.ok();
    }
}

/// Receive within a bounded time so a missing delivery fails the test fast
/// instead of hanging the whole suite.
async fn recv_within(consumer: &mut Consumer, secs: u64) -> Delivery {
    tokio::time::timeout(Duration::from_secs(secs), consumer.recv())
        .await
        .expect("recv timed out waiting for a delivery")
        .expect("recv")
}

/// Connect, retrying a few transient failures. Single-node broker dev containers
/// (notably Artemis, which holds connections for a 60s TTL) can briefly refuse a
/// fresh handshake under the suite's rapid open/close churn; a real client
/// retries, so the interop suite does too.
async fn open_conn(url: &str) -> Connection {
    let mut last = String::new();
    for attempt in 0..6 {
        match Connection::open(url).await {
            Ok(c) => return c,
            Err(e) => {
                last = format!("{e:?}");
                tokio::time::sleep(Duration::from_millis(400 * (attempt + 1))).await;
            }
        }
    }
    panic!("connect failed after retries: {last}");
}

/// The node address to produce/consume against. Brokers differ: RabbitMQ 4.x
/// uses `/queues/<name>`, Artemis uses the bare queue name. Override with
/// `RAMQP_BROKER_ADDRESS`.
fn broker_address() -> String {
    std::env::var("RAMQP_BROKER_ADDRESS").unwrap_or_else(|_| "ramqp.integration.queue".to_string())
}

#[tokio::test]
async fn produce_consume_roundtrip() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };

    let address = broker_address();
    let address = address.as_str();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("begin session");
    drain(&session, address).await;

    let producer = session.create_producer(address).await.expect("producer");
    let outcome = producer
        .send(Message::text("hello from the ramqp integration suite"))
        .await
        .expect("send");
    assert!(
        matches!(outcome, ramqp::types::messaging::DeliveryState::Accepted(_)),
        "broker should accept the message, got {outcome:?}"
    );

    let mut consumer = session.create_consumer(address).await.expect("consumer");
    let delivery = recv_within(&mut consumer, 10).await;
    consumer.accept(&delivery).await.expect("accept");

    producer.detach().await.ok();
    consumer.detach().await.ok();
    session.end().await.ok();
    conn.close().await.expect("close");
}

#[tokio::test]
async fn many_messages_roundtrip() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };

    let address = broker_address();
    let address = address.as_str();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("begin session");
    drain(&session, address).await;
    let producer = session.create_producer(address).await.expect("producer");

    const N: usize = 100;
    for i in 0..N {
        producer
            .send(Message::text(format!("msg-{i}")))
            .await
            .expect("send");
    }

    let mut consumer = session.create_consumer(address).await.expect("consumer");
    for _ in 0..N {
        let delivery = recv_within(&mut consumer, 10).await;
        consumer.accept(&delivery).await.expect("accept");
    }

    conn.close().await.expect("close");
}

/// `release` returns the delivery to the queue; the broker must redeliver it.
#[tokio::test]
async fn release_redelivers() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };
    let address = broker_address();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("session");
    drain(&session, &address).await;

    let producer = session.create_producer(&address).await.expect("producer");
    producer
        .send(Message::text("release-me"))
        .await
        .expect("send");
    producer.detach().await.ok();

    let mut consumer = session.create_consumer(&address).await.expect("consumer");
    let first = consumer.recv().await.expect("recv1");
    assert_eq!(first.message().unwrap().body, Message::text("release-me").body);
    consumer.release(&first).await.expect("release");

    // The released message must come back (to this or a fresh consumer).
    let second = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
        .await
        .expect("released message should be redelivered")
        .expect("recv2");
    assert_eq!(second.message().unwrap().body, Message::text("release-me").body);
    consumer.accept(&second).await.expect("accept");

    conn.close().await.expect("close");
}

/// `reject` (no dead-letter configured) must drop the message: it is not
/// redelivered, and the broker accepts the disposition without error.
#[tokio::test]
async fn reject_drops() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };
    let address = broker_address();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("session");
    drain(&session, &address).await;

    let producer = session.create_producer(&address).await.expect("producer");
    producer
        .send(Message::text("reject-me"))
        .await
        .expect("send");
    producer.detach().await.ok();

    let mut consumer = session.create_consumer(&address).await.expect("consumer");
    let d = consumer.recv().await.expect("recv");
    assert_eq!(d.message().unwrap().body, Message::text("reject-me").body);
    consumer.reject(&d, None).await.expect("reject");

    // Nothing should be redelivered (no DLX): recv must idle out.
    let again = tokio::time::timeout(Duration::from_secs(1), consumer.recv()).await;
    assert!(again.is_err(), "a rejected message must not be redelivered");

    conn.close().await.expect("close");
}

/// `modify { delivery_failed: true }` requeues with an incremented
/// delivery-count; the broker must redeliver it.
#[tokio::test]
async fn modify_requeues() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };
    let address = broker_address();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("session");
    drain(&session, &address).await;

    let producer = session.create_producer(&address).await.expect("producer");
    producer
        .send(Message::text("modify-me"))
        .await
        .expect("send");
    producer.detach().await.ok();

    let mut consumer = session.create_consumer(&address).await.expect("consumer");
    let d = consumer.recv().await.expect("recv");
    consumer
        .modify(
            &d,
            Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(false),
                message_annotations: None,
            },
        )
        .await
        .expect("modify");

    let redelivered = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
        .await
        .expect("modified message should be redelivered")
        .expect("recv2");
    assert_eq!(redelivered.message().unwrap().body, Message::text("modify-me").body);
    // Drain it so the test leaves the queue empty.
    consumer.accept(&redelivered).await.expect("accept");

    conn.close().await.expect("close");
}

/// The terminal outcome the broker settles a *produced* message with is
/// `Accepted` on a normal enqueue.
#[tokio::test]
async fn produce_outcome_is_accepted() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };
    let address = broker_address();
    let conn = open_conn(&url).await;
    let session = conn.begin_session().await.expect("session");
    drain(&session, &address).await;
    let producer = session.create_producer(&address).await.expect("producer");
    let outcome = producer.send(Message::text("x")).await.expect("send");
    assert!(matches!(outcome, DeliveryState::Accepted(_)), "got {outcome:?}");
    // clean up
    let mut c = session.create_consumer(&address).await.expect("consumer");
    let d = recv_within(&mut c, 10).await;
    c.accept(&d).await.ok();
    conn.close().await.expect("close");
}

/// SCRAM-SHA-256 authentication against the broker (requires the `scram`
/// feature: `cargo test --test broker --features scram`).
#[cfg(feature = "scram")]
#[tokio::test]
async fn scram_sha256_auth() {
    let Some(url) = broker_url() else {
        eprintln!("skipping scram test: set RAMQP_BROKER_URL to run");
        return;
    };
    use ramqp::sasl::{SaslProfile, ScramMechanism};

    let user = std::env::var("RAMQP_BROKER_USER").unwrap_or_else(|_| "guest".into());
    let pass = std::env::var("RAMQP_BROKER_PASS").unwrap_or_else(|_| "guest".into());

    let result = ramqp::ConnectionBuilder::new(&url)
        .sasl(SaslProfile::Scram {
            mechanism: ScramMechanism::Sha256,
            username: user,
            password: pass,
        })
        .connect()
        .await;

    match result {
        Ok(conn) => {
            // Beginning a session proves the AMQP layer came up after SCRAM auth.
            let session = conn.begin_session().await.expect("begin");
            session.end().await.ok();
            conn.close().await.expect("close");
        }
        // Many brokers (e.g. default RabbitMQ) don't advertise SCRAM; that's a
        // broker-config matter, not a client failure, so skip rather than fail.
        Err(e)
            if e.kind() == ramqp::error::ErrorKind::Sasl
                && format!("{e}").contains("does not offer") =>
        {
            eprintln!("skipping scram test: broker does not offer SCRAM-SHA-256");
        }
        Err(e) => panic!("SCRAM-SHA-256 connect failed: {e}"),
    }
}
