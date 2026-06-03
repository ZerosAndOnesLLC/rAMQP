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

use ramqp::{Connection, Message};

fn broker_url() -> Option<String> {
    std::env::var("RAMQP_BROKER_URL").ok()
}

#[tokio::test]
async fn produce_consume_roundtrip() {
    let Some(url) = broker_url() else {
        eprintln!("skipping broker test: set RAMQP_BROKER_URL to run");
        return;
    };

    let address = "ramqp.integration.queue";
    let conn = Connection::open(&url).await.expect("connect");
    let session = conn.begin_session().await.expect("begin session");

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
    let delivery = consumer.recv().await.expect("recv");
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

    let address = "ramqp.integration.bulk";
    let conn = Connection::open(&url).await.expect("connect");
    let session = conn.begin_session().await.expect("begin session");
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
        let delivery = consumer.recv().await.expect("recv");
        consumer.accept(&delivery).await.expect("accept");
    }

    conn.close().await.expect("close");
}
