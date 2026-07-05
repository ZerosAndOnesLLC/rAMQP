//! Client-facing quorum queue tests: the unmodified `ramqp` client produces
//! to and consumes from `/quorum/<name>` addresses — every publish is
//! acknowledged only after the enqueue committed to the queue group's Raft
//! log, and dispatch reads from the applied state machine.

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig};

async fn start(config: BrokerConfig) -> (std::net::SocketAddr, ramqp_broker::ShutdownHandle) {
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

fn text_of(delivery: &ramqp::Delivery) -> String {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    let msg = delivery.message().expect("decodable message");
    match msg.body {
        Body::Value(Value::String(s)) => s,
        other => panic!("expected text body, got {other:?}"),
    }
}

#[tokio::test]
async fn quorum_produce_consume_round_trip() {
    let (addr, shutdown) = start(BrokerConfig::default()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    // Each send's accepted disposition is the REPLICATED confirm (commit).
    let producer = session
        .create_producer("/quorum/orders")
        .await
        .expect("producer");
    for i in 0..10 {
        let outcome = producer
            .send(Message::text(format!("q{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "commit-backed accept expected, got {outcome:?}"
        );
    }

    let mut consumer = session
        .create_consumer("/quorum/orders")
        .await
        .expect("consumer");
    for i in 0..10 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(
            text_of(&d),
            format!("q{i}"),
            "FIFO order from applied state"
        );
        consumer.accept(&d).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn quorum_released_message_redelivers_without_penalty() {
    let (addr, shutdown) = start(BrokerConfig::default()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/quorum/retry")
        .await
        .expect("producer");
    producer.send(Message::text("try me")).await.expect("send");

    let mut consumer = session
        .create_consumer("/quorum/retry")
        .await
        .expect("consumer");
    let first = consumer.recv().await.expect("first");
    consumer.release(&first).await.expect("release");
    let second = consumer.recv().await.expect("redelivery");
    assert_eq!(text_of(&second), "try me");
    consumer.accept(&second).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn quorum_consumer_drop_requeues_unacked() {
    let (addr, shutdown) = start(BrokerConfig::default()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/quorum/orphan")
        .await
        .expect("producer");
    producer
        .send(Message::text("orphaned"))
        .await
        .expect("send");

    let mut c1 = session.create_consumer("/quorum/orphan").await.expect("c1");
    let d = c1.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "orphaned");
    c1.detach().await.expect("detach unsettled");

    let mut c2 = session.create_consumer("/quorum/orphan").await.expect("c2");
    let d = c2.recv().await.expect("redelivery");
    assert_eq!(text_of(&d), "orphaned");
    c2.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn quorum_overflow_rejects_the_publish() {
    let config = BrokerConfig {
        max_queue_depth: 2,
        ..Default::default()
    };
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/quorum/tiny")
        .await
        .expect("producer");

    for i in 0..2 {
        producer
            .send(Message::text(format!("fits{i}")))
            .await
            .expect("fits");
    }
    let outcome = producer
        .send(Message::text("overflow"))
        .await
        .expect("terminal outcome");
    assert!(
        matches!(outcome, DeliveryState::Rejected(_)),
        "expected rejected on overflow, got {outcome:?}"
    );

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn transient_and_quorum_queues_coexist() {
    let (addr, shutdown) = start(BrokerConfig::default()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    // Same base name, different kinds — distinct queues.
    let tp = session.create_producer("/queues/same").await.expect("tp");
    let qp = session.create_producer("/quorum/same").await.expect("qp");
    tp.send(Message::text("transient")).await.expect("send t");
    qp.send(Message::text("quorum")).await.expect("send q");

    let mut tc = session.create_consumer("/queues/same").await.expect("tc");
    let mut qc = session.create_consumer("/quorum/same").await.expect("qc");
    assert_eq!(text_of(&tc.recv().await.expect("t recv")), "transient");
    assert_eq!(text_of(&qc.recv().await.expect("q recv")), "quorum");

    conn.close().await.expect("close");
    shutdown.shutdown();
}
