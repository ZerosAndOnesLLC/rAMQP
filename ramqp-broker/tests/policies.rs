//! Client-facing queue-policy tests (broker.md Phase 7): TTL, max-length,
//! and dead-lettering, across transient and quorum queues.

use std::time::Duration;

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig, OverflowBehavior, QueuePolicy, ShutdownHandle};

async fn start(config: BrokerConfig) -> (std::net::SocketAddr, ShutdownHandle) {
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

/// TTL: an expired message is dead-lettered instead of delivered.
#[tokio::test]
async fn expired_messages_dead_letter_instead_of_delivering() {
    let mut policy = QueuePolicy::default();
    policy.message_ttl = Some(Duration::from_millis(150));
    policy.dead_letter = Some("/queues/dead".to_owned());
    let mut config = BrokerConfig::default();
    config.policies = vec![("ttl-".to_owned(), policy)];
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/queues/ttl-orders")
        .await
        .expect("producer");
    producer
        .send(Message::text("will expire"))
        .await
        .expect("send");

    // Let it outlive its TTL before any consumer shows up.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // The consumer gets nothing from the source queue...
    let mut consumer = session
        .create_consumer("/queues/ttl-orders")
        .await
        .expect("consumer");
    // Trigger a dispatch cycle (expiry is lazy): a fresh publish flows
    // through, the expired one does not.
    producer.send(Message::text("fresh")).await.expect("send");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "fresh", "expired message must not deliver");
    consumer.accept(&d).await.expect("accept");

    // ...and the dead-letter queue holds the expired one.
    let mut dead = session
        .create_consumer("/queues/dead")
        .await
        .expect("dlx consumer");
    let d = tokio::time::timeout(Duration::from_secs(5), dead.recv())
        .await
        .expect("dead letter in time")
        .expect("dead letter");
    assert_eq!(text_of(&d), "will expire");
    dead.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Max-length with drop-head: the queue keeps the newest N, dead-lettering
/// the displaced head.
#[tokio::test]
async fn drop_head_keeps_newest_and_dead_letters_the_oldest() {
    let mut policy = QueuePolicy::default();
    policy.max_length = Some(3);
    policy.overflow = OverflowBehavior::DropHead;
    policy.dead_letter = Some("/queues/displaced".to_owned());
    let mut config = BrokerConfig::default();
    config.policies = vec![("ring-".to_owned(), policy)];
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/queues/ring-buffer")
        .await
        .expect("producer");
    // Five in, cap three: m0 and m1 get displaced.
    for i in 0..5 {
        let outcome = producer
            .send(Message::text(format!("m{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "drop-head admits every publish, got {outcome:?}"
        );
    }

    let mut consumer = session
        .create_consumer("/queues/ring-buffer")
        .await
        .expect("consumer");
    for i in 2..5 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), format!("m{i}"), "newest three survive");
        consumer.accept(&d).await.expect("accept");
    }

    let mut displaced = session
        .create_consumer("/queues/displaced")
        .await
        .expect("dlx consumer");
    for i in 0..2 {
        let d = tokio::time::timeout(Duration::from_secs(5), displaced.recv())
            .await
            .expect("displaced in time")
            .expect("displaced");
        assert_eq!(text_of(&d), format!("m{i}"), "oldest dead-letter in order");
        displaced.accept(&d).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Max delivery attempts: a message that keeps failing is dead-lettered
/// instead of redelivering forever.
#[tokio::test]
async fn delivery_limit_dead_letters_poison_messages() {
    let mut policy = QueuePolicy::default();
    policy.max_delivery_attempts = Some(2);
    policy.dead_letter = Some("/queues/poison-dlx".to_owned());
    let mut config = BrokerConfig::default();
    config.policies = vec![("poison-".to_owned(), policy)];
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/queues/poison-work")
        .await
        .expect("producer");
    producer.send(Message::text("poison")).await.expect("send");

    let mut consumer = session
        .create_consumer("/queues/poison-work")
        .await
        .expect("consumer");
    // Fail it twice (modified{delivery-failed} counts an attempt).
    for _ in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), "poison");
        consumer
            .modify(
                &d,
                ramqp::types::messaging::Modified {
                    delivery_failed: Some(true),
                    undeliverable_here: None,
                    message_annotations: None,
                },
            )
            .await
            .expect("modify");
    }
    // Third delivery never happens: the message is in the DLX instead.
    let starved = tokio::time::timeout(Duration::from_millis(400), consumer.recv()).await;
    assert!(starved.is_err(), "poison message must stop redelivering");

    let mut dlx = session
        .create_consumer("/queues/poison-dlx")
        .await
        .expect("dlx consumer");
    let d = tokio::time::timeout(Duration::from_secs(5), dlx.recv())
        .await
        .expect("dead letter in time")
        .expect("dead letter");
    assert_eq!(text_of(&d), "poison");
    dlx.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// MED-1 (issue #19): quorum delivery limits count nacks EXACTLY, even in a
/// fast nack loop — counting from applied state alone lagged the pipelined
/// failure increments and fired late (or never).
#[tokio::test]
async fn quorum_delivery_limit_fires_exactly() {
    let mut policy = QueuePolicy::default();
    policy.max_delivery_attempts = Some(2);
    policy.dead_letter = Some("/queues/qpoison-dlx".to_owned());
    let mut config = BrokerConfig::default();
    config.policies = vec![("qpoison-".to_owned(), policy)];
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/quorum/qpoison-work")
        .await
        .expect("producer");
    producer.send(Message::text("poison")).await.expect("send");

    let mut consumer = session
        .create_consumer("/quorum/qpoison-work")
        .await
        .expect("consumer");
    // Nack as fast as the deliveries arrive: exactly two attempts allowed.
    for _ in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), "poison");
        consumer
            .modify(
                &d,
                ramqp::types::messaging::Modified {
                    delivery_failed: Some(true),
                    undeliverable_here: None,
                    message_annotations: None,
                },
            )
            .await
            .expect("modify");
    }
    let starved = tokio::time::timeout(Duration::from_millis(400), consumer.recv()).await;
    assert!(
        starved.is_err(),
        "third delivery must not happen: the limit is exact"
    );

    let mut dlx = session
        .create_consumer("/queues/qpoison-dlx")
        .await
        .expect("dlx consumer");
    let d = tokio::time::timeout(Duration::from_secs(5), dlx.recv())
        .await
        .expect("dead letter in time")
        .expect("dead letter");
    assert_eq!(text_of(&d), "poison");
    dlx.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// The same policy machinery holds for quorum queues (leader-local
/// enforcement; expiry timestamps ride the replicated log).
#[tokio::test]
async fn quorum_queues_honor_ttl_and_dead_lettering() {
    let mut policy = QueuePolicy::default();
    policy.message_ttl = Some(Duration::from_millis(150));
    policy.dead_letter = Some("/queues/qdead".to_owned());
    let mut config = BrokerConfig::default();
    config.policies = vec![("qttl-".to_owned(), policy)];
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/quorum/qttl-orders")
        .await
        .expect("producer");
    producer
        .send(Message::text("will expire"))
        .await
        .expect("send");
    tokio::time::sleep(Duration::from_millis(300)).await;

    let mut consumer = session
        .create_consumer("/quorum/qttl-orders")
        .await
        .expect("consumer");
    producer.send(Message::text("fresh")).await.expect("send");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "fresh", "expired message must not deliver");
    consumer.accept(&d).await.expect("accept");

    let mut dead = session
        .create_consumer("/queues/qdead")
        .await
        .expect("dlx consumer");
    let d = tokio::time::timeout(Duration::from_secs(5), dead.recv())
        .await
        .expect("dead letter in time")
        .expect("dead letter");
    assert_eq!(text_of(&d), "will expire");
    dead.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}
