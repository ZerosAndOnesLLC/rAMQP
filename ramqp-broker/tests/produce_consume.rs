//! Phase 4 end-to-end tests: the unmodified `ramqp` client produces to and
//! consumes from this broker's transient queues over loopback TCP —
//! store-and-forward, live dispatch, competing consumers, and
//! settlement-driven requeue.

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig};

async fn start() -> (std::net::SocketAddr, ramqp_broker::ShutdownHandle) {
    let bound = Broker::new(BrokerConfig::default())
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

async fn connect(addr: std::net::SocketAddr) -> ramqp::Connection {
    ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect")
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

/// Detaching a consumer and attaching another on the same session reuses the
/// link handle (client allocates LIFO). The broker must rebind that reused
/// (channel, handle) to the new queue and route only that queue's messages to
/// it — the binding-generation guarantee (H1): a stale command for the old
/// binding must never be misrouted to the link that now occupies the handle.
///
/// Asserts the *routing* property (a queue-A message never reaches a queue-B
/// consumer and vice-versa) under repeated handle-reuse churn; it tolerates
/// at-least-once redelivery within a queue.
#[tokio::test]
async fn consumer_handle_reuse_across_queues_never_cross_delivers() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let pa = session
        .create_producer("/queues/reuse-a")
        .await
        .expect("pa");
    let pb = session
        .create_producer("/queues/reuse-b")
        .await
        .expect("pb");

    for round in 0..8 {
        pa.send(Message::text(format!("a{round}")))
            .await
            .expect("send a");
        pb.send(Message::text(format!("b{round}")))
            .await
            .expect("send b");

        // Consume from A on a fresh consumer, then detach it (frees the handle).
        let mut ca = session
            .create_consumer("/queues/reuse-a")
            .await
            .expect("ca");
        let da = ca.recv().await.expect("recv a");
        assert!(
            text_of(&da).starts_with('a'),
            "queue-A consumer received a non-A message: {:?}",
            text_of(&da)
        );
        ca.accept(&da).await.expect("accept a");
        ca.detach().await.expect("detach a");

        // A new consumer on B reuses A's just-freed handle. It must only ever
        // receive B's messages — never one of A's (the misrouting bug).
        let mut cb = session
            .create_consumer("/queues/reuse-b")
            .await
            .expect("cb");
        let db = cb.recv().await.expect("recv b");
        assert!(
            text_of(&db).starts_with('b'),
            "reused handle received the wrong queue's message: {:?}",
            text_of(&db)
        );
        cb.accept(&db).await.expect("accept b");
        cb.detach().await.expect("detach b");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Refusing one link (here: an attach that trips the queue cap) must detach
/// only that link — the session and its sibling links stay fully usable. A
/// bad address must never tear the whole session down.
#[tokio::test]
async fn link_refusal_keeps_the_session_alive() {
    let mut config = BrokerConfig::default();
    config.max_queues = 1;
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    // Declare the one allowed queue and publish to it.
    let producer = session
        .create_producer("/queues/only")
        .await
        .expect("producer declares the sole queue");
    producer.send(Message::text("hi")).await.expect("send");

    // A consumer on a *new* address is refused at the cap. However it surfaces
    // (error, or an immediately-detached link), it must not kill the session.
    let _refused = session.create_consumer("/queues/second").await;

    // The session is still alive: consume from the queue that does exist.
    let mut consumer = session
        .create_consumer("/queues/only")
        .await
        .expect("consumer on the still-live session");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "hi");
    consumer.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// An abrupt connection drop (crash — no detach/close) must requeue the
/// consumer's unsettled deliveries for the next consumer. Exercises the
/// broker's connection-death cleanup path, not the graceful-detach path.
#[tokio::test]
async fn abrupt_connection_drop_requeues_unacked() {
    let (addr, shutdown) = start().await;

    // Producer publishes one message on its own connection.
    let pc = connect(addr).await;
    let ps = pc.begin_session().await.expect("producer session");
    let producer = ps
        .create_producer("/queues/abrupt")
        .await
        .expect("producer");
    producer.send(Message::text("survive")).await.expect("send");

    // Consumer receives it but never settles, then the whole connection is
    // dropped without any detach/close — a simulated crash.
    let cc = connect(addr).await;
    let cs = cc.begin_session().await.expect("consumer session");
    let mut consumer = cs
        .create_consumer("/queues/abrupt")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "survive");
    drop(consumer);
    drop(cs);
    drop(cc); // dropping the Connection tears the driver task down → TCP closes

    // A fresh consumer (new connection) gets the requeued message.
    let cc2 = connect(addr).await;
    let cs2 = cc2.begin_session().await.expect("second consumer session");
    let mut c2 = cs2
        .create_consumer("/queues/abrupt")
        .await
        .expect("second consumer");
    let d2 = c2.recv().await.expect("redelivery after abrupt drop");
    assert_eq!(text_of(&d2), "survive");
    c2.accept(&d2).await.expect("accept");

    pc.close().await.expect("close producer");
    cc2.close().await.expect("close consumer");
    shutdown.shutdown();
}

#[tokio::test]
async fn store_and_forward_produce_then_consume() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    // Produce 10 messages BEFORE any consumer exists; each send's outcome is
    // the broker's accepted disposition.
    let producer = session
        .create_producer("/queues/saf")
        .await
        .expect("producer");
    for i in 0..10 {
        let outcome = producer
            .send(Message::text(format!("m{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "broker must accept the publish, got {outcome:?}"
        );
    }

    // Then consume them all, in order.
    let mut consumer = session
        .create_consumer("/queues/saf")
        .await
        .expect("consumer");
    for i in 0..10 {
        let delivery = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&delivery), format!("m{i}"));
        consumer.accept(&delivery).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn live_dispatch_consumer_first() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let mut consumer = session
        .create_consumer("/queues/live")
        .await
        .expect("consumer");
    let producer = session
        .create_producer("/queues/live")
        .await
        .expect("producer");

    producer
        .send(Message::text("hot path"))
        .await
        .expect("send");
    let delivery = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&delivery), "hot path");
    consumer.accept(&delivery).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn fire_and_forget_settled_sends_arrive() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/queues/ff")
        .await
        .expect("producer");
    for i in 0..5 {
        producer
            .send_settled(Message::text(format!("f{i}")))
            .await
            .expect("send_settled");
    }

    let mut consumer = session
        .create_consumer("/queues/ff")
        .await
        .expect("consumer");
    for i in 0..5 {
        let delivery = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&delivery), format!("f{i}"));
        consumer.accept(&delivery).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn competing_consumers_share_the_queue() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let mut c1 = session.create_consumer("/queues/comp").await.expect("c1");
    let mut c2 = session.create_consumer("/queues/comp").await.expect("c2");
    let producer = session
        .create_producer("/queues/comp")
        .await
        .expect("producer");

    for i in 0..10 {
        producer
            .send(Message::text(format!("c{i}")))
            .await
            .expect("send");
    }

    // Round-robin between two consumers with equal demand: each gets 5.
    let mut got1 = Vec::new();
    let mut got2 = Vec::new();
    for _ in 0..5 {
        let d1 = c1.recv().await.expect("c1 delivery");
        got1.push(text_of(&d1));
        c1.accept(&d1).await.expect("accept");
        let d2 = c2.recv().await.expect("c2 delivery");
        got2.push(text_of(&d2));
        c2.accept(&d2).await.expect("accept");
    }
    // All ten messages arrived exactly once across the two.
    let mut all: Vec<String> = got1.into_iter().chain(got2).collect();
    all.sort();
    let expected: Vec<String> = (0..10).map(|i| format!("c{i}")).collect();
    let mut expected = expected;
    expected.sort();
    assert_eq!(all, expected);

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn released_delivery_is_redelivered() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/queues/rel")
        .await
        .expect("producer");
    producer.send(Message::text("try me")).await.expect("send");

    let mut consumer = session
        .create_consumer("/queues/rel")
        .await
        .expect("consumer");
    let first = consumer.recv().await.expect("first delivery");
    assert_eq!(text_of(&first), "try me");
    // Decline to process it: released → back on the queue.
    consumer.release(&first).await.expect("release");

    // The same message comes around again.
    let second = consumer.recv().await.expect("redelivery");
    assert_eq!(text_of(&second), "try me");
    consumer.accept(&second).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn consumer_drop_requeues_unacked_for_the_next_consumer() {
    let (addr, shutdown) = start().await;
    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/queues/orphan")
        .await
        .expect("producer");
    producer
        .send(Message::text("orphaned"))
        .await
        .expect("send");

    // First consumer receives but never settles, then detaches.
    let mut c1 = session.create_consumer("/queues/orphan").await.expect("c1");
    let d = c1.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "orphaned");
    c1.detach().await.expect("detach without settling");

    // A new consumer gets the requeued message.
    let mut c2 = session.create_consumer("/queues/orphan").await.expect("c2");
    let d = c2.recv().await.expect("redelivery");
    assert_eq!(text_of(&d), "orphaned");
    c2.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn cross_connection_produce_consume() {
    let (addr, shutdown) = start().await;

    // Producer and consumer on separate connections (separate driver tasks).
    let pc = connect(addr).await;
    let cc = connect(addr).await;
    let ps = pc.begin_session().await.expect("producer session");
    let cs = cc.begin_session().await.expect("consumer session");

    let producer = ps.create_producer("/queues/x").await.expect("producer");
    let mut consumer = cs.create_consumer("/queues/x").await.expect("consumer");

    for i in 0..20 {
        producer
            .send(Message::text(format!("x{i}")))
            .await
            .expect("send");
    }
    for i in 0..20 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), format!("x{i}"));
        consumer.accept(&d).await.expect("accept");
    }

    pc.close().await.expect("close producer conn");
    cc.close().await.expect("close consumer conn");
    shutdown.shutdown();
}

#[tokio::test]
async fn queue_overflow_rejects_the_publish() {
    let mut config = BrokerConfig::default();
    config.max_queue_depth = 3;
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    let conn = connect(addr).await;
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/queues/tiny")
        .await
        .expect("producer");

    for i in 0..3 {
        producer
            .send(Message::text(format!("fits{i}")))
            .await
            .expect("fits");
    }
    // The fourth overflows: the broker answers `rejected` (the terminal
    // outcome of the send) rather than growing unbounded.
    let outcome = producer
        .send(Message::text("overflow"))
        .await
        .expect("send completes with a terminal outcome");
    match outcome {
        DeliveryState::Rejected(r) => {
            let err = r.error.expect("carries the broker's error");
            assert!(
                err.to_string().contains("resource-limit-exceeded"),
                "unexpected rejection error: {err}"
            );
        }
        other => panic!("expected rejected, got {other:?}"),
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}
