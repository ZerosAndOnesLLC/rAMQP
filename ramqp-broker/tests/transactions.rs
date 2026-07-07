//! Client-facing transaction tests (broker.md Phase 8): the
//! `amqp:coordinator` target — declare, transactional publishes and
//! settlements, commit and rollback — driven by the unmodified `ramqp`
//! client's `TransactionController`.

use std::time::Duration;

use ramqp::types::messaging::{Accepted, DeliveryState, Outcome};
use ramqp::{ConnectionBuilder, Message, txn};
use ramqp_broker::{Broker, BrokerConfig, ShutdownHandle};

async fn start() -> (std::net::SocketAddr, ShutdownHandle) {
    let bound = Broker::new(BrokerConfig::default())
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
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

/// Committed publishes become visible atomically; nothing leaks before the
/// discharge.
#[tokio::test]
async fn transactional_publishes_commit_atomically() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/queues/txn-commit")
        .await
        .expect("producer");
    let mut consumer = session
        .create_consumer("/queues/txn-commit")
        .await
        .expect("consumer");

    let txn_id = ctl.declare().await.expect("declare");
    for i in 0..3 {
        producer
            .send_with_state(
                Message::text(format!("t{i}")),
                txn::transactional_state(txn_id.clone(), None),
            )
            .await
            .expect("staged send");
    }
    // Before commit: nothing is in the queue.
    let early = tokio::time::timeout(Duration::from_millis(300), consumer.recv()).await;
    assert!(early.is_err(), "staged publishes must not be visible");

    ctl.commit(txn_id).await.expect("commit");
    for i in 0..3 {
        let d = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
            .await
            .expect("committed delivery in time")
            .expect("delivery");
        assert_eq!(text_of(&d), format!("t{i}"), "committed in staging order");
        consumer.accept(&d).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Rolled-back publishes vanish without a trace.
#[tokio::test]
async fn transactional_publishes_roll_back() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/queues/txn-abort")
        .await
        .expect("producer");

    let txn_id = ctl.declare().await.expect("declare");
    for i in 0..2 {
        producer
            .send_with_state(
                Message::text(format!("a{i}")),
                txn::transactional_state(txn_id.clone(), None),
            )
            .await
            .expect("staged send");
    }
    ctl.rollback(txn_id).await.expect("rollback");

    // Only a fresh (non-transactional) publish arrives.
    producer
        .send(Message::text("after"))
        .await
        .expect("plain send");
    let mut consumer = session
        .create_consumer("/queues/txn-abort")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(
        text_of(&d),
        "after",
        "rolled-back publishes must not appear"
    );
    consumer.accept(&d).await.expect("accept");
    let extra = tokio::time::timeout(Duration::from_millis(300), consumer.recv()).await;
    assert!(extra.is_err());

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Transactional settlements: acks stage under the txn — commit applies
/// them, rollback requeues the messages.
#[tokio::test]
async fn transactional_settlements_commit_and_roll_back() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/queues/txn-ack")
        .await
        .expect("producer");
    for i in 0..2 {
        producer
            .send(Message::text(format!("m{i}")))
            .await
            .expect("send");
    }
    let mut consumer = session
        .create_consumer("/queues/txn-ack")
        .await
        .expect("consumer");

    // Round 1: settle both inside a txn, then ROLL BACK → both redeliver.
    let txn_id = ctl.declare().await.expect("declare");
    for _ in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        consumer
            .settle_in_txn(&d, txn_id.clone(), Outcome::Accepted(Accepted::default()))
            .await
            .expect("staged settle");
    }
    ctl.rollback(txn_id).await.expect("rollback");
    let mut redelivered = Vec::new();
    for _ in 0..2 {
        let d = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
            .await
            .expect("redelivery in time")
            .expect("redelivery");
        redelivered.push(text_of(&d));
        // Round 2: settle inside a NEW txn and COMMIT → gone for good.
        // (Settle after the loop; hold deliveries first.)
        let txn2 = ctl.declare().await.expect("declare 2");
        consumer
            .settle_in_txn(&d, txn2.clone(), Outcome::Accepted(Accepted::default()))
            .await
            .expect("staged settle 2");
        ctl.commit(txn2).await.expect("commit");
    }
    redelivered.sort();
    assert_eq!(redelivered, vec!["m0".to_owned(), "m1".to_owned()]);

    // Nothing left.
    let extra = tokio::time::timeout(Duration::from_millis(300), consumer.recv()).await;
    assert!(extra.is_err(), "committed acks must not redeliver");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// The coordinator is cluster-aware for free: staged enqueues commit
/// through the queue layer's own confirms — for a quorum queue that is a
/// Raft commit, so a committed transaction's messages carry replicated
/// durability.
#[tokio::test]
async fn transactions_commit_into_quorum_queues() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/quorum/txn-q")
        .await
        .expect("producer");

    let txn_id = ctl.declare().await.expect("declare");
    producer
        .send_with_state(
            Message::text("replicated"),
            txn::transactional_state(txn_id.clone(), None),
        )
        .await
        .expect("staged send");
    ctl.commit(txn_id).await.expect("commit (raft-backed)");

    let mut consumer = session
        .create_consumer("/quorum/txn-q")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "replicated");
    consumer.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// A commit spanning multiple queues where one refuses (full, no drop-head)
/// must apply NOTHING — the healthy queue stays empty and the discharge is
/// rejected (the CRIT-1 atomicity regression from issue #19).
#[tokio::test]
async fn commit_with_a_full_queue_applies_nothing() {
    let mut config = BrokerConfig::default();
    config.policies.push((
        "txn-full".to_owned(),
        ramqp_broker::QueuePolicy {
            max_length: Some(1),
            ..Default::default()
        },
    ));
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let full_producer = session
        .create_producer("/queues/txn-full")
        .await
        .expect("producer (bounded queue)");
    let ok_producer = session
        .create_producer("/queues/txn-ok")
        .await
        .expect("producer (healthy queue)");

    // Fill the bounded queue outside the transaction.
    full_producer
        .send(Message::text("occupier"))
        .await
        .expect("fill the bounded queue");

    // Stage into the healthy queue FIRST (the old sequential commit would
    // land this before discovering the full queue), then into the full one.
    let txn_id = ctl.declare().await.expect("declare");
    ok_producer
        .send_with_state(
            Message::text("must-not-land"),
            txn::transactional_state(txn_id.clone(), None),
        )
        .await
        .expect("staged send (healthy)");
    full_producer
        .send_with_state(
            Message::text("refused"),
            txn::transactional_state(txn_id.clone(), None),
        )
        .await
        .expect("staged send (full)");
    let err = ctl
        .commit(txn_id)
        .await
        .expect_err("commit must fail: one target queue is full");
    assert!(
        err.to_string().contains("rejected"),
        "expected a rejected discharge, got: {err}"
    );

    // Atomicity: the healthy queue saw nothing from the failed transaction.
    ok_producer
        .send(Message::text("marker"))
        .await
        .expect("plain send");
    let mut consumer = session
        .create_consumer("/queues/txn-ok")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(
        text_of(&d),
        "marker",
        "a staged publish from the failed transaction leaked"
    );
    consumer.accept(&d).await.expect("accept");
    let extra = tokio::time::timeout(Duration::from_millis(300), consumer.recv()).await;
    assert!(extra.is_err(), "no further messages expected");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Discharging an unknown transaction is rejected, not silently accepted.
#[tokio::test]
async fn unknown_transaction_discharge_is_rejected() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let err = ctl
        .commit(bytes::Bytes::from_static(b"no-such-txn"))
        .await
        .expect_err("unknown txn must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("rejected"),
        "expected a rejected discharge, got: {msg}"
    );
    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// A transactional settlement that races its own discharge (the disposition
/// arrives after the discharge frame) must requeue the message, not strand
/// it invisibly in the unacked map (HIGH-5 from issue #19).
#[tokio::test]
async fn settle_after_discharge_requeues_instead_of_stranding() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/queues/txn-late-settle")
        .await
        .expect("producer");
    producer.send(Message::text("m")).await.expect("send");
    let mut consumer = session
        .create_consumer("/queues/txn-late-settle")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");

    // Discharge the transaction FIRST, then send the transactional settle —
    // by the time the disposition reaches the broker the txn is gone.
    let txn_id = ctl.declare().await.expect("declare");
    ctl.commit(txn_id.clone()).await.expect("commit");
    consumer
        .settle_in_txn(&d, txn_id, Outcome::Accepted(Accepted::default()))
        .await
        .expect("late transactional settle");

    // The broker must requeue the message (at-least-once), not strand it.
    let again = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
        .await
        .expect("requeued redelivery in time")
        .expect("redelivery");
    assert_eq!(text_of(&again), "m", "the late-settled message redelivers");
    consumer.accept(&again).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// A dropped connection rolls its transactions back implicitly.
#[tokio::test]
async fn connection_close_rolls_back_open_transactions() {
    let (addr, shutdown) = start().await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let ctl = session
        .create_transaction_controller()
        .await
        .expect("controller");
    let producer = session
        .create_producer("/queues/txn-orphan")
        .await
        .expect("producer");
    let txn_id = ctl.declare().await.expect("declare");
    producer
        .send_with_state(
            Message::text("orphaned"),
            txn::transactional_state(txn_id, None),
        )
        .await
        .expect("staged send");
    // Close WITHOUT discharging.
    conn.close().await.expect("close");

    // A fresh connection sees an empty queue.
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("reconnect");
    let session = conn.begin_session().await.expect("session");
    let mut consumer = session
        .create_consumer("/queues/txn-orphan")
        .await
        .expect("consumer");
    let extra = tokio::time::timeout(Duration::from_millis(400), consumer.recv()).await;
    assert!(extra.is_err(), "implicitly rolled-back publish leaked");
    conn.close().await.expect("close");
    shutdown.shutdown();
}

// Quiet the unused-import lint when the transaction feature shapes differ.
#[allow(unused_imports)]
use DeliveryState as _;
