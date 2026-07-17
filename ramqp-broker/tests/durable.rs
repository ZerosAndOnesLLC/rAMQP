//! Client-facing durable queue tests (`store-redb` feature): `/durable/<name>`
//! addresses persist to disk — a publish is acknowledged only after its
//! group-commit batch fsyncs, and messages survive a full broker restart.
#![cfg(feature = "store-redb")]

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig, ShutdownHandle};

async fn start(config: BrokerConfig) -> (std::net::SocketAddr, ShutdownHandle) {
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

fn config_with(dir: &std::path::Path) -> BrokerConfig {
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.to_path_buf());
    config
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
async fn durable_produce_consume_round_trip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    // Each accepted disposition is an fsynced-commit confirm.
    let producer = session
        .create_producer("/durable/orders")
        .await
        .expect("producer");
    for i in 0..10 {
        let outcome = producer
            .send(Message::text(format!("d{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "disk-commit-backed accept expected, got {outcome:?}"
        );
    }

    let mut consumer = session
        .create_consumer("/durable/orders")
        .await
        .expect("consumer");
    for i in 0..10 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), format!("d{i}"), "FIFO from the store");
        consumer.accept(&d).await.expect("accept");
    }

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Durable → durable dead-lettering (MED-12 from issue #19): the expired
/// message reaches the durable DLQ, riding the confirm-ordered path (the
/// source's Remove now waits for the copy's fate before committing).
#[tokio::test]
async fn durable_dead_letter_lands_in_a_durable_dlq() {
    let dir = tempfile::tempdir().expect("tempdir");
    let mut config = config_with(dir.path());
    let mut policy = ramqp_broker::QueuePolicy::default();
    policy.message_ttl = Some(std::time::Duration::from_millis(200));
    policy.dead_letter = Some("/durable/ttl-dlx".to_owned());
    config.policies.push(("ttl-src".to_owned(), policy));
    let (addr, shutdown) = start(config).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    let producer = session
        .create_producer("/durable/ttl-src")
        .await
        .expect("producer");
    producer
        .send(Message::text("stale"))
        .await
        .expect("fsynced send");
    tokio::time::sleep(std::time::Duration::from_millis(400)).await;

    // Attaching a consumer triggers dispatch → lazy expiry → dead-letter.
    let mut src = session
        .create_consumer("/durable/ttl-src")
        .await
        .expect("source consumer");
    let empty = tokio::time::timeout(std::time::Duration::from_millis(300), src.recv()).await;
    assert!(empty.is_err(), "expired message must not deliver");

    let mut dlq = session
        .create_consumer("/durable/ttl-dlx")
        .await
        .expect("dlq consumer");
    let d = tokio::time::timeout(std::time::Duration::from_secs(5), dlq.recv())
        .await
        .expect("dead letter in time")
        .expect("dead letter");
    assert_eq!(text_of(&d), "stale");
    dlq.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// The Phase 7 headline: accepted messages survive a full broker restart —
/// stop the broker, start a new one on the same data dir, and a fresh
/// consumer receives everything that was not settled.
#[tokio::test]
async fn durable_messages_survive_a_broker_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Broker #1: produce 5, consume-and-ack 2, then shut down.
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/durable/restart")
        .await
        .expect("producer");
    for i in 0..5 {
        producer
            .send(Message::text(format!("r{i}")))
            .await
            .expect("send");
    }
    let mut consumer = session
        .create_consumer("/durable/restart")
        .await
        .expect("consumer");
    for i in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), format!("r{i}"));
        consumer.accept(&d).await.expect("accept");
    }
    conn.close().await.expect("close");
    shutdown.shutdown();

    // Broker #2 on the same data dir. In-process, the store's file lock
    // frees only when broker #1's writer thread exits, so the first attaches
    // may be refused (surfacing as a detached link at `recv`) — retry until
    // the store opens; a real restart is a process boundary. An attach
    // refusal is deliberately NOT sticky (failed opens are retried).
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("reconnect");
    let session = conn.begin_session().await.expect("session");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
    let (mut consumer, first) = loop {
        let mut c = session
            .create_consumer("/durable/restart")
            .await
            .expect("attach");
        match tokio::time::timeout(std::time::Duration::from_secs(2), c.recv()).await {
            Ok(Ok(d)) => break (c, d),
            _ => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "store never became attachable after restart"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };
    // The three unsettled messages are recovered, in order.
    assert_eq!(text_of(&first), "r2", "recovered in FIFO order");
    consumer.accept(&first).await.expect("accept");
    for i in 3..5 {
        let d = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
            .await
            .expect("recovered delivery in time")
            .expect("delivery");
        assert_eq!(text_of(&d), format!("r{i}"), "recovered in FIFO order");
        consumer.accept(&d).await.expect("accept");
    }
    // And nothing settled came back.
    let extra = tokio::time::timeout(std::time::Duration::from_millis(300), consumer.recv()).await;
    assert!(extra.is_err(), "settled messages must not resurrect");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// `/durable/x`, `/queues/x`, and `/quorum/x` are three distinct queues.
#[tokio::test]
async fn durable_transient_and_quorum_coexist() {
    let dir = tempfile::tempdir().expect("tempdir");
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");

    let dp = session.create_producer("/durable/same").await.expect("dp");
    let tp = session.create_producer("/queues/same").await.expect("tp");
    dp.send(Message::text("durable")).await.expect("send d");
    tp.send(Message::text("transient")).await.expect("send t");

    let mut dc = session.create_consumer("/durable/same").await.expect("dc");
    let mut tc = session.create_consumer("/queues/same").await.expect("tc");
    assert_eq!(text_of(&dc.recv().await.expect("d recv")), "durable");
    assert_eq!(text_of(&tc.recv().await.expect("t recv")), "transient");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Without a data dir, a durable attach is refused (the link detaches with
/// `not-found`, surfacing at `recv`) but the session survives.
#[tokio::test]
async fn durable_without_data_dir_is_refused() {
    let (addr, shutdown) = start(BrokerConfig::default()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let mut consumer = session
        .create_consumer("/durable/nope")
        .await
        .expect("attach itself completes");
    let refused = tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv())
        .await
        .expect("refusal arrives promptly");
    assert!(
        refused.is_err(),
        "attach must be refused without a data dir, got {refused:?}"
    );
    // The session survives the refusal.
    let p = session
        .create_producer("/queues/ok")
        .await
        .expect("other links fine");
    drop(p);
    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Phase 7 quorum restart recovery: with `store-redb` + a data dir, a
/// quorum queue's Raft log/vote/snapshot persist — stop the broker, start a
/// new one on the same data dir, and unsettled replicated messages recover.
#[tokio::test]
async fn quorum_messages_survive_a_broker_restart() {
    let dir = tempfile::tempdir().expect("tempdir");

    // Broker #1: 5 commit-confirmed publishes, 2 consumed+acked.
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/quorum/qrestart")
        .await
        .expect("producer");
    for i in 0..5 {
        let outcome = producer
            .send(Message::text(format!("q{i}")))
            .await
            .expect("send");
        assert!(matches!(outcome, DeliveryState::Accepted(_)));
    }
    let mut consumer = session
        .create_consumer("/quorum/qrestart")
        .await
        .expect("consumer");
    for i in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        assert_eq!(text_of(&d), format!("q{i}"));
        consumer.accept(&d).await.expect("accept");
    }
    conn.close().await.expect("close");
    shutdown.shutdown();

    // Broker #2 on the same data dir: recovery replays the persisted log.
    // (In-process the redb lock frees when broker #1's store drops; the
    // first attaches may be refused until then — retry, as in the durable
    // restart test.)
    let (addr, shutdown) = start(config_with(dir.path())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("reconnect");
    let session = conn.begin_session().await.expect("session");
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let (mut consumer, first) = loop {
        let mut c = session
            .create_consumer("/quorum/qrestart")
            .await
            .expect("attach");
        match tokio::time::timeout(std::time::Duration::from_secs(2), c.recv()).await {
            Ok(Ok(d)) => break (c, d),
            _ => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "quorum queue never recovered after restart"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };
    assert_eq!(text_of(&first), "q2", "recovered in FIFO order");
    consumer.accept(&first).await.expect("accept");
    for i in 3..5 {
        let d = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
            .await
            .expect("recovered delivery in time")
            .expect("delivery");
        assert_eq!(text_of(&d), format!("q{i}"), "recovered in FIFO order");
        consumer.accept(&d).await.expect("accept");
    }
    // Settled messages never resurrect.
    let extra = tokio::time::timeout(std::time::Duration::from_millis(300), consumer.recv()).await;
    assert!(extra.is_err(), "acked messages must not resurrect");

    // And the recovered queue still accepts new work.
    let producer = session
        .create_producer("/quorum/qrestart")
        .await
        .expect("producer");
    let outcome = producer
        .send(Message::text("post-restart"))
        .await
        .expect("send");
    assert!(matches!(outcome, DeliveryState::Accepted(_)));
    let d = consumer.recv().await.expect("new delivery");
    assert_eq!(text_of(&d), "post-restart");
    consumer.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// The metadata catalog persists too: a clustered broker (single-node
/// cluster) declares a quorum queue, restarts on the same data dir, and the
/// recovered catalog + queue group serve the replicated messages again.
#[tokio::test(flavor = "multi_thread")]
async fn clustered_catalog_and_queue_survive_a_restart() {
    let dir = tempfile::tempdir().expect("tempdir");
    let cluster_config = |listen: String| {
        let mut config = BrokerConfig::default();
        config.cluster = Some(ramqp_broker::ClusterMemberConfig::new(
            1,
            listen.clone(),
            vec![(1, listen)],
        ));
        config.data_dir = Some(dir.path().to_path_buf());
        config
    };
    // Reserve a fabric port so both lives use the same seed address.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fabric_addr = l.local_addr().unwrap().to_string();
    drop(l);

    // Life #1: declare + publish through the catalog.
    let broker = Broker::new(cluster_config(fabric_addr.clone()));
    let bound = broker.clone().bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    assert!(
        broker
            .cluster_formed(std::time::Duration::from_secs(15))
            .await,
        "single-node cluster forms"
    );
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/quorum/cat-restart")
        .await
        .expect("producer");
    for i in 0..3 {
        let outcome = producer
            .send(Message::text(format!("c{i}")))
            .await
            .expect("send");
        assert!(matches!(outcome, DeliveryState::Accepted(_)));
    }
    conn.close().await.expect("close");
    shutdown.shutdown();
    // Drop OUR handle too: the registry (and its store lock) lives while any
    // Broker clone does.
    drop(broker);

    // Life #2: same data dir + fabric address. The bind itself may race the
    // released file lock in-process; retry the whole broker start.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(15);
    let (addr, shutdown) = loop {
        match Broker::new(cluster_config(fabric_addr.clone()))
            .bind("127.0.0.1:0")
            .await
        {
            Ok(bound) => {
                let addr = bound.local_addr();
                let shutdown = bound.shutdown_handle();
                tokio::spawn(bound.run());
                break (addr, shutdown);
            }
            Err(e) => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "clustered broker never restarted: {e}"
                );
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            }
        }
    };
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("reconnect");
    let session = conn.begin_session().await.expect("session");
    let (mut consumer, first) = loop {
        let mut c = session
            .create_consumer("/quorum/cat-restart")
            .await
            .expect("attach");
        match tokio::time::timeout(std::time::Duration::from_secs(2), c.recv()).await {
            Ok(Ok(d)) => break (c, d),
            _ => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "catalog/queue never recovered after clustered restart"
                );
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
            }
        }
    };
    assert_eq!(text_of(&first), "c0", "replicated messages recovered");
    consumer.accept(&first).await.expect("accept");
    for i in 1..3 {
        let d = tokio::time::timeout(std::time::Duration::from_secs(10), consumer.recv())
            .await
            .expect("in time")
            .expect("delivery");
        assert_eq!(text_of(&d), format!("c{i}"));
        consumer.accept(&d).await.expect("accept");
    }
    conn.close().await.expect("close");
    shutdown.shutdown();
}
