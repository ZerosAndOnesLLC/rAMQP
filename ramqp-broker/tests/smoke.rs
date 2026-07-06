//! Phase 3 smoke tests: the published `ramqp` client (unmodified) connects,
//! authenticates, opens sessions, and attaches links against this broker over
//! loopback TCP — no external broker involved.

use std::sync::Arc;

use ramqp::ConnectionBuilder;
use ramqp_broker::{Broker, BrokerConfig, StaticPlain};

async fn start(broker: Broker) -> (std::net::SocketAddr, ramqp_broker::ShutdownHandle) {
    let bound = broker.bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

#[tokio::test]
async fn client_connects_begins_and_attaches() {
    let (addr, shutdown) = start(Broker::new(BrokerConfig::default())).await;

    // SASL ANONYMOUS (no credentials in the URL).
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("client connects to our broker");

    let session = conn.begin_session().await.expect("session begins");

    // Peer-initiated attaches: the broker mirrors a receiver for our producer
    // and a sender for our consumer, and answers both.
    let producer = session
        .create_producer("/queues/smoke")
        .await
        .expect("producer attach accepted");
    let consumer = session
        .create_consumer("/queues/smoke")
        .await
        .expect("consumer attach accepted");

    drop(producer);
    drop(consumer);
    conn.close().await.expect("graceful close");
    shutdown.shutdown();
}

#[tokio::test]
async fn plain_auth_accepts_good_and_rejects_bad_credentials() {
    let auth = Arc::new(StaticPlain::new().with_user("alice", "secret"));
    let (addr, shutdown) =
        start(Broker::new(BrokerConfig::default()).with_authenticator(auth)).await;

    // Correct password.
    let conn = ConnectionBuilder::new(format!("amqp://alice:secret@{addr}"))
        .connect()
        .await
        .expect("valid PLAIN credentials accepted");
    conn.close().await.expect("close");

    // Wrong password → SASL failure surfaced by the client.
    let err = ConnectionBuilder::new(format!("amqp://alice:wrong@{addr}"))
        .connect()
        .await
        .expect_err("bad password rejected");
    assert_eq!(err.kind(), ramqp::error::ErrorKind::Sasl);

    // ANONYMOUS is not offered by StaticPlain → also a SASL failure.
    let err = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect_err("anonymous rejected");
    assert_eq!(err.kind(), ramqp::error::ErrorKind::Sasl);

    shutdown.shutdown();
}

#[tokio::test]
async fn sessions_can_end_and_reopen() {
    let (addr, shutdown) = start(Broker::new(BrokerConfig::default())).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");

    let s1 = conn.begin_session().await.expect("first session");
    let s2 = conn
        .begin_session()
        .await
        .expect("second session concurrently");
    s1.end().await.expect("first session ends");
    let s3 = conn.begin_session().await.expect("third session after end");
    s3.end().await.expect("third ends");
    drop(s2);

    conn.close().await.expect("close");
    shutdown.shutdown();
}

#[tokio::test]
async fn connection_limit_refuses_excess_then_recovers() {
    let config = BrokerConfig {
        max_connections: 1,
        ..Default::default()
    };
    let (addr, shutdown) = start(Broker::new(config)).await;

    // The first connection establishes and holds the sole permit.
    let c1 = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("first connection accepted");

    // A second connection is refused: the broker drops the socket without
    // completing the handshake, so the client's connect fails.
    let refused = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await;
    assert!(
        refused.is_err(),
        "second connection must be refused at the cap"
    );

    // Closing the first frees the permit; a fresh connection then succeeds
    // (the permit release is async, so allow a few attempts).
    c1.close().await.expect("close first");
    let mut recovered = None;
    for _ in 0..50 {
        if let Ok(c) = ConnectionBuilder::new(format!("amqp://{addr}"))
            .connect()
            .await
        {
            recovered = Some(c);
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }
    recovered
        .expect("a permit frees up after the first connection closes")
        .close()
        .await
        .expect("close recovered");

    shutdown.shutdown();
}

#[tokio::test]
async fn many_concurrent_connections() {
    let (addr, shutdown) = start(Broker::new(BrokerConfig::default())).await;

    let mut tasks = Vec::new();
    for i in 0..16 {
        let url = format!("amqp://{addr}");
        tasks.push(tokio::spawn(async move {
            let conn = ConnectionBuilder::new(&url)
                .connect()
                .await
                .expect("connect");
            let session = conn.begin_session().await.expect("session");
            let _p = session
                .create_producer(&format!("/queues/q{i}"))
                .await
                .expect("producer");
            conn.close().await.expect("close");
        }));
    }
    for t in tasks {
        t.await.expect("connection task");
    }
    shutdown.shutdown();
}
