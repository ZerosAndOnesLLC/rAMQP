//! Cross-implementation interop (broker.md Phase 10): the independent
//! `fe2o3-amqp` client against `ramqp-broker` — a different clean-room AMQP
//! 1.0 implementation exercising our server-side handshake, session/link
//! management, transfers, dispositions, and flow control.

use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
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

/// Produce/consume round trip with per-message settlement.
#[tokio::test]
async fn fe2o3_produce_consume_round_trip() {
    let (addr, shutdown) = start().await;
    let mut conn = Connection::open("fe2o3-interop", format!("amqp://{addr}").as_str())
        .await
        .expect("fe2o3 connect (server handshake interop)");
    let mut session = Session::begin(&mut conn).await.expect("session");

    let mut sender = Sender::attach(&mut session, "interop-send", "/queues/fe2o3")
        .await
        .expect("producer attach");
    for i in 0..50 {
        sender
            .send(format!("interop-{i}").as_str())
            .await
            .expect("send (broker disposition interop)");
    }
    sender.close().await.expect("sender close");

    let mut receiver = Receiver::builder()
        .name("interop-recv")
        .source("/queues/fe2o3")
        .credit_mode(CreditMode::Auto(64))
        .attach(&mut session)
        .await
        .expect("consumer attach");
    for i in 0..50 {
        let d = tokio::time::timeout(Duration::from_secs(10), receiver.recv::<String>())
            .await
            .expect("delivery in time")
            .expect("delivery");
        assert_eq!(
            d.body(),
            &format!("interop-{i}"),
            "FIFO across implementations"
        );
        receiver.accept(&d).await.expect("accept");
    }
    receiver.close().await.expect("receiver close");
    session.end().await.expect("session end");
    conn.close().await.expect("connection close");
    shutdown.shutdown();
}

/// The release/redelivery path across implementations.
#[tokio::test]
async fn fe2o3_release_redelivers() {
    let (addr, shutdown) = start().await;
    let mut conn = Connection::open("fe2o3-redeliver", format!("amqp://{addr}").as_str())
        .await
        .expect("connect");
    let mut session = Session::begin(&mut conn).await.expect("session");
    let mut sender = Sender::attach(&mut session, "s", "/queues/fe2o3-r")
        .await
        .expect("producer");
    sender.send("try-again").await.expect("send");
    sender.close().await.expect("close");

    let mut receiver = Receiver::attach(&mut session, "r", "/queues/fe2o3-r")
        .await
        .expect("consumer");
    let d = receiver.recv::<String>().await.expect("first");
    receiver.release(&d).await.expect("release");
    let d = tokio::time::timeout(Duration::from_secs(10), receiver.recv::<String>())
        .await
        .expect("redelivery in time")
        .expect("redelivery");
    assert_eq!(d.body(), "try-again");
    receiver.accept(&d).await.expect("accept");
    receiver.close().await.expect("close");
    session.end().await.expect("end");
    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Quorum queues speak the same wire protocol: fe2o3 against `/quorum/*`
/// gets Raft-commit-backed accepts.
#[tokio::test]
async fn fe2o3_quorum_queue_round_trip() {
    let (addr, shutdown) = start().await;
    let mut conn = Connection::open("fe2o3-quorum", format!("amqp://{addr}").as_str())
        .await
        .expect("connect");
    let mut session = Session::begin(&mut conn).await.expect("session");
    let mut sender = Sender::attach(&mut session, "qs", "/quorum/fe2o3-q")
        .await
        .expect("producer");
    sender.send("replicated").await.expect("commit-backed send");
    sender.close().await.expect("close");

    let mut receiver = Receiver::attach(&mut session, "qr", "/quorum/fe2o3-q")
        .await
        .expect("consumer");
    let d = receiver.recv::<String>().await.expect("delivery");
    assert_eq!(d.body(), "replicated");
    receiver.accept(&d).await.expect("accept");
    receiver.close().await.expect("close");
    session.end().await.expect("end");
    conn.close().await.expect("close");
    shutdown.shutdown();
}
