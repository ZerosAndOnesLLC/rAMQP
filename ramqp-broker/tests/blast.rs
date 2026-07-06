//! Sustained-load regression test: a settled blast far beyond the session
//! incoming-window, drained with batched ranged accepts. Guards the two
//! stall bugs Phase 4 shook out: the queue<->connection bounded-channel
//! deadlock cycle, and senders stalling permanently when a pure session flow
//! (no handle) reopened the remote incoming-window.
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig};

/// The same sustained-load shape against a QUORUM queue: every publish is a
/// Raft commit, so this also guards the quorum actor's await patterns
/// (mailbox-bounded publishes must backpressure producers without deadlock).
#[tokio::test]
async fn quorum_blast_with_ranged_accepts() {
    // Full size in release (the perf regression guard); scaled down in debug
    // so a plain `cargo test` stays fast.
    blast(
        "/quorum/blast",
        if cfg!(debug_assertions) { 800 } else { 5000 },
    )
    .await;
}

#[tokio::test]
async fn settled_blast_with_ranged_accepts() {
    blast(
        "/queues/blast",
        if cfg!(debug_assertions) { 3000 } else { 50000 },
    )
    .await;
}

async fn blast(address: &str, n: usize) {
    let bound = Broker::new(BrokerConfig::default())
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = bound.local_addr();
    tokio::spawn(bound.run());

    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(address).await.expect("p");
    let mut consumer = session.create_consumer(address).await.expect("c");

    for _ in 0..n {
        producer
            .send_settled(Message::data(vec![0u8; 256]))
            .await
            .expect("send");
    }
    let mut last = None;
    for i in 1..=n {
        let d = tokio::time::timeout(std::time::Duration::from_secs(20), consumer.recv())
            .await
            .unwrap_or_else(|_| panic!("recv #{i} timed out (stall regression)"))
            .expect("recv");
        if i == n {
            last = Some(d);
        } else if i % 64 == 0 {
            consumer.accept_through(&d).await.expect("accept_through");
        }
    }
    consumer
        .accept_through(&last.expect("last"))
        .await
        .expect("final accept");
    conn.close().await.expect("close");
}
