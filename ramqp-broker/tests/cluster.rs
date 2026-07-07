//! Client-facing cluster tests: the unmodified `ramqp` client against a
//! 3-node broker cluster — any node serves any queue through the forwarding
//! fabric, and killing a quorum queue's leader mid-stream loses nothing that
//! was acknowledged (broker.md Phase 6).

use std::collections::BTreeSet;
use std::time::Duration;

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig, ClusterMemberConfig};

struct Node {
    broker: Broker,
    amqp_addr: std::net::SocketAddr,
    shutdown: ramqp_broker::ShutdownHandle,
}

/// Reserve `n` loopback ports for the fabric, then release them for the
/// nodes to re-bind (small race window; fine for tests).
async fn reserve_addrs(n: usize) -> Vec<String> {
    let mut addrs = Vec::new();
    for _ in 0..n {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        addrs.push(l.local_addr().unwrap().to_string());
        drop(l);
    }
    addrs
}

/// Start an `n`-node cluster, each node serving AMQP on an ephemeral port.
async fn start_cluster(n: usize) -> Vec<Node> {
    let fabric_addrs = reserve_addrs(n).await;
    let seeds: Vec<(u64, String)> = (1..=n as u64).zip(fabric_addrs).collect();
    let mut nodes = Vec::new();
    for (id, fabric_addr) in &seeds {
        let config = BrokerConfig {
            cluster: Some(ClusterMemberConfig::new(
                *id,
                fabric_addr.clone(),
                seeds.clone(),
            )),
            ..Default::default()
        };
        let broker = Broker::new(config);
        let bound = broker
            .clone()
            .bind("127.0.0.1:0")
            .await
            .expect("bind AMQP listener");
        let amqp_addr = bound.local_addr();
        let shutdown = bound.shutdown_handle();
        tokio::spawn(bound.run());
        nodes.push(Node {
            broker,
            amqp_addr,
            shutdown,
        });
    }
    for node in &nodes {
        assert!(
            node.broker.cluster_formed(Duration::from_secs(15)).await,
            "cluster formed on every node"
        );
    }
    nodes
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

/// Any node serves any quorum queue: produce through node 0, consume the
/// same queue through node 2 — the fabric routes both to wherever the
/// queue group's leader lives.
#[tokio::test(flavor = "multi_thread")]
async fn any_node_serves_any_queue() {
    let nodes = start_cluster(3).await;

    let pconn = ConnectionBuilder::new(format!("amqp://{}", nodes[0].amqp_addr))
        .connect()
        .await
        .expect("producer connect");
    let psession = pconn.begin_session().await.expect("session");
    let producer = psession
        .create_producer("/quorum/dist")
        .await
        .expect("producer");
    for i in 0..10 {
        let outcome = producer
            .send(Message::text(format!("d{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "commit-backed accept, got {outcome:?}"
        );
    }

    let cconn = ConnectionBuilder::new(format!("amqp://{}", nodes[2].amqp_addr))
        .connect()
        .await
        .expect("consumer connect");
    let csession = cconn.begin_session().await.expect("session");
    let mut consumer = csession
        .create_consumer("/quorum/dist")
        .await
        .expect("consumer");
    let mut got = BTreeSet::new();
    for _ in 0..10 {
        let d = tokio::time::timeout(Duration::from_secs(15), consumer.recv())
            .await
            .expect("delivery in time")
            .expect("delivery");
        got.insert(text_of(&d));
        consumer.accept(&d).await.expect("accept");
    }
    let want: BTreeSet<String> = (0..10).map(|i| format!("d{i}")).collect();
    assert_eq!(got, want, "every message crossed the fabric");

    pconn.close().await.expect("close producer conn");
    cconn.close().await.expect("close consumer conn");
    for node in &nodes {
        node.shutdown.shutdown();
    }
}

/// The Phase 6 headline, client-facing: produce to a quorum queue, kill the
/// LEADER NODE mid-stream, and the consumer — attached to a survivor —
/// receives every acknowledged message (zero committed loss,
/// at-least-once).
#[tokio::test(flavor = "multi_thread")]
async fn kill_leader_mid_stream_loses_nothing() {
    let nodes = start_cluster(3).await;
    let queue = "/quorum/failover";

    // Declare via a throwaway probe so the leader is knowable, then place
    // producer and consumer on the two NON-leader nodes.
    let probe = ConnectionBuilder::new(format!("amqp://{}", nodes[0].amqp_addr))
        .connect()
        .await
        .expect("probe connect");
    let probe_session = probe.begin_session().await.expect("probe session");
    let probe_producer = probe_session
        .create_producer(queue)
        .await
        .expect("probe producer");
    let outcome = probe_producer
        .send(Message::text("probe"))
        .await
        .expect("probe send");
    assert!(matches!(outcome, DeliveryState::Accepted(_)));
    probe.close().await.expect("probe close");

    let leader = nodes[0]
        .broker
        .queue_leader(queue)
        .await
        .expect("queue group has a leader");
    let survivors: Vec<&Node> = nodes
        .iter()
        .enumerate()
        .filter(|(i, _)| (*i as u64 + 1) != leader)
        .map(|(_, n)| n)
        .collect();
    assert_eq!(survivors.len(), 2);

    let pconn = ConnectionBuilder::new(format!("amqp://{}", survivors[0].amqp_addr))
        .connect()
        .await
        .expect("producer connect");
    let psession = pconn.begin_session().await.expect("session");
    let producer = psession.create_producer(queue).await.expect("producer");

    let cconn = ConnectionBuilder::new(format!("amqp://{}", survivors[1].amqp_addr))
        .connect()
        .await
        .expect("consumer connect");
    let csession = cconn.begin_session().await.expect("session");
    let mut consumer = csession.create_consumer(queue).await.expect("consumer");

    // Send with retry: only ACCEPTED sends join the zero-loss contract
    // (rejections during the election window are the producer's cue to
    // retry — the standard publisher-confirm pattern).
    async fn send_accepted(producer: &ramqp::Producer, text: String) {
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        loop {
            match producer.send(Message::text(text.clone())).await {
                Ok(DeliveryState::Accepted(_)) => return,
                Ok(_) | Err(_) => {
                    assert!(
                        std::time::Instant::now() < deadline,
                        "send of {text} never accepted"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    // Phase 1: 50 messages with the leader alive.
    for i in 0..50 {
        send_accepted(&producer, format!("m{i}")).await;
    }

    // KILL the leader node mid-stream.
    let leader_node = &nodes[leader as usize - 1];
    leader_node.shutdown.shutdown();

    // Phase 2: 50 more through the failover window.
    for i in 50..100 {
        send_accepted(&producer, format!("m{i}")).await;
    }

    // The consumer (never touched the dead node) receives every accepted
    // message. At-least-once: duplicates possible across the failover —
    // dedupe by content.
    let mut got: BTreeSet<String> = BTreeSet::new();
    let deadline = std::time::Instant::now() + Duration::from_secs(90);
    while got.len() < 101 {
        assert!(
            std::time::Instant::now() < deadline,
            "only {}/101 messages arrived after the leader kill",
            got.len()
        );
        match tokio::time::timeout(Duration::from_secs(15), consumer.recv()).await {
            Ok(Ok(d)) => {
                got.insert(text_of(&d));
                let _ = consumer.accept(&d).await;
            }
            _ => continue,
        }
    }
    let mut want: BTreeSet<String> = (0..100).map(|i| format!("m{i}")).collect();
    want.insert("probe".to_owned());
    assert_eq!(
        got, want,
        "zero accepted-message loss across the leader kill"
    );

    pconn.close().await.expect("close producer conn");
    cconn.close().await.expect("close consumer conn");
    for node in survivors {
        node.shutdown.shutdown();
    }
}
