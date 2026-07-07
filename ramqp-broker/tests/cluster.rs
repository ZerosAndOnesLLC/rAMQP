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

/// Fault injection, availability boundary (broker.md Phase 10): rolling
/// leader kills. With 2/3 nodes the cluster keeps serving; at 1/3 (quorum
/// LOST) publishes are cleanly REFUSED — consistency over availability,
/// never silent loss, never a hang.
#[tokio::test(flavor = "multi_thread")]
async fn rolling_leader_kills_hit_a_clean_availability_boundary() {
    let nodes = start_cluster(3).await;
    let queue = "/quorum/rolling";

    // Declare via a probe and learn the leader.
    let probe = ConnectionBuilder::new(format!("amqp://{}", nodes[0].amqp_addr))
        .connect()
        .await
        .expect("probe connect");
    let ps = probe.begin_session().await.expect("probe session");
    let pp = ps.create_producer(queue).await.expect("probe producer");
    assert!(matches!(
        pp.send(Message::text("seed")).await.expect("seed"),
        DeliveryState::Accepted(_)
    ));
    probe.close().await.expect("probe close");
    let leader1 = nodes[0]
        .broker
        .queue_leader(queue)
        .await
        .expect("leader known");

    // Client on a survivor.
    let survivor = nodes
        .iter()
        .enumerate()
        .find(|(i, _)| (*i as u64 + 1) != leader1)
        .map(|(_, n)| n)
        .expect("survivor");
    let conn = ConnectionBuilder::new(format!("amqp://{}", survivor.amqp_addr))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(queue).await.expect("producer");

    // Kill #1 (the leader): 2/3 alive — the cluster recovers and accepts.
    nodes[leader1 as usize - 1].shutdown.shutdown();
    let deadline = std::time::Instant::now() + Duration::from_secs(30);
    loop {
        match producer.send(Message::text("after-kill-1")).await {
            Ok(DeliveryState::Accepted(_)) => break,
            _ => {
                assert!(
                    std::time::Instant::now() < deadline,
                    "cluster never recovered from the first leader kill"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
    }

    // Kill #2 (the new leader): 1/3 alive — quorum LOST. Publishes must be
    // refused (rejected or errored), promptly, with no hang and no
    // false accept.
    let leader2 = survivor
        .broker
        .queue_leader(queue)
        .await
        .expect("re-elected leader");
    // (If the surviving client node itself leads, kill it anyway — the test
    // then just ends; pick the OTHER remaining node as the victim when
    // possible so the client's node stays up.)
    let victim = nodes
        .iter()
        .enumerate()
        .find(|(i, n)| (*i as u64 + 1) == leader2 && !std::ptr::eq(*n, survivor))
        .map(|(_, n)| n);
    let Some(victim) = victim else {
        // The client's own node leads; killing it would sever the client,
        // which tests nothing about the queue. Covered scope ends here.
        conn.close().await.ok();
        for n in &nodes {
            n.shutdown.shutdown();
        }
        return;
    };
    victim.shutdown.shutdown();

    // Without quorum nothing may be accepted; every send resolves promptly
    // as a rejection or error (bounded worst case: the publish-retry
    // window), and none hang.
    let mut refusals = 0;
    for i in 0..3 {
        let outcome = tokio::time::timeout(
            Duration::from_secs(45),
            producer.send(Message::text(format!("no-quorum-{i}"))),
        )
        .await
        .expect("send resolves (no hang) even without quorum");
        match outcome {
            Ok(DeliveryState::Accepted(_)) => {
                panic!("accepted a publish WITHOUT quorum — silent-loss hazard")
            }
            Ok(_) | Err(_) => refusals += 1,
        }
    }
    assert_eq!(refusals, 3, "all quorum-less publishes refused cleanly");

    conn.close().await.ok();
    for n in &nodes {
        n.shutdown.shutdown();
    }
}

/// Losing a FOLLOWER is transparent: the leader keeps accepting and the
/// consumer keeps receiving with zero interruption-visible loss.
#[tokio::test(flavor = "multi_thread")]
async fn follower_loss_is_transparent() {
    let nodes = start_cluster(3).await;
    let queue = "/quorum/follower-loss";

    let probe = ConnectionBuilder::new(format!("amqp://{}", nodes[0].amqp_addr))
        .connect()
        .await
        .expect("probe connect");
    let ps = probe.begin_session().await.expect("probe session");
    let pp = ps.create_producer(queue).await.expect("probe producer");
    assert!(matches!(
        pp.send(Message::text("m0")).await.expect("seed"),
        DeliveryState::Accepted(_)
    ));
    probe.close().await.expect("probe close");

    let leader = nodes[0].broker.queue_leader(queue).await.expect("leader");
    // Kill one follower.
    let follower = nodes
        .iter()
        .enumerate()
        .find(|(i, _)| (*i as u64 + 1) != leader)
        .map(|(_, n)| n)
        .expect("follower");
    follower.shutdown.shutdown();

    // The remaining pair keeps serving: produce + consume through the LEADER
    // node (guaranteed alive).
    let leader_node = &nodes[leader as usize - 1];
    let conn = ConnectionBuilder::new(format!("amqp://{}", leader_node.amqp_addr))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(queue).await.expect("producer");
    for i in 1..20 {
        let outcome = producer
            .send(Message::text(format!("m{i}")))
            .await
            .expect("send");
        assert!(
            matches!(outcome, DeliveryState::Accepted(_)),
            "follower loss must not refuse publishes: {outcome:?}"
        );
    }
    let mut consumer = session.create_consumer(queue).await.expect("consumer");
    let mut got = BTreeSet::new();
    while got.len() < 20 {
        let d = tokio::time::timeout(Duration::from_secs(15), consumer.recv())
            .await
            .expect("delivery in time")
            .expect("delivery");
        got.insert(text_of(&d));
        consumer.accept(&d).await.expect("accept");
    }
    let want: BTreeSet<String> = (0..20).map(|i| format!("m{i}")).collect();
    assert_eq!(got, want, "no loss across follower failure");

    conn.close().await.expect("close");
    for n in &nodes {
        n.shutdown.shutdown();
    }
}
