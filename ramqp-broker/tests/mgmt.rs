//! Management endpoint tests (broker.md Phase 9): Prometheus metrics and
//! queue inspection, collected off the hot path.

use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn http_get(addr: &std::net::SocketAddr, path: &str) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect mgmt");
    stream
        .write_all(format!("GET {path} HTTP/1.1\r\nhost: x\r\n\r\n").as_bytes())
        .await
        .expect("write");
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.expect("read");
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .expect("status");
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

#[tokio::test]
async fn metrics_and_queue_inspection() {
    // Reserve a management port.
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgmt_addr = l.local_addr().unwrap();
    drop(l);

    let config = BrokerConfig {
        management_listen: Some(mgmt_addr.to_string()),
        ..Default::default()
    };
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    // Create some observable state: a queue with 2 ready messages + a consumer.
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer("/queues/watched").await.expect("p");
    for i in 0..2 {
        producer
            .send(Message::text(format!("w{i}")))
            .await
            .expect("send");
    }

    // /metrics: Prometheus text with our gauges.
    let (status, body) = http_get(&mgmt_addr, "/metrics").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("ramqp_connections 1"),
        "one live connection: {body}"
    );
    assert!(body.contains("ramqp_process_resident_bytes"));
    assert!(
        body.contains("ramqp_queue_ready{queue=\"watched\",kind=\"transient\"} 2"),
        "queue depth gauge: {body}"
    );

    // /queues: JSON inspection.
    let (status, body) = http_get(&mgmt_addr, "/queues").await;
    assert_eq!(status, 200);
    assert!(
        body.contains("\"name\":\"watched\"") && body.contains("\"ready\":2"),
        "queue listing: {body}"
    );

    // Unknown path → 404; non-GET → 405.
    let (status, _) = http_get(&mgmt_addr, "/nope").await;
    assert_eq!(status, 404);

    conn.close().await.expect("close");
    shutdown.shutdown();
}
