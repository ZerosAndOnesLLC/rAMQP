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

    let mut config = BrokerConfig::default();
    config.management_listen = Some(mgmt_addr.to_string());
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

/// One HTTP request with an optional bearer token / basic auth and body.
async fn http(
    addr: &std::net::SocketAddr,
    method: &str,
    path: &str,
    auth: Option<&str>,
    body: Option<&str>,
) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr)
        .await
        .expect("connect mgmt");
    let body = body.unwrap_or("");
    let auth = auth
        .map(|a| format!("authorization: {a}\r\n"))
        .unwrap_or_default();
    stream
        .write_all(
            format!(
                "{method} {path} HTTP/1.1\r\nhost: x\r\n{auth}content-length: {}\r\n\r\n{body}",
                body.len()
            )
            .as_bytes(),
        )
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

const TOKEN: &str = "test-token-0123456789";
const BEARER: &str = "Bearer test-token-0123456789";

async fn start_admin(
    users: Option<ramqp_broker::StaticUsers>,
    auth_reads: bool,
) -> (
    std::net::SocketAddr,
    std::net::SocketAddr,
    ramqp_broker::ShutdownHandle,
) {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgmt_addr = l.local_addr().unwrap();
    drop(l);
    let mut config = BrokerConfig::default();
    config.management_listen = Some(mgmt_addr.to_string());
    config.management_token = Some(ramqp_broker::SecretString::new(TOKEN));
    config.management_auth_reads = auth_reads;
    let mut broker = Broker::new(config);
    if let Some(u) = users {
        broker = broker.with_authenticator(std::sync::Arc::new(u));
    }
    let bound = broker.bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, mgmt_addr, shutdown)
}

/// Issue #37 §3: declare/delete queues, exchanges, and bindings over HTTP —
/// idempotent, 409 on conflicting arguments, authenticated writes — and a
/// single queue's stats for autoscalers.
#[tokio::test]
async fn admin_api_declares_routes_and_deletes() {
    let (addr, mgmt, shutdown) = start_admin(None, false).await;

    // Writes need credentials; reads do not (by default).
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/jobs",
        None,
        Some(r#"{"kind":"durable"}"#),
    )
    .await;
    assert_eq!(s, 401);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/jobs",
        Some("Bearer wrong-token-xxxxxxxx"),
        None,
    )
    .await;
    assert_eq!(s, 401);

    // Queues: 201 then 204; a different kind under the same name is 409.
    let (s, _) = http(&mgmt, "PUT", "/queues/%2F/jobs", Some(BEARER), None).await;
    assert_eq!(s, 201);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/jobs",
        Some(BEARER),
        Some(r#"{"kind":"transient"}"#),
    )
    .await;
    assert_eq!(s, 204);
    let (s, body) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/jobs",
        Some(BEARER),
        Some(r#"{"kind":"quorum"}"#),
    )
    .await;
    assert_eq!(s, 409, "{body}");
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/jobs",
        Some(BEARER),
        Some("{not json"),
    )
    .await;
    assert_eq!(s, 400);

    // Exchanges + bindings.
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/exchanges/%2F/events",
        Some(BEARER),
        Some(r#"{"type":"topic"}"#),
    )
    .await;
    assert_eq!(s, 201);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/exchanges/%2F/events",
        Some(BEARER),
        Some(r#"{"type":"topic"}"#),
    )
    .await;
    assert_eq!(s, 204);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/exchanges/%2F/events",
        Some(BEARER),
        Some(r#"{"type":"fanout"}"#),
    )
    .await;
    assert_eq!(s, 409);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/exchanges/%2F/x",
        Some(BEARER),
        Some(r#"{"type":"headers"}"#),
    )
    .await;
    assert_eq!(s, 400);
    let bind = r#"{"queue":"/queues/jobs","key":"job.*"}"#;
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/bindings/%2F/events",
        Some(BEARER),
        Some(bind),
    )
    .await;
    assert_eq!(s, 201);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/bindings/%2F/events",
        Some(BEARER),
        Some(bind),
    )
    .await;
    assert_eq!(s, 204);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/bindings/%2F/missing",
        Some(BEARER),
        Some(bind),
    )
    .await;
    assert_eq!(s, 404);
    let (s, body) = http(&mgmt, "GET", "/exchanges/%2F/events", None, None).await;
    assert_eq!(s, 200);
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["type"], "topic");
    assert_eq!(v["bindings"][0]["queue"], "/queues/jobs");
    assert_eq!(v["bindings"][0]["key"], "job.*");

    // Route two messages through the exchange, then read one queue's stats.
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let p = session
        .create_producer("/exchanges/events/job.email")
        .await
        .expect("p");
    p.send(Message::text("a")).await.expect("send");
    p.send(Message::text("b")).await.expect("send");
    let (s, body) = http(&mgmt, "GET", "/queues/%2F/jobs", None, None).await;
    assert_eq!(s, 200, "{body}");
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["ready"], 2, "{body}");
    assert_eq!(v["messages"], 2);
    assert_eq!(v["kind"], "transient");
    let (s, _) = http(&mgmt, "GET", "/queues/%2F/nope", None, None).await;
    assert_eq!(s, 404);

    // Deleting the queue detaches its consumer with resource-deleted.
    let mut consumer = session.create_consumer("/queues/jobs").await.expect("c");
    for _ in 0..2 {
        let d = consumer.recv().await.expect("delivery");
        consumer.accept(&d).await.expect("accept");
    }
    let (s, _) = http(&mgmt, "DELETE", "/queues/%2F/jobs", Some(BEARER), None).await;
    assert_eq!(s, 204);
    let gone = tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv())
        .await
        .expect("detach arrives");
    assert!(gone.is_err(), "consumer of a deleted queue is detached");
    let (s, _) = http(&mgmt, "GET", "/queues/%2F/jobs", None, None).await;
    assert_eq!(s, 404);
    let (s, _) = http(&mgmt, "DELETE", "/queues/%2F/jobs", Some(BEARER), None).await;
    assert_eq!(s, 404);

    // Deleting the queue also removed its binding (otherwise the next
    // exchange publish would re-create the queue), so unbinding is a 404.
    let (_, body) = http(&mgmt, "GET", "/exchanges/%2F/events", None, None).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["bindings"].as_array().map(Vec::len), Some(0), "{body}");
    let (s, _) = http(
        &mgmt,
        "DELETE",
        "/bindings/%2F/events",
        Some(BEARER),
        Some(bind),
    )
    .await;
    assert_eq!(s, 404);
    let (s, _) = http(&mgmt, "DELETE", "/exchanges/%2F/events", Some(BEARER), None).await;
    assert_eq!(s, 204);
    let (s, _) = http(&mgmt, "GET", "/exchanges/%2F/events", None, None).await;
    assert_eq!(s, 404);
    let (s, _) = http(&mgmt, "POST", "/queues", Some(BEARER), None).await;
    assert_eq!(s, 405);

    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// Admin users (Basic) may write; non-admin users and anonymous may not;
/// with `management_auth_reads`, GETs need credentials too.
#[tokio::test]
async fn admin_api_basic_auth_and_read_auth() {
    let users = ramqp_broker::StaticUsers::new()
        .with_password("ops", "ops-pw")
        .with_admin("ops")
        .with_password("app", "app-pw");
    let (_addr, mgmt, shutdown) = start_admin(Some(users), true).await;
    let basic = |u: &str, p: &str| {
        use base64::Engine;
        format!(
            "Basic {}",
            base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"))
        )
    };
    let (s, _) = http(&mgmt, "GET", "/metrics", None, None).await;
    assert_eq!(s, 401, "reads need credentials when configured");
    let (s, _) = http(&mgmt, "GET", "/metrics", Some(BEARER), None).await;
    assert_eq!(s, 200);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/tenant/q",
        Some(&basic("ops", "ops-pw")),
        None,
    )
    .await;
    assert_eq!(s, 201, "admin may write");
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/tenant/q2",
        Some(&basic("ops", "nope")),
        None,
    )
    .await;
    assert_eq!(s, 401);
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/tenant/q2",
        Some(&basic("app", "app-pw")),
        None,
    )
    .await;
    assert_eq!(s, 401, "a non-admin user may not write");
    let (s, body) = http(
        &mgmt,
        "GET",
        "/queues/tenant/q",
        Some(&basic("ops", "ops-pw")),
        None,
    )
    .await;
    assert_eq!(s, 200, "{body}");
    shutdown.shutdown();
}

/// Deleting a standalone quorum queue: its consumer is detached, the queue
/// is gone, and a re-declare starts empty.
#[tokio::test]
async fn deleting_a_quorum_queue() {
    let (addr, mgmt, shutdown) = start_admin(None, false).await;
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/qq",
        Some(BEARER),
        Some(r#"{"kind":"quorum"}"#),
    )
    .await;
    assert_eq!(s, 201);
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let p = session.create_producer("/quorum/qq").await.expect("p");
    for i in 0..3 {
        p.send(Message::text(format!("m{i}"))).await.expect("send");
    }
    let mut consumer = session.create_consumer("/quorum/qq").await.expect("c");
    for _ in 0..3 {
        let d = consumer.recv().await.expect("delivery");
        consumer.accept(&d).await.expect("accept");
    }
    p.send(Message::text("left-behind")).await.expect("send");
    // Drain the in-flight copy so only the detach remains.
    let d = consumer.recv().await.expect("delivery");
    consumer.release(&d).await.expect("release");

    let (s, body) = http(
        &mgmt,
        "DELETE",
        "/queues/%2F/qq?kind=quorum",
        Some(BEARER),
        None,
    )
    .await;
    assert_eq!(s, 204, "{body}");
    let gone = loop {
        match tokio::time::timeout(std::time::Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(d)) => consumer.release(&d).await.expect("release"),
            other => break other,
        }
    };
    assert!(
        matches!(gone, Ok(Err(_))),
        "the consumer of a deleted quorum queue is detached: {gone:?}"
    );
    let (s, _) = http(&mgmt, "GET", "/queues/%2F/qq?kind=quorum", None, None).await;
    assert_eq!(s, 404);
    let (s, _) = http(
        &mgmt,
        "DELETE",
        "/queues/%2F/qq?kind=quorum",
        Some(BEARER),
        None,
    )
    .await;
    assert_eq!(s, 404);
    // Re-declared: empty.
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/qq",
        Some(BEARER),
        Some(r#"{"kind":"quorum"}"#),
    )
    .await;
    assert_eq!(s, 201);
    let (_, body) = http(&mgmt, "GET", "/queues/%2F/qq?kind=quorum", None, None).await;
    assert!(body.contains("\"ready\":0"), "{body}");
    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// On disk: the deleted queue's messages never come back — not on
/// re-declare, not after a restart — and its stored group is purged.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn deleting_a_durable_quorum_queue_survives_restart() {
    let dir = tempfile::tempdir().unwrap();
    let start = |dir: std::path::PathBuf| async move {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mgmt = l.local_addr().unwrap();
        drop(l);
        let mut config = BrokerConfig::default();
        config.data_dir = Some(dir);
        config.management_listen = Some(mgmt.to_string());
        config.management_token = Some(ramqp_broker::SecretString::new(TOKEN));
        let mut bound = None;
        for _ in 0..50 {
            match Broker::new(config.clone()).bind("127.0.0.1:0").await {
                Ok(b) => {
                    bound = Some(b);
                    break;
                }
                Err(_) => tokio::time::sleep(std::time::Duration::from_millis(100)).await,
            }
        }
        let bound = bound.expect("bind (the previous store lock frees asynchronously)");
        let addr = bound.local_addr();
        let shutdown = bound.shutdown_handle();
        tokio::spawn(bound.run());
        (addr, mgmt, shutdown)
    };
    let (addr, mgmt, shutdown) = start(dir.path().to_path_buf()).await;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let p = session.create_producer("/quorum/kept").await.expect("p");
    for i in 0..5 {
        p.send(Message::text(format!("old{i}")))
            .await
            .expect("send");
    }
    conn.close().await.expect("close");
    let spill_dirs = || {
        std::fs::read_dir(dir.path().join("spill"))
            .map(|d| d.count())
            .unwrap_or(0)
    };
    assert_eq!(spill_dirs(), 1, "the queue's group has its files");
    let (s, body) = http(
        &mgmt,
        "DELETE",
        "/queues/%2F/kept?kind=quorum",
        Some(BEARER),
        None,
    )
    .await;
    assert_eq!(s, 204, "{body}");
    assert_eq!(spill_dirs(), 0, "the deleted group's files are purged");
    shutdown.shutdown();

    // Restart: the deleted messages do not resurrect.
    let (addr, mgmt, shutdown) = start(dir.path().to_path_buf()).await;
    let mut ready = None;
    for _ in 0..50 {
        let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
            .connect()
            .await
            .expect("connect");
        let session = conn.begin_session().await.expect("session");
        let p = session.create_producer("/quorum/kept").await;
        conn.close().await.ok();
        if p.is_ok() {
            let (s, body) = http(&mgmt, "GET", "/queues/%2F/kept?kind=quorum", None, None).await;
            if s == 200 {
                ready = Some(body);
                break;
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
    let body = ready.expect("re-declared after restart");
    assert!(
        body.contains("\"ready\":0"),
        "deleted messages stay deleted: {body}"
    );
    shutdown.shutdown();
}

/// Deleting a durable queue purges its rows: a re-declare (even after a
/// restart) starts empty, and a producer still attached is detached.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn deleting_a_durable_queue_purges_it() {
    let dir = tempfile::tempdir().unwrap();
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgmt = l.local_addr().unwrap();
    drop(l);
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    config.management_listen = Some(mgmt.to_string());
    config.management_token = Some(ramqp_broker::SecretString::new(TOKEN));
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let p = session.create_producer("/durable/doomed").await.expect("p");
    for i in 0..3 {
        p.send(Message::text(format!("m{i}"))).await.expect("send");
    }
    let (s, body) = http(&mgmt, "GET", "/queues/%2F/doomed", None, None).await;
    assert_eq!(s, 200);
    assert!(body.contains("\"ready\":3"), "{body}");
    let (s, _) = http(
        &mgmt,
        "DELETE",
        "/queues/%2F/doomed?kind=durable",
        Some(BEARER),
        None,
    )
    .await;
    assert_eq!(s, 204);
    // The attached producer learns: its next publish fails.
    let next = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        p.send(Message::text("late")),
    )
    .await
    .expect("answer in time");
    assert!(
        !matches!(
            next,
            Ok(ramqp::types::messaging::DeliveryState::Accepted(_))
        ),
        "publish into a deleted queue must not be accepted: {next:?}"
    );
    // Re-declared empty.
    let (s, _) = http(
        &mgmt,
        "PUT",
        "/queues/%2F/doomed",
        Some(BEARER),
        Some(r#"{"kind":"durable"}"#),
    )
    .await;
    assert_eq!(s, 201);
    let (_, body) = http(&mgmt, "GET", "/queues/%2F/doomed", None, None).await;
    assert!(body.contains("\"ready\":0"), "{body}");
    conn.close().await.expect("close");
    shutdown.shutdown();
}
