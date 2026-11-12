//! RabbitMQ 4.x parity (issue #45): `/queues/<name>` finds any declared
//! queue, `delivery-count` on redelivery, unroutable = `released`, per-queue
//! policies and the `{queue}` dead-letter template, and runtime users and
//! permissions. Plus the #47 follow-ups: a consumer's `rejected` is
//! dead-lettered, and a refused attach fails the attach.

use std::time::Duration;

use ramqp::types::messaging::{DeliveryState, Modified};
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::auth::StaticUsers;
use ramqp_broker::{Broker, BrokerConfig, ExchangeKind, ShutdownHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const TOKEN: &str = "parity-test-token-0123";

struct Running {
    addr: std::net::SocketAddr,
    mgmt: std::net::SocketAddr,
    broker: Broker,
    shutdown: ShutdownHandle,
}

/// A broker with a management endpoint (bearer [`TOKEN`]); `users` turns on
/// the runtime user store over those configured users.
async fn start(config: BrokerConfig, users: Option<StaticUsers>) -> Running {
    start_on(config, users, "127.0.0.1:0").await
}

/// [`start`] on a given AMQP listen address (a restart on the same port, for
/// a reconnecting client).
async fn start_on(mut config: BrokerConfig, users: Option<StaticUsers>, listen: &str) -> Running {
    for _ in 0..50 {
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let mgmt = l.local_addr().unwrap();
        drop(l);
        config.management_listen = Some(mgmt.to_string());
        config.management_token = Some(ramqp_broker::SecretString::new(TOKEN));
        let mut broker = Broker::new(config.clone());
        if let Some(u) = users.clone() {
            broker = broker.with_runtime_users(u);
        }
        // A previous instance's store lock frees asynchronously.
        let Ok(bound) = broker.clone().bind(listen).await else {
            tokio::time::sleep(Duration::from_millis(100)).await;
            continue;
        };
        let addr = bound.local_addr();
        let shutdown = bound.shutdown_handle();
        tokio::spawn(bound.run());
        assert!(
            broker.definitions_ready(Duration::from_secs(10)).await,
            "definitions loaded"
        );
        return Running {
            addr,
            mgmt,
            broker,
            shutdown,
        };
    }
    panic!("could not start a broker");
}

async fn http(addr: &std::net::SocketAddr, method: &str, path: &str, body: &str) -> (u16, String) {
    http_auth(addr, method, path, body, Some(&format!("Bearer {TOKEN}"))).await
}

async fn http_auth(
    addr: &std::net::SocketAddr,
    method: &str,
    path: &str,
    body: &str,
    auth: Option<&str>,
) -> (u16, String) {
    let mut stream = tokio::net::TcpStream::connect(addr).await.unwrap();
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
        .unwrap();
    let mut buf = Vec::new();
    stream.read_to_end(&mut buf).await.unwrap();
    let text = String::from_utf8_lossy(&buf).into_owned();
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b.to_owned())
        .unwrap_or_default();
    (status, body)
}

fn text(d: &ramqp::Delivery) -> String {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    match d.message().map(|m| m.body) {
        Ok(Body::Value(Value::String(s))) => s,
        other => format!("{other:?}"),
    }
}

async fn recv(c: &mut ramqp::Consumer) -> Option<ramqp::Delivery> {
    tokio::time::timeout(Duration::from_secs(5), c.recv())
        .await
        .ok()
        .and_then(Result::ok)
}

fn delivery_failed() -> Modified {
    Modified {
        delivery_failed: Some(true),
        ..Default::default()
    }
}

// ---------------------------------------------------------------- gap 1

/// `/queues/<name>` reaches the queue declared under `<name>`, whatever its
/// kind — never a separate transient queue of the same name.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn queues_address_finds_the_declared_queue_of_any_kind() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config, None).await;
    for (kind, name) in [("durable", "jobs"), ("quorum", "orders")] {
        let (s, body) = http(
            &r.mgmt,
            "PUT",
            &format!("/queues/%2F/{name}"),
            &format!(r#"{{"kind":"{kind}"}}"#),
        )
        .await;
        assert_eq!(s, 201, "{body}");
    }
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    for name in ["jobs", "orders"] {
        s.create_producer(&format!("/queues/{name}"))
            .await
            .unwrap()
            .send(Message::text(format!("via-queues-{name}")))
            .await
            .unwrap();
    }
    // The messages are in the declared durable / quorum queues.
    let mut d = s.create_consumer("/durable/jobs").await.unwrap();
    assert_eq!(
        text(&recv(&mut d).await.expect("durable")),
        "via-queues-jobs"
    );
    let mut q = s.create_consumer("/quorum/orders").await.unwrap();
    assert_eq!(
        text(&recv(&mut q).await.expect("quorum")),
        "via-queues-orders"
    );
    // ... and no transient twin was created.
    let (_, body) = http(&r.mgmt, "GET", "/queues", "").await;
    assert!(!body.contains("\"kind\":\"transient\""), "{body}");
    // Nothing declared: `/queues/<name>` still declares a transient queue.
    s.create_producer("/queues/fresh").await.unwrap();
    let (st, body) = http(&r.mgmt, "GET", "/queues/%2F/fresh", "").await;
    assert!(
        st == 200 && body.contains("\"kind\":\"transient\""),
        "{body}"
    );
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A name holds one kind: an explicit `/quorum/` attach does not add a
/// second kind beside an existing transient queue (after which
/// `/queues/<name>` could no longer tell them apart).
#[tokio::test]
async fn a_second_kind_under_one_name_is_refused() {
    let r = start(BrokerConfig::default(), None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    s.create_producer("/queues/twin")
        .await
        .unwrap()
        .send(Message::text("t"))
        .await
        .unwrap();
    // The attach itself is refused.
    let refused = s.create_producer("/quorum/twin").await.err();
    let refused = refused.expect("second kind refused");
    assert!(
        refused.to_string().contains("amqp:not-allowed"),
        "{refused}"
    );
    // The existing queue is untouched and still reached at /queues/.
    let p = s.create_producer("/queues/twin").await.unwrap();
    assert!(matches!(
        p.send(Message::text("t2")).await.unwrap(),
        DeliveryState::Accepted(_)
    ));
    let (_, body) = http(&r.mgmt, "GET", "/queues", "").await;
    assert!(!body.contains("\"kind\":\"quorum\""), "{body}");
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- gap 2

/// `header.delivery-count` counts failed attempts (AMQP 1.0 §3.2.1): a
/// `delivery-failed` redelivery increments it; a plain release does not.
#[tokio::test]
async fn delivery_count_counts_failed_attempts_on_every_queue_kind() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let addresses: &[&str] = if cfg!(feature = "store-redb") {
        &["/queues/retry", "/durable/retry2", "/quorum/retry3"]
    } else {
        &["/queues/retry", "/quorum/retry3"]
    };
    for &address in addresses {
        s.create_producer(address)
            .await
            .unwrap()
            .send(Message::text("r"))
            .await
            .unwrap();
        let mut c = s.create_consumer(address).await.unwrap();
        let count =
            |d: &ramqp::Delivery| d.message().unwrap().header.map_or(0, |h| h.delivery_count);
        let mut seen = Vec::new();
        for _ in 0..3 {
            let d = recv(&mut c).await.expect("delivery");
            seen.push(count(&d));
            c.modify(&d, delivery_failed()).await.unwrap();
        }
        // A plain release is not a failed attempt.
        let d = recv(&mut c).await.expect("delivery");
        seen.push(count(&d));
        c.release(&d).await.unwrap();
        let d = recv(&mut c).await.expect("delivery");
        seen.push(count(&d));
        c.accept(&d).await.unwrap();
        assert_eq!(seen, [0, 1, 2, 3, 3], "{address}");
        assert_eq!(text(&d), "r", "the body is untouched");
        c.detach().await.unwrap();
    }
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- gap 3

/// An exchange publish that matches no binding is settled `released`
/// (RabbitMQ 4.x); with an alternate queue it is `accepted`.
#[tokio::test]
async fn unroutable_exchange_publish_is_released() {
    let r = start(BrokerConfig::default(), None).await;
    r.broker
        .declare_exchange("", "plain", ExchangeKind::Direct, None)
        .await
        .unwrap();
    r.broker
        .declare_exchange(
            "",
            "with-alt",
            ExchangeKind::Direct,
            Some("/queues/unroutable"),
        )
        .await
        .unwrap();
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let p = s.create_producer("/exchanges/plain/nobody").await.unwrap();
    for _ in 0..3 {
        assert!(matches!(
            p.send(Message::text("x")).await.unwrap(),
            DeliveryState::Released(_)
        ));
    }
    let p = s
        .create_producer("/exchanges/with-alt/nobody")
        .await
        .unwrap();
    assert!(matches!(
        p.send(Message::text("y")).await.unwrap(),
        DeliveryState::Accepted(_)
    ));
    let mut alt = s.create_consumer("/queues/unroutable").await.unwrap();
    assert_eq!(text(&recv(&mut alt).await.expect("alternate")), "y");
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- gap 5

/// A queue declared with its own dead-letter target and retry budget
/// dead-letters there after the budget, keeps the policy over a restart,
/// and refuses a re-declare with a different policy.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn per_queue_policy_dead_letters_and_persists() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), None).await;
    let decl = r#"{"kind":"durable","dead_letter":"/durable/jobs.dlq","max_delivery_attempts":2}"#;
    let (s, body) = http(&r.mgmt, "PUT", "/queues/%2F/jobs", decl).await;
    assert_eq!(s, 201, "{body}");
    assert_eq!(http(&r.mgmt, "PUT", "/queues/%2F/jobs", decl).await.0, 204);
    let (s, _) = http(&r.mgmt, "PUT", "/queues/%2F/jobs", r#"{"kind":"durable"}"#).await;
    assert_eq!(s, 409, "a different policy is refused");
    let (_, body) = http(&r.mgmt, "GET", "/queues/%2F/jobs", "").await;
    assert!(
        body.contains("\"dead_letter\":\"/durable/jobs.dlq\""),
        "{body}"
    );
    r.shutdown.shutdown();
    drop(r); // releases the store

    // After a restart the queue still uses its policy.
    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    s.create_producer("/queues/jobs")
        .await
        .unwrap()
        .send(Message::text("job-1"))
        .await
        .unwrap();
    let mut c = s.create_consumer("/queues/jobs").await.unwrap();
    for _ in 0..2 {
        let d = recv(&mut c).await.expect("attempt");
        c.modify(&d, delivery_failed()).await.unwrap();
    }
    let mut dlq = s.create_consumer("/durable/jobs.dlq").await.unwrap();
    assert_eq!(text(&recv(&mut dlq).await.expect("dead-lettered")), "job-1");
    conn.close().await.unwrap();
    // Deleting the queue removes its policy: a re-declare states its own.
    let (st, _) = http(&r.mgmt, "DELETE", "/queues/%2F/jobs", "").await;
    assert_eq!(st, 204);
    let (st, _) = http(&r.mgmt, "PUT", "/queues/%2F/jobs", r#"{"kind":"durable"}"#).await;
    assert_eq!(st, 201);
    r.shutdown.shutdown();
}

/// A prefix policy with `{queue}` gives every queue its own dead-letter
/// companion.
#[tokio::test]
async fn dead_letter_template_gives_each_queue_its_companion() {
    let mut config = BrokerConfig::default();
    let mut p = ramqp_broker::QueuePolicy::default();
    p.dead_letter = Some("/queues/{queue}.dlq".into());
    p.max_delivery_attempts = Some(1);
    config.policies = vec![("app.".into(), p)];
    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    for q in ["app.a", "app.b"] {
        s.create_producer(&format!("/queues/{q}"))
            .await
            .unwrap()
            .send(Message::text(q))
            .await
            .unwrap();
        let mut c = s.create_consumer(&format!("/queues/{q}")).await.unwrap();
        let d = recv(&mut c).await.expect("delivery");
        c.modify(&d, delivery_failed()).await.unwrap();
        let mut dlq = s
            .create_consumer(&format!("/queues/{q}.dlq"))
            .await
            .unwrap();
        assert_eq!(text(&recv(&mut dlq).await.expect("own companion")), q);
        // Delivered once (nothing else arrives).
        let more = tokio::time::timeout(Duration::from_millis(300), dlq.recv()).await;
        assert!(more.is_err(), "delivered once");
    }
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- gap 4

#[cfg(feature = "store-redb")]
fn base_users() -> StaticUsers {
    StaticUsers::new()
        .with_password("ops", "ops-pw")
        .with_admin("ops")
        .with_permission(ramqp_broker::auth::Permission::new(
            "ops", "*", "*", true, true,
        ))
}

#[cfg(feature = "store-redb")]
async fn login(
    addr: std::net::SocketAddr,
    user: &str,
    password: &str,
) -> Result<ramqp::Connection, String> {
    ConnectionBuilder::new(format!("amqp://{user}:{password}@{addr}"))
        .connect()
        .await
        .map_err(|e| e.to_string())
}

/// A user and its grants created at runtime: it logs in (PLAIN and SCRAM),
/// reads only what it was granted, survives a restart, and deleting it
/// closes its connections. Configuration users are read-only.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn runtime_users_and_permissions() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), Some(base_users())).await;

    // Never public, even for reads.
    assert_eq!(http_auth(&r.mgmt, "GET", "/users", "", None).await.0, 401);
    let (s, body) = http(
        &r.mgmt,
        "PUT",
        "/users/reader",
        r#"{"password":"reader-pw"}"#,
    )
    .await;
    assert_eq!(s, 201, "{body}");
    assert_eq!(
        http(&r.mgmt, "PUT", "/users/ops", r#"{"password":"x"}"#)
            .await
            .0,
        409,
        "configuration users are read-only"
    );
    let grants = r#"[{"address":"/queues/jobs","operations":["receive"]}]"#;
    assert_eq!(
        http(&r.mgmt, "PUT", "/permissions/%2F/reader", grants)
            .await
            .0,
        201
    );
    let (_, body) = http(&r.mgmt, "GET", "/users", "").await;
    assert!(
        body.contains("\"name\":\"reader\"") && body.contains("\"source\":\"api\""),
        "{body}"
    );
    r.shutdown.shutdown();
    drop(r); // releases the store

    // Persisted: the user and its grants survive a restart.
    let r = start(config, Some(base_users())).await;
    let ops = login(r.addr, "ops", "ops-pw").await.unwrap();
    let os = ops.begin_session().await.unwrap();
    os.create_producer("/queues/jobs")
        .await
        .unwrap()
        .send(Message::text("for-reader"))
        .await
        .unwrap();

    let reader = login(r.addr, "reader", "reader-pw")
        .await
        .expect("runtime user logs in");
    let rs = reader.begin_session().await.unwrap();
    let mut c = rs.create_consumer("/queues/jobs").await.unwrap();
    assert_eq!(text(&recv(&mut c).await.expect("granted")), "for-reader");
    // A refusal fails the attach itself, with the broker's reason (#47).
    let refused = rs.create_producer("/queues/jobs").await.err();
    let refused = refused.expect("no send grant");
    assert!(
        refused.to_string().contains("amqp:unauthorized-access"),
        "{refused}"
    );
    let refused = rs.create_consumer("/queues/other").await.err();
    let refused = refused.expect("no grant there");
    assert!(
        refused.to_string().contains("amqp:unauthorized-access"),
        "{refused}"
    );
    // The session survives the refusals: the granted link still works.
    rs.create_consumer("/queues/jobs")
        .await
        .unwrap()
        .detach()
        .await
        .unwrap();
    // SCRAM works for a runtime user too.
    let scram = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .sasl(ramqp::sasl::SaslProfile::Scram {
            mechanism: ramqp::sasl::ScramMechanism::Sha256,
            username: "reader".into(),
            password: "reader-pw".into(),
        })
        .connect()
        .await;
    assert!(scram.is_ok(), "{:?}", scram.err());
    assert!(login(r.addr, "reader", "wrong").await.is_err());

    // Deleted: its open connection closes, and it can no longer log in.
    assert_eq!(http(&r.mgmt, "DELETE", "/users/reader", "").await.0, 204);
    let closed = tokio::time::timeout(Duration::from_secs(5), c.recv()).await;
    assert!(
        matches!(closed, Ok(Err(_))),
        "connection closed: {closed:?}"
    );
    assert!(login(r.addr, "reader", "reader-pw").await.is_err());
    assert_eq!(
        http(&r.mgmt, "GET", "/permissions/%2F/reader", "").await.0,
        404,
        "its permissions went with it"
    );
    assert_eq!(http(&r.mgmt, "DELETE", "/users/ops", "").await.0, 409);
    ops.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A broker without the runtime user store says so.
#[tokio::test]
async fn runtime_users_need_the_user_store() {
    let r = start(BrokerConfig::default(), None).await;
    assert_eq!(
        http(&r.mgmt, "PUT", "/users/x", r#"{"password":"p"}"#)
            .await
            .0,
        501
    );
    r.shutdown.shutdown();
}

// ------------------------------------------------- review follow-ups (#46)

/// `/users` and `/permissions` are never public — whatever form of the
/// path routes to them (doubled slashes, percent-encoding).
#[tokio::test]
async fn user_endpoints_need_credentials_in_every_path_form() {
    let r = start(
        BrokerConfig::default(),
        Some(StaticUsers::new().with_password("ops", "ops-pw")),
    )
    .await;
    for path in [
        "/users",
        "//users",
        "/%75sers",
        "/users/",
        "//permissions/%2F/ops",
        "/%70ermissions/%2F/ops",
    ] {
        let (s, body) = http_auth(&r.mgmt, "GET", path, "", None).await;
        assert_eq!(s, 401, "{path}: {body}");
    }
    r.shutdown.shutdown();
}

/// Standalone quorum queue on disk, not attached since a restart:
/// `/queues/<name>` reaches it (never a new transient queue shadowing it).
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn queues_address_finds_an_on_disk_quorum_queue_after_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), None).await;
    let (s, body) = http(&r.mgmt, "PUT", "/queues/%2F/orders", r#"{"kind":"quorum"}"#).await;
    assert_eq!(s, 201, "{body}");
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    s.create_producer("/queues/orders")
        .await
        .unwrap()
        .send(Message::text("kept"))
        .await
        .unwrap();
    conn.close().await.unwrap();
    r.shutdown.shutdown();
    drop(r);

    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let mut c = s.create_consumer("/queues/orders").await.unwrap();
    assert_eq!(
        text(&recv(&mut c).await.expect("persisted message")),
        "kept"
    );
    let (_, body) = http(&r.mgmt, "GET", "/queues", "").await;
    assert!(
        !body.contains("\"kind\":\"transient\""),
        "no shadow: {body}"
    );
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A dead letter reaches the dead-letter queue even when the name was first
/// declared (by a consumer at `/queues/<name>`) as another kind than the
/// policy's target names.
#[tokio::test]
async fn dead_letters_reach_a_dlq_declared_as_another_kind() {
    let r = start(BrokerConfig::default(), None).await;
    let decl = r#"{"kind":"quorum","dead_letter":"/quorum/jobs.dlq","max_delivery_attempts":1}"#;
    assert_eq!(http(&r.mgmt, "PUT", "/queues/%2F/jobs", decl).await.0, 201);
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    // The DLQ consumer is there first, at the RabbitMQ address: transient.
    let mut dlq = s.create_consumer("/queues/jobs.dlq").await.unwrap();
    s.create_producer("/queues/jobs")
        .await
        .unwrap()
        .send(Message::text("doomed"))
        .await
        .unwrap();
    let mut c = s.create_consumer("/queues/jobs").await.unwrap();
    let d = recv(&mut c).await.expect("delivery");
    c.modify(&d, delivery_failed()).await.unwrap();
    assert_eq!(
        text(&recv(&mut dlq).await.expect("dead letter arrives")),
        "doomed"
    );
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// Concurrent first attaches of two kinds under one name never leave the
/// name with both kinds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_declarations_keep_one_kind_per_name() {
    let r = start(BrokerConfig::default(), None).await;
    let addr = r.addr;
    let mut tasks = Vec::new();
    for i in 0..30 {
        for kind in ["queues", "quorum"] {
            tasks.push(tokio::spawn(async move {
                let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
                    .connect()
                    .await
                    .unwrap();
                let s = conn.begin_session().await.unwrap();
                // The loser of a race is refused at attach, for the
                // name's other kind (and only for that).
                match s.create_producer(&format!("/{kind}/race{i}")).await {
                    Ok(p) => {
                        let _ = p.send(Message::text("x")).await;
                    }
                    Err(e) => assert!(e.to_string().contains("amqp:not-allowed"), "{e}"),
                }
                conn.close().await.ok();
            }));
        }
    }
    for t in tasks {
        t.await.unwrap();
    }
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    for i in 0..30 {
        let p = s
            .create_producer(&format!("/queues/race{i}"))
            .await
            .unwrap();
        assert!(
            p.send(Message::text("after")).await.is_ok(),
            "race{i} still reachable at /queues/"
        );
    }
    // The decisive check: no name is held by two queues.
    let (_, body) = http(&r.mgmt, "GET", "/queues", "").await;
    let listed: serde_json::Value = serde_json::from_str(&body).unwrap();
    let mut seen = std::collections::HashMap::new();
    for q in listed.as_array().unwrap() {
        *seen
            .entry(q["name"].as_str().unwrap().to_owned())
            .or_insert(0) += 1;
    }
    let doubled: Vec<_> = seen.iter().filter(|(_, n)| **n > 1).collect();
    assert!(doubled.is_empty(), "names held by two kinds: {doubled:?}");
    assert_eq!(seen.len(), 30);
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A transactional publish that reaches no queue says so: a transactional
/// state whose outcome is `released`.
#[tokio::test]
async fn transactional_unroutable_publish_is_released() {
    use ramqp::types::messaging::Outcome;
    let r = start(BrokerConfig::default(), None).await;
    r.broker
        .declare_exchange("", "tx", ExchangeKind::Direct, None)
        .await
        .unwrap();
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let ctl = s.create_transaction_controller().await.unwrap();
    let txn = ctl.declare().await.unwrap();
    let p = s.create_producer("/exchanges/tx/nobody").await.unwrap();
    let state = p
        .send_with_state(
            Message::text("x"),
            ramqp::txn::transactional_state(txn.clone(), None),
        )
        .await
        .unwrap();
    let ts = ramqp_core::txn::txn_state(&state).expect("transactional state");
    assert!(matches!(ts.outcome, Some(Outcome::Released(_))), "{ts:?}");
    ctl.commit(txn).await.unwrap();
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// `strict_queue_grants`: a `/queues/*` grant does not reach a quorum queue
/// through its `/queues/` alias; an explicit grant does.
#[tokio::test]
async fn strict_queue_grants_keep_queues_grants_to_transient() {
    use ramqp_broker::auth::Permission;
    let mut config = BrokerConfig::default();
    config.strict_queue_grants = true;
    let users = StaticUsers::new()
        .with_password("ops", "ops-pw")
        .with_admin("ops")
        .with_password("app", "app-pw")
        .with_permission(Permission::new("app", "*", "/queues/*", true, true));
    let r = start(config, Some(users)).await;
    assert_eq!(
        http(
            &r.mgmt,
            "PUT",
            "/queues/%2F/payments",
            r#"{"kind":"quorum"}"#
        )
        .await
        .0,
        201
    );
    let conn = ConnectionBuilder::new(format!("amqp://app:app-pw@{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let refused = s.create_producer("/queues/payments").await.err();
    let refused = refused.expect("quorum needs its own grant");
    assert!(
        refused.to_string().contains("amqp:unauthorized-access"),
        "{refused}"
    );
    let p = s.create_producer("/queues/scratch").await.unwrap();
    assert!(
        p.send(Message::text("y")).await.is_ok(),
        "transient is covered"
    );
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// Deleting two runtime users back to back closes both users' connections;
/// grants cannot be given to configuration users; a non-admin runtime user
/// cannot administer.
#[tokio::test]
async fn runtime_user_deletions_and_limits() {
    let base = StaticUsers::new()
        .with_password("ops", "ops-pw")
        .with_admin("ops");
    let r = start(BrokerConfig::default(), Some(base)).await;
    for u in ["a", "b"] {
        let body = format!(r#"{{"password":"{u}-pw"}}"#);
        assert_eq!(
            http(&r.mgmt, "PUT", &format!("/users/{u}"), &body).await.0,
            201
        );
        let grant = r#"[{"address":"/queues/*","operations":["send","receive"]}]"#;
        assert_eq!(
            http(&r.mgmt, "PUT", &format!("/permissions/%2F/{u}"), grant)
                .await
                .0,
            201
        );
    }
    let grant = r#"[{"address":"/queues/*","operations":["send"]}]"#;
    assert_eq!(
        http(&r.mgmt, "PUT", "/permissions/%2F/ops", grant).await.0,
        409
    );
    use base64::Engine;
    let basic = base64::engine::general_purpose::STANDARD.encode("a:a-pw");
    let (s, _) = http_auth(
        &r.mgmt,
        "PUT",
        "/queues/%2F/q",
        r#"{"kind":"transient"}"#,
        Some(&format!("Basic {basic}")),
    )
    .await;
    assert_eq!(s, 401, "a non-admin runtime user cannot administer");

    let mut consumers = Vec::new();
    let mut conns = Vec::new();
    for u in ["a", "b"] {
        let conn = ConnectionBuilder::new(format!("amqp://{u}:{u}-pw@{}", r.addr))
            .connect()
            .await
            .unwrap();
        let s = conn.begin_session().await.unwrap();
        consumers.push(
            s.create_consumer(&format!("/queues/{u}-inbox"))
                .await
                .unwrap(),
        );
        conns.push(conn);
    }
    // Back to back: neither deletion may be missed.
    assert_eq!(http(&r.mgmt, "DELETE", "/users/a", "").await.0, 204);
    assert_eq!(http(&r.mgmt, "DELETE", "/users/b", "").await.0, 204);
    for c in &mut consumers {
        let closed = tokio::time::timeout(Duration::from_secs(5), c.recv()).await;
        assert!(
            matches!(closed, Ok(Err(_))),
            "connection closed: {closed:?}"
        );
    }
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- #47

/// A consumer's `rejected` dead-letters through the queue's policy, and so
/// does `modified{undeliverable-here}` (RabbitMQ 4.x); with no dead-letter
/// target the message is dropped. On every queue kind.
#[tokio::test]
async fn rejected_messages_are_dead_lettered_on_every_queue_kind() {
    use ramqp::types::definitions::{AmqpError, Error};

    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let kinds: &[&str] = if cfg!(feature = "store-redb") {
        &["transient", "durable", "quorum"]
    } else {
        &["transient", "quorum"]
    };
    for &kind in kinds {
        let (q, dlq, bare) = (
            format!("{kind}-jobs"),
            format!("{kind}-jobs.dlq"),
            format!("{kind}-bare"),
        );
        let declare = |name: &str, body: String| {
            let path = format!("/queues/%2F/{name}");
            let mgmt = r.mgmt;
            async move { http(&mgmt, "PUT", &path, &body).await }
        };
        assert_eq!(
            declare(&dlq, format!(r#"{{"kind":"{kind}"}}"#)).await.0,
            201
        );
        let (st, body) = declare(
            &q,
            format!(r#"{{"kind":"{kind}","dead_letter":"/queues/{dlq}"}}"#),
        )
        .await;
        assert_eq!(st, 201, "{kind}: {body}");
        assert_eq!(
            declare(&bare, format!(r#"{{"kind":"{kind}"}}"#)).await.0,
            201
        );

        let p = s.create_producer(&format!("/queues/{q}")).await.unwrap();
        p.send(Message::text("rejected")).await.unwrap();
        p.send(Message::text("undeliverable")).await.unwrap();
        let mut c = s.create_consumer(&format!("/queues/{q}")).await.unwrap();
        let d = recv(&mut c).await.expect("first delivery");
        assert_eq!(text(&d), "rejected", "{kind}");
        c.reject(
            &d,
            Some(Error::new(
                AmqpError::InternalError,
                Some("out of retries".into()),
            )),
        )
        .await
        .unwrap();
        let d = recv(&mut c).await.expect("second delivery");
        assert_eq!(text(&d), "undeliverable", "{kind}");
        c.modify(
            &d,
            Modified {
                delivery_failed: Some(true),
                undeliverable_here: Some(true),
                ..Default::default()
            },
        )
        .await
        .unwrap();

        let mut dl = s.create_consumer(&format!("/queues/{dlq}")).await.unwrap();
        let mut got = Vec::new();
        for _ in 0..2 {
            let d = recv(&mut dl)
                .await
                .unwrap_or_else(|| panic!("{kind}: dead letter arrives"));
            got.push(text(&d));
            dl.accept(&d).await.unwrap();
        }
        got.sort();
        assert_eq!(got, ["rejected", "undeliverable"], "{kind}");
        // Neither went back to the source queue.
        let again = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
        assert!(again.is_err(), "{kind}: nothing redelivered: {again:?}");
        c.detach().await.unwrap();
        dl.detach().await.unwrap();

        // No dead-letter target: a reject drops the message.
        s.create_producer(&format!("/queues/{bare}"))
            .await
            .unwrap()
            .send(Message::text("gone"))
            .await
            .unwrap();
        let mut c = s.create_consumer(&format!("/queues/{bare}")).await.unwrap();
        let d = recv(&mut c).await.expect("delivery");
        c.reject(&d, None).await.unwrap();
        let again = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
        assert!(
            again.is_err(),
            "{kind}: dropped, not redelivered: {again:?}"
        );
        c.detach().await.unwrap();
    }
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A rejected message on a durable queue is dead-lettered, and after a clean
/// restart the dead letter is still there and the source copy is gone. (The
/// crash window between storing the dead letter and removing the source is
/// not exercised: a crash there redelivers the source copy.)
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn durable_rejected_dead_letter_persists_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), None).await;
    assert_eq!(
        http(
            &r.mgmt,
            "PUT",
            "/queues/%2F/jobs.dlq",
            r#"{"kind":"durable"}"#
        )
        .await
        .0,
        201
    );
    assert_eq!(
        http(
            &r.mgmt,
            "PUT",
            "/queues/%2F/jobs",
            r#"{"kind":"durable","dead_letter":"/queues/jobs.dlq"}"#
        )
        .await
        .0,
        201
    );
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    s.create_producer("/queues/jobs")
        .await
        .unwrap()
        .send(Message::text("job-1"))
        .await
        .unwrap();
    let mut c = s.create_consumer("/queues/jobs").await.unwrap();
    let d = recv(&mut c).await.expect("delivery");
    c.reject(&d, None).await.unwrap();
    // Let the dead letter land before stopping.
    let mut dl = s.create_consumer("/queues/jobs.dlq").await.unwrap();
    let d = recv(&mut dl).await.expect("dead-lettered");
    assert_eq!(text(&d), "job-1");
    dl.release(&d).await.unwrap();
    conn.close().await.unwrap();
    r.shutdown.shutdown();
    drop(r);

    let r = start(config, None).await;
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let mut dl = s.create_consumer("/queues/jobs.dlq").await.unwrap();
    assert_eq!(text(&recv(&mut dl).await.expect("still there")), "job-1");
    let mut c = s.create_consumer("/queues/jobs").await.unwrap();
    let again = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
    assert!(again.is_err(), "the source copy is gone: {again:?}");
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

/// A reconnecting client whose grant on one queue was revoked while it was
/// away: after the reconnect that link ends with the broker's reason, for a
/// consumer (`recv`) and a producer (`send`) alike, and every other link is
/// restored.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn reconnect_ends_only_the_refused_link() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), Some(base_users())).await;
    let listen = r.addr.to_string();
    assert_eq!(
        http(&r.mgmt, "PUT", "/users/app", r#"{"password":"app-pw"}"#)
            .await
            .0,
        201
    );
    let both = r#"[{"address":"/queues/kept","operations":["send","receive"]},
                   {"address":"/queues/lost","operations":["send","receive"]}]"#;
    assert_eq!(
        http(&r.mgmt, "PUT", "/permissions/%2F/app", both).await.0,
        201
    );

    let conn = ConnectionBuilder::new(format!("amqp://app:app-pw@{listen}"))
        .reconnecting(true)
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let kept_p = s.create_producer("/queues/kept").await.unwrap();
    let mut kept_c = s.create_consumer("/queues/kept").await.unwrap();
    let lost_p = s.create_producer("/queues/lost").await.unwrap();
    let mut lost_c = s.create_consumer("/queues/lost").await.unwrap();

    // Revoke `lost`, then restart the broker under the client.
    let kept = r#"[{"address":"/queues/kept","operations":["send","receive"]}]"#;
    assert_eq!(
        http(&r.mgmt, "PUT", "/permissions/%2F/app", kept).await.0,
        204
    );
    r.shutdown.shutdown();
    drop(r);
    let r = start_on(config, Some(base_users()), &listen).await;

    // The kept links are restored (a send waits out the reconnect).
    let sent = tokio::time::timeout(Duration::from_secs(20), kept_p.send(Message::text("back")))
        .await
        .expect("send within the reconnect")
        .expect("kept producer restored");
    assert!(matches!(sent, DeliveryState::Accepted(_)), "{sent:?}");
    assert_eq!(
        text(&recv(&mut kept_c).await.expect("kept consumer restored")),
        "back"
    );

    // The refused ones end with the broker's reason.
    let lost = tokio::time::timeout(Duration::from_secs(5), lost_c.recv())
        .await
        .expect("the refusal reaches the consumer");
    let lost = lost.expect_err("refused consumer");
    assert!(
        lost.to_string().contains("amqp:unauthorized-access"),
        "{lost}"
    );
    let lost = lost_p
        .send(Message::text("no"))
        .await
        .expect_err("refused producer");
    assert_eq!(lost.kind(), ramqp::error::ErrorKind::Refused, "{lost}");
    assert!(
        lost.to_string().contains("amqp:unauthorized-access"),
        "{lost}"
    );
    conn.close().await.ok();
    r.shutdown.shutdown();
}

/// A reconnecting client whose link is refused for a reason that clears (an
/// exchange not declared yet on the restarted broker) keeps retrying the
/// re-attach, and the link works once it's there.
#[tokio::test]
async fn reconnect_retries_a_link_refused_for_a_reason_that_clears() {
    let declare = r#"{"type":"topic"}"#;
    let r = start(BrokerConfig::default(), None).await;
    let listen = r.addr.to_string();
    assert_eq!(
        http(&r.mgmt, "PUT", "/exchanges/%2F/ev", declare).await.0,
        201
    );
    let conn = ConnectionBuilder::new(format!("amqp://{listen}"))
        .reconnecting(true)
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let p = s.create_producer("/exchanges/ev/k").await.unwrap();
    r.shutdown.shutdown();
    drop(r);

    // The restarted broker has no `ev` yet: the re-attach is refused
    // (`not-found`) until it's declared a moment later.
    let r = start_on(BrokerConfig::default(), None, &listen).await;
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert_eq!(
        http(&r.mgmt, "PUT", "/exchanges/%2F/ev", declare).await.0,
        201
    );
    let sent = tokio::time::timeout(Duration::from_secs(20), p.send(Message::text("x")))
        .await
        .expect("send within the retry budget")
        .expect("the link was restored");
    // No binding: unroutable, which an exchange settles `released`.
    assert!(matches!(sent, DeliveryState::Released(_)), "{sent:?}");
    conn.close().await.ok();
    r.shutdown.shutdown();
}

/// Inside a transaction, a `rejected` (and a bare `modified{undeliverable-here}`)
/// is staged: a rollback requeues the message, a commit dead-letters it.
#[tokio::test]
async fn transactional_reject_dead_letters_on_commit_only() {
    use ramqp::types::messaging::{Outcome, Rejected};

    let r = start(BrokerConfig::default(), None).await;
    for (name, body) in [
        ("tx.dlq", r#"{"kind":"transient"}"#),
        (
            "tx",
            r#"{"kind":"transient","dead_letter":"/queues/tx.dlq"}"#,
        ),
    ] {
        let (st, b) = http(&r.mgmt, "PUT", &format!("/queues/%2F/{name}"), body).await;
        assert_eq!(st, 201, "{name}: {b}");
    }
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let ctl = s.create_transaction_controller().await.unwrap();
    let p = s.create_producer("/queues/tx").await.unwrap();
    p.send(Message::text("rejected")).await.unwrap();
    p.send(Message::text("undeliverable")).await.unwrap();
    let mut c = s.create_consumer("/queues/tx").await.unwrap();
    let mut dl = s.create_consumer("/queues/tx.dlq").await.unwrap();
    // Only `undeliverable-here`: no `delivery-failed`.
    let undeliverable = || {
        Outcome::Modified(Modified {
            undeliverable_here: Some(true),
            ..Default::default()
        })
    };
    let settle = |text: &str| {
        if text == "rejected" {
            Outcome::Rejected(Rejected::default())
        } else {
            undeliverable()
        }
    };

    // Rolled back: both come back to the queue, nothing is dead-lettered.
    let id = ctl.declare().await.unwrap();
    for _ in 0..2 {
        let d = recv(&mut c).await.expect("delivery");
        c.settle_in_txn(&d, id.clone(), settle(&text(&d)))
            .await
            .unwrap();
    }
    ctl.rollback(id).await.unwrap();
    let none = tokio::time::timeout(Duration::from_millis(300), dl.recv()).await;
    assert!(none.is_err(), "nothing dead-lettered on rollback: {none:?}");

    // Committed: both are dead-lettered.
    let id = ctl.declare().await.unwrap();
    for _ in 0..2 {
        let d = recv(&mut c).await.expect("redelivery after rollback");
        c.settle_in_txn(&d, id.clone(), settle(&text(&d)))
            .await
            .unwrap();
    }
    ctl.commit(id).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..2 {
        let d = recv(&mut dl).await.expect("dead-lettered on commit");
        got.push(text(&d));
        dl.accept(&d).await.unwrap();
    }
    got.sort();
    assert_eq!(got, ["rejected", "undeliverable"]);
    let again = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
    assert!(again.is_err(), "not redelivered: {again:?}");
    conn.close().await.unwrap();
    r.shutdown.shutdown();
}

// ---------------------------------------------------------------- #50

/// After a restart the admin API and `/metrics` report the queues on disk
/// before anything attaches: a standalone quorum queue's `GET` is its
/// backlog (not `404`), and the listing and metrics include both durable and
/// quorum queues. An autoscaler reading them must see the backlog to start
/// a consumer at all.
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn queues_on_disk_are_visible_after_restart_before_any_attach() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), None).await;
    for (name, kind) in [("qq", "quorum"), ("dq", "durable")] {
        let body = format!(r#"{{"kind":"{kind}"}}"#);
        let path = format!("/queues/%2F/{name}");
        assert_eq!(http(&r.mgmt, "PUT", &path, &body).await.0, 201);
    }
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    for address in ["/queues/qq", "/queues/dq"] {
        let p = s.create_producer(address).await.unwrap();
        for _ in 0..5 {
            p.send(Message::text("backlog")).await.unwrap();
        }
    }
    conn.close().await.unwrap();
    r.shutdown.shutdown();
    drop(r); // releases the store

    let r = start(config, None).await;
    // The listing and metrics fill in as the queues on disk are recovered.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    let listed = loop {
        let (status, body) = http(&r.mgmt, "GET", "/queues", "").await;
        assert_eq!(status, 200, "{body}");
        if body.contains("\"name\":\"qq\"") && body.contains("\"name\":\"dq\"") {
            break body;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "queues on disk never listed: {body}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    let listed: serde_json::Value = serde_json::from_str(&listed).unwrap();
    for q in listed.as_array().unwrap() {
        assert_eq!(q["ready"], 5, "{q}");
    }
    let (_, metrics) = http(&r.mgmt, "GET", "/metrics", "").await;
    for line in [
        r#"ramqp_queue_ready{queue="qq",kind="quorum"} 5"#,
        r#"ramqp_queue_ready{queue="dq",kind="durable"} 5"#,
    ] {
        assert!(metrics.contains(line), "missing `{line}` in:\n{metrics}");
    }
    for name in ["qq", "dq"] {
        let (status, body) = http(&r.mgmt, "GET", &format!("/queues/%2F/{name}"), "").await;
        assert_eq!(status, 200, "{name}: {body}");
        let q: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(q["ready"], 5, "{name}: {body}");
    }
    r.shutdown.shutdown();
}

/// A standalone quorum queue on disk is found by `GET` even if nothing has
/// started it yet (the lookup recovers it itself, like a durable queue).
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn quorum_queue_get_recovers_it_on_demand() {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let r = start(config.clone(), None).await;
    assert_eq!(
        http(&r.mgmt, "PUT", "/queues/%2F/lazy", r#"{"kind":"quorum"}"#)
            .await
            .0,
        201
    );
    let conn = ConnectionBuilder::new(format!("amqp://{}", r.addr))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let p = s.create_producer("/quorum/lazy").await.unwrap();
    for _ in 0..3 {
        p.send(Message::text("x")).await.unwrap();
    }
    conn.close().await.unwrap();
    r.shutdown.shutdown();
    drop(r);

    // Straight after the restart, whether or not startup recovery has
    // reached it yet.
    let r = start(config, None).await;
    let (status, body) = http(&r.mgmt, "GET", "/queues/%2F/lazy?kind=quorum", "").await;
    assert_eq!(status, 200, "{body}");
    let q: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(q["ready"], 3, "{body}");
    r.shutdown.shutdown();
}
