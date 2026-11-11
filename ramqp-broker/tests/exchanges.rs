//! Exchange routing end to end with the unmodified `ramqp` client (issue
//! #37 §1): RabbitMQ 4.x `/exchanges/<name>[/<key>]` addressing, fanout /
//! direct / topic kinds, subject-keyed publishes, unroutable handling,
//! aggregated confirms, transactions, live binding changes, persistence,
//! and cluster-wide routing.

use std::time::Duration;

use ramqp::types::messaging::{DeliveryState, Properties};
use ramqp::{ConnectionBuilder, Message, Session};
use ramqp_broker::{
    Broker, BrokerConfig, ExchangeError, ExchangeKind, QueuePolicy, ShutdownHandle,
};

async fn start(config: BrokerConfig) -> (Broker, std::net::SocketAddr, ShutdownHandle) {
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let broker = bound.broker().clone();
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (broker, addr, shutdown)
}

async fn session(addr: std::net::SocketAddr) -> (ramqp::Connection, Session) {
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    (conn, session)
}

fn text_of(delivery: &ramqp::Delivery) -> String {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    match delivery.message().expect("decodable").body {
        Body::Value(Value::String(s)) => s,
        other => panic!("expected text body, got {other:?}"),
    }
}

fn with_subject(text: &str, subject: &str) -> Message {
    let mut m = Message::text(text);
    m.properties = Some(Properties {
        subject: Some(subject.to_owned()),
        ..Default::default()
    });
    m
}

/// Receive exactly the given texts (any order), then assert nothing else
/// arrives within a short window.
async fn expect_exactly(consumer: &mut ramqp::Consumer, want: &[&str]) {
    let mut got = Vec::new();
    for _ in 0..want.len() {
        let d = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
            .await
            .expect("delivery in time")
            .expect("delivery");
        got.push(text_of(&d));
        consumer.accept(&d).await.expect("accept");
    }
    got.sort();
    let mut want: Vec<String> = want.iter().map(|s| (*s).to_owned()).collect();
    want.sort();
    assert_eq!(got, want);
    let extra = tokio::time::timeout(Duration::from_millis(200), consumer.recv()).await;
    assert!(extra.is_err(), "unexpected extra delivery: {extra:?}");
}

fn accepted(state: &DeliveryState) -> bool {
    matches!(state, DeliveryState::Accepted(_))
}

#[tokio::test]
async fn topic_exchange_routes_by_address_key_and_subject() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "events", ExchangeKind::Topic, None)
        .await
        .unwrap();
    broker
        .bind_queue("", "events", "/queues/orders", "order.*")
        .await
        .unwrap();
    broker
        .bind_queue("", "events", "/queues/created", "*.created")
        .await
        .unwrap();
    broker
        .bind_queue("", "events", "/queues/audit", "#")
        .await
        .unwrap();
    // A second binding of the same queue: still one copy per message.
    broker
        .bind_queue("", "events", "/queues/audit", "order.#")
        .await
        .unwrap();

    let (conn, s) = session(addr).await;
    let mut orders = s.create_consumer("/queues/orders").await.unwrap();
    let mut created = s.create_consumer("/queues/created").await.unwrap();
    let mut audit = s.create_consumer("/queues/audit").await.unwrap();

    // Key in the address.
    let p = s
        .create_producer("/exchanges/events/order.created")
        .await
        .unwrap();
    assert!(accepted(&p.send(Message::text("o1")).await.unwrap()));
    // Keyless: the routing key is each message's subject.
    let p2 = s.create_producer("/exchanges/events").await.unwrap();
    assert!(accepted(
        &p2.send(with_subject("u1", "user.created")).await.unwrap()
    ));
    assert!(accepted(
        &p2.send(with_subject("b1", "billing.paid")).await.unwrap()
    ));

    expect_exactly(&mut orders, &["o1"]).await;
    expect_exactly(&mut created, &["o1", "u1"]).await;
    expect_exactly(&mut audit, &["o1", "u1", "b1"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn direct_fanout_and_builtin_exchanges() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "bcast", ExchangeKind::Fanout, None)
        .await
        .unwrap();
    broker
        .bind_queue("", "bcast", "/queues/f1", "")
        .await
        .unwrap();
    broker
        .bind_queue("", "bcast", "/queues/f2", "ignored")
        .await
        .unwrap();
    // amq.direct exists implicitly: request/response reply routing.
    broker
        .bind_queue("", "amq.direct", "/queues/reply-client-7", "client-7")
        .await
        .unwrap();

    let (conn, s) = session(addr).await;
    let mut f1 = s.create_consumer("/queues/f1").await.unwrap();
    let mut f2 = s.create_consumer("/queues/f2").await.unwrap();
    let mut reply = s.create_consumer("/queues/reply-client-7").await.unwrap();

    let fan = s
        .create_producer("/exchanges/bcast/whatever")
        .await
        .unwrap();
    assert!(accepted(&fan.send(Message::text("all")).await.unwrap()));
    let direct = s
        .create_producer("/exchanges/amq.direct/client-7")
        .await
        .unwrap();
    assert!(accepted(
        &direct.send(Message::text("answer")).await.unwrap()
    ));
    let miss = s
        .create_producer("/exchanges/amq.direct/client-8")
        .await
        .unwrap();
    // Routed nowhere: released (RabbitMQ 4.x).
    assert!(matches!(
        miss.send(Message::text("nobody")).await.unwrap(),
        DeliveryState::Released(_)
    ));

    expect_exactly(&mut f1, &["all"]).await;
    expect_exactly(&mut f2, &["all"]).await;
    expect_exactly(&mut reply, &["answer"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn unroutable_is_released_or_goes_to_alternate() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "plain", ExchangeKind::Direct, None)
        .await
        .unwrap();
    broker
        .declare_exchange("", "alt", ExchangeKind::Direct, Some("/queues/lost"))
        .await
        .unwrap();
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/plain/nothing").await.unwrap();
    // Released (it reached no queue), like RabbitMQ 4.x.
    assert!(matches!(
        p.send(Message::text("void")).await.unwrap(),
        DeliveryState::Released(_)
    ));
    let p = s.create_producer("/exchanges/alt/nothing").await.unwrap();
    assert!(accepted(&p.send(Message::text("caught")).await.unwrap()));
    let mut lost = s.create_consumer("/queues/lost").await.unwrap();
    expect_exactly(&mut lost, &["caught"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn unknown_exchange_and_exchange_consumers_are_refused() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "events", ExchangeKind::Topic, None)
        .await
        .unwrap();
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/missing/k").await;
    let refused = match p {
        Err(_) => true,
        Ok(p) => p.send(Message::text("x")).await.is_err(),
    };
    assert!(refused, "publishing to an undeclared exchange must fail");
    let mut c = s
        .create_consumer("/exchanges/events")
        .await
        .expect("attach completes");
    let r = tokio::time::timeout(Duration::from_secs(5), c.recv())
        .await
        .expect("refusal arrives");
    assert!(r.is_err(), "consuming from an exchange must be refused");
    // The session survives the link refusals (`a` matches no binding, so
    // the publish settles `released`).
    let ok = s.create_producer("/exchanges/events/a").await.unwrap();
    assert!(matches!(
        ok.send(Message::text("fine")).await.unwrap(),
        DeliveryState::Released(_)
    ));
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn binding_changes_apply_to_attached_producers() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "ev", ExchangeKind::Topic, None)
        .await
        .unwrap();
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/ev/a.b").await.unwrap();
    let mut q = s.create_consumer("/queues/late").await.unwrap();
    p.send(Message::text("before")).await.unwrap();
    broker
        .bind_queue("", "ev", "/queues/late", "a.*")
        .await
        .unwrap();
    p.send(Message::text("bound")).await.unwrap();
    broker
        .unbind_queue("", "ev", "/queues/late", "a.*")
        .await
        .unwrap();
    p.send(Message::text("after")).await.unwrap();
    expect_exactly(&mut q, &["bound"]).await;
    // Deleting the exchange detaches the attached producer (instead of
    // silently accepting and dropping everything it sends); the session
    // itself survives.
    broker.delete_exchange("", "ev").await.unwrap();
    let gone = tokio::time::timeout(Duration::from_secs(5), p.send(Message::text("gone")))
        .await
        .expect("answer in time");
    assert!(
        !matches!(gone, Ok(DeliveryState::Accepted(_))),
        "a publish to a deleted exchange must not be accepted: {gone:?}"
    );
    let other = s.create_producer("/queues/still-fine").await.unwrap();
    assert!(accepted(&other.send(Message::text("ok")).await.unwrap()));
    assert!(matches!(
        broker.delete_exchange("", "ev").await,
        Err(ExchangeError::NotFound(_))
    ));
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn fanout_confirm_waits_for_every_queue_and_rejects_if_any_refuses() {
    let mut config = BrokerConfig::default();
    let mut tiny = QueuePolicy::default();
    tiny.max_length = Some(1);
    config.policies.push(("tiny".to_owned(), tiny));
    let (broker, addr, shutdown) = start(config).await;
    broker
        .declare_exchange("", "fan", ExchangeKind::Fanout, None)
        .await
        .unwrap();
    broker
        .bind_queue("", "fan", "/queues/big", "")
        .await
        .unwrap();
    broker
        .bind_queue("", "fan", "/queues/tiny", "")
        .await
        .unwrap();
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/fan").await.unwrap();
    assert!(accepted(&p.send(Message::text("1")).await.unwrap()));
    // `tiny` is full now: the fan-out confirm is a rejection…
    let second = p.send(Message::text("2")).await.unwrap();
    assert!(
        matches!(second, DeliveryState::Rejected(_)),
        "expected rejected, got {second:?}"
    );
    // …though `big` took its copy (no cross-queue atomicity, as RabbitMQ).
    let mut big = s.create_consumer("/queues/big").await.unwrap();
    expect_exactly(&mut big, &["1", "2"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn transactional_publish_through_an_exchange() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    broker
        .declare_exchange("", "tx", ExchangeKind::Fanout, None)
        .await
        .unwrap();
    broker.bind_queue("", "tx", "/queues/t1", "").await.unwrap();
    broker.bind_queue("", "tx", "/queues/t2", "").await.unwrap();
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/tx").await.unwrap();
    let mut t1 = s.create_consumer("/queues/t1").await.unwrap();
    let mut t2 = s.create_consumer("/queues/t2").await.unwrap();

    let ctl = s.create_transaction_controller().await.expect("controller");
    let txn_id = ctl.declare().await.expect("declare");
    p.send_with_state(
        Message::text("rolled"),
        ramqp::txn::transactional_state(txn_id.clone(), None),
    )
    .await
    .unwrap();
    ctl.rollback(txn_id).await.expect("rollback");
    let txn_id = ctl.declare().await.expect("declare");
    p.send_with_state(
        Message::text("kept"),
        ramqp::txn::transactional_state(txn_id.clone(), None),
    )
    .await
    .unwrap();
    // Staged: invisible until commit.
    let early = tokio::time::timeout(Duration::from_millis(200), t1.recv()).await;
    assert!(early.is_err(), "staged publish must not be visible");
    ctl.commit(txn_id).await.expect("commit");

    expect_exactly(&mut t1, &["kept"]).await;
    expect_exactly(&mut t2, &["kept"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

#[tokio::test]
async fn admin_ops_are_idempotent_and_validated() {
    let (broker, _addr, shutdown) = start(BrokerConfig::default()).await;
    assert_eq!(
        broker
            .declare_exchange("", "e", ExchangeKind::Topic, None)
            .await,
        Ok(true)
    );
    assert_eq!(
        broker
            .declare_exchange("", "e", ExchangeKind::Topic, None)
            .await,
        Ok(false)
    );
    assert!(matches!(
        broker
            .declare_exchange("", "e", ExchangeKind::Direct, None)
            .await,
        Err(ExchangeError::Conflict(_))
    ));
    assert!(matches!(
        broker
            .declare_exchange("", "a/b", ExchangeKind::Direct, None)
            .await,
        Err(ExchangeError::Invalid(_))
    ));
    assert!(matches!(
        broker.bind_queue("", "missing", "/queues/q", "k").await,
        Err(ExchangeError::NotFound(_))
    ));
    assert!(matches!(
        broker.bind_queue("", "e", "orders", "k").await,
        Err(ExchangeError::Invalid(_))
    ));
    assert_eq!(broker.bind_queue("", "e", "/queues/q", "k").await, Ok(true));
    assert_eq!(
        broker.bind_queue("", "e", "/queues/q", "k").await,
        Ok(false)
    );
    shutdown.shutdown();
}

/// Definitions survive a restart (standalone, redb store).
#[cfg(feature = "store-redb")]
#[tokio::test]
async fn definitions_survive_restart() {
    let dir = tempfile::tempdir().unwrap();
    let config = || {
        let mut c = BrokerConfig::default();
        c.data_dir = Some(dir.path().to_path_buf());
        c
    };
    {
        let (broker, _addr, shutdown) = start(config()).await;
        broker
            .declare_exchange("", "persist", ExchangeKind::Topic, None)
            .await
            .unwrap();
        broker
            .bind_queue("", "persist", "/durable/kept", "k.#")
            .await
            .unwrap();
        shutdown.shutdown();
    }
    // The previous instance releases the store lock asynchronously: the
    // restarted broker binds at once, and refuses exchange changes as
    // Unavailable (rather than serving an empty topology) until it can
    // read the persisted definitions.
    let (broker, addr, shutdown) = start(config()).await;
    let redeclare = loop {
        match broker
            .declare_exchange("", "persist", ExchangeKind::Topic, None)
            .await
        {
            Err(ExchangeError::Unavailable(_)) => {
                tokio::time::sleep(Duration::from_millis(100)).await
            }
            other => break other,
        }
    };
    assert_eq!(redeclare, Ok(false), "exchange came back");
    assert_eq!(
        broker
            .bind_queue("", "persist", "/durable/kept", "k.#")
            .await,
        Ok(false),
        "binding came back"
    );
    let (conn, s) = session(addr).await;
    let p = s.create_producer("/exchanges/persist/k.x").await.unwrap();
    assert!(accepted(&p.send(Message::text("routed")).await.unwrap()));
    let mut c = s.create_consumer("/durable/kept").await.unwrap();
    expect_exactly(&mut c, &["routed"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

/// Review H1: a destination that cannot be resolved (here: the queue cap)
/// rejects the publish — nothing is half-delivered, nothing is cached — so
/// the producer can retry instead of losing that queue's copy silently.
#[tokio::test]
async fn unresolvable_destination_rejects_instead_of_skipping() {
    let mut config = BrokerConfig::default();
    config.max_queues = 1;
    let (broker, addr, shutdown) = start(config).await;
    broker
        .declare_exchange("", "ev", ExchangeKind::Fanout, None)
        .await
        .unwrap();
    broker.declare_queue("", "/queues/first").await.unwrap();
    broker
        .bind_queue("", "ev", "/queues/first", "")
        .await
        .unwrap();
    broker
        .bind_queue("", "ev", "/queues/over-cap", "")
        .await
        .unwrap();
    let (conn, s) = session(addr).await;
    let mut first = s.create_consumer("/queues/first").await.unwrap();
    let p = s.create_producer("/exchanges/ev").await.unwrap();
    for _ in 0..2 {
        let state = p.send(Message::text("x")).await.unwrap();
        assert!(
            matches!(state, DeliveryState::Rejected(_)),
            "expected rejected, got {state:?}"
        );
    }
    // Nothing reached the queue that did resolve.
    let none = tokio::time::timeout(Duration::from_millis(200), first.recv()).await;
    assert!(none.is_err(), "no partial delivery");
    // Once the over-cap destination is unbound, routing works (no stale cache).
    broker
        .unbind_queue("", "ev", "/queues/over-cap", "")
        .await
        .unwrap();
    assert!(accepted(&p.send(Message::text("ok")).await.unwrap()));
    expect_exactly(&mut first, &["ok"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

/// Review L2: `$`-prefixed queue names are reserved (the transaction
/// coordinator authorizes as `$coordinator`).
#[tokio::test]
async fn dollar_queue_names_are_reserved() {
    let (broker, addr, shutdown) = start(BrokerConfig::default()).await;
    assert!(
        broker
            .declare_queue("", "/queues/$coordinator")
            .await
            .is_err()
    );
    assert!(matches!(
        broker.bind_queue("", "amq.direct", "/queues/$x", "k").await,
        Err(ExchangeError::Invalid(_))
    ));
    let (conn, s) = session(addr).await;
    let mut c = s
        .create_consumer("$coordinator")
        .await
        .expect("attach completes");
    let r = tokio::time::timeout(Duration::from_secs(5), c.recv())
        .await
        .expect("refusal arrives");
    assert!(r.is_err(), "a `$` queue must not be auto-declared");
    conn.close().await.unwrap();
    shutdown.shutdown();
}

/// RabbitMQ compatibility: its clients send `vhost:/` for the default vhost
/// — that is OUR default vhost (same queues as no vhost at all).
#[tokio::test]
async fn rabbitmq_default_vhost_name_maps_to_ours() {
    let (_broker, addr, shutdown) = start(BrokerConfig::default()).await;
    let mut config = ramqp::Config::default();
    config.connection.hostname = Some("vhost:/".to_owned());
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .config(config)
        .connect()
        .await
        .expect("vhost:/ is accepted");
    let s = conn.begin_session().await.unwrap();
    let p = s.create_producer("/queues/shared").await.unwrap();
    assert!(accepted(&p.send(Message::text("x")).await.unwrap()));
    conn.close().await.unwrap();
    // A plain connection (default vhost) sees the same queue.
    let (conn, s) = session(addr).await;
    let mut c = s.create_consumer("/queues/shared").await.unwrap();
    expect_exactly(&mut c, &["x"]).await;
    conn.close().await.unwrap();
    shutdown.shutdown();
}

/// RabbitMQ compatibility: its client libraries open `/management` links at
/// connect and send topology requests there. The broker accepts the links
/// (so those clients connect and publish) and answers every request with a
/// correlated `501` reply instead of letting it time out.
#[tokio::test]
async fn rabbitmq_management_requests_get_a_501_reply() {
    use ramqp::codec::Value;
    use ramqp::types::messaging::{Body, MessageId};
    let (_broker, addr, shutdown) = start(BrokerConfig::default()).await;
    let (conn, s) = session(addr).await;
    let mut replies = s.create_consumer("/management").await.expect("reply link");
    let requests = s
        .create_producer("/management")
        .await
        .expect("request link");
    let mut req = Message::text("{}");
    req.properties = Some(Properties {
        message_id: Some(MessageId::String("req-1".into())),
        to: Some("/queues/orders".into()),
        subject: Some("PUT".into()),
        reply_to: Some("$me".into()),
        ..Default::default()
    });
    requests.send(req).await.expect("request delivered");
    let d = tokio::time::timeout(Duration::from_secs(5), replies.recv())
        .await
        .expect("a reply, not a timeout")
        .expect("reply");
    let msg = d.message().expect("decodable reply");
    let props = msg.properties.expect("reply properties");
    assert_eq!(
        props.correlation_id,
        Some(MessageId::String("req-1".into()))
    );
    assert_eq!(props.subject.as_deref(), Some("501"));
    assert!(matches!(msg.body, Body::Value(Value::String(ref t)) if t.contains("admin API")));
    // `/management` is not a queue: nothing was declared under that name.
    assert!(
        broker_has_no_queue_named(addr, "management").await,
        "`/management` must not auto-declare a queue"
    );
    conn.close().await.unwrap();
    shutdown.shutdown();
}

/// Nothing published to `/management` ever lands in a queue a consumer of
/// `/queues/management` could read.
async fn broker_has_no_queue_named(addr: std::net::SocketAddr, name: &str) -> bool {
    let (conn, s) = session(addr).await;
    let mut c = s.create_consumer(&format!("/queues/{name}")).await.unwrap();
    let got = tokio::time::timeout(Duration::from_millis(300), c.recv()).await;
    conn.close().await.unwrap();
    got.is_err()
}
