//! Client-facing auth tests (broker.md Phase 9): SCRAM authentication
//! against verifier storage, per-address authorization at attach, and
//! vhost isolation.

use std::sync::Arc;
use std::time::Duration;

use ramqp::sasl::SaslProfile;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{
    Authenticator, Broker, BrokerConfig, Credentials, Operation, ShutdownHandle, StaticScram,
};

async fn start_with(auth: Arc<dyn Authenticator>) -> (std::net::SocketAddr, ShutdownHandle) {
    let bound = Broker::new(BrokerConfig::default())
        .with_authenticator(auth)
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

fn scram_profile(username: &str, password: &str) -> SaslProfile {
    SaslProfile::Scram {
        mechanism: ramqp::sasl::ScramMechanism::Sha256,
        username: username.to_owned(),
        password: password.to_owned(),
    }
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

/// SCRAM-SHA-256 end to end: mutual auth against verifier-based storage.
#[tokio::test]
async fn scram_authentication_round_trip() {
    let auth = Arc::new(StaticScram::new().with_user("alice", "correct horse"));
    let (addr, shutdown) = start_with(auth).await;

    // Right password: connects and works.
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .sasl(scram_profile("alice", "correct horse"))
        .connect()
        .await
        .expect("scram connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer("/queues/scram").await.expect("p");
    producer.send(Message::text("hi")).await.expect("send");
    conn.close().await.expect("close");

    // Wrong password: refused.
    let err = ConnectionBuilder::new(format!("amqp://{addr}"))
        .sasl(scram_profile("alice", "wrong"))
        .connect()
        .await;
    assert!(err.is_err(), "wrong password must fail");

    // Unknown user: refused.
    let err = ConnectionBuilder::new(format!("amqp://{addr}"))
        .sasl(scram_profile("mallory", "whatever"))
        .connect()
        .await;
    assert!(err.is_err(), "unknown user must fail");

    // PLAIN (not offered): refused.
    let err = ConnectionBuilder::new(format!("amqp://alice:correct horse@{addr}"))
        .connect()
        .await;
    assert!(
        err.is_err(),
        "PLAIN must not be accepted by a SCRAM-only broker"
    );

    shutdown.shutdown();
}

/// Per-address authorization: consumers of `secret-*` require the right
/// identity; the refusal is link-level (the session survives).
#[tokio::test]
async fn per_address_authorization_gates_attaches() {
    #[derive(Debug)]
    struct Rules;
    impl Authenticator for Rules {
        fn mechanisms(&self) -> &[&'static str] {
            &["PLAIN", "ANONYMOUS"]
        }
        fn verify(&self, _credentials: Credentials<'_>) -> bool {
            true
        }
        fn authorize(
            &self,
            identity: Option<&str>,
            _vhost: &str,
            address: &str,
            operation: Operation,
        ) -> bool {
            // Only "auditor" may consume from secret queues; anyone may send.
            if address.contains("secret") && operation == Operation::Receive {
                return identity == Some("auditor");
            }
            true
        }
    }
    let (addr, shutdown) = start_with(Arc::new(Rules)).await;

    // A random user can produce but not consume.
    let conn = ConnectionBuilder::new(format!("amqp://bob:pw@{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session
        .create_producer("/queues/secret-audit")
        .await
        .expect("send is allowed");
    producer.send(Message::text("entry")).await.expect("send");
    let mut consumer = session
        .create_consumer("/queues/secret-audit")
        .await
        .expect("attach completes");
    let denied = tokio::time::timeout(Duration::from_secs(5), consumer.recv())
        .await
        .expect("refusal arrives");
    assert!(denied.is_err(), "bob must not consume secrets: {denied:?}");
    // The session survives: other addresses still work.
    let mut ok = session.create_consumer("/queues/open").await.expect("open");
    producer_send(&session, "/queues/open", "fine").await;
    let d = ok.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "fine");
    ok.accept(&d).await.expect("accept");
    conn.close().await.expect("close");

    // The auditor can consume.
    let conn = ConnectionBuilder::new(format!("amqp://auditor:pw@{addr}"))
        .connect()
        .await
        .expect("connect");
    let session = conn.begin_session().await.expect("session");
    let mut consumer = session
        .create_consumer("/queues/secret-audit")
        .await
        .expect("consumer");
    let d = consumer.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "entry");
    consumer.accept(&d).await.expect("accept");
    conn.close().await.expect("close");
    shutdown.shutdown();
}

async fn producer_send(session: &ramqp::Session, address: &str, text: &str) {
    let p = session.create_producer(address).await.expect("producer");
    p.send(Message::text(text)).await.expect("send");
}

/// HIGH-10 (issue #19): a SCRAM user bound to a vhost cannot attach inside
/// another vhost — authenticated no longer means unrestrained.
#[tokio::test]
async fn vhost_grants_confine_authenticated_users() {
    let auth = Arc::new(
        StaticScram::new()
            .with_user("tenant-user", "pw")
            .with_user_vhosts("tenant-user", &["tenant-a"]),
    );
    let (addr, shutdown) = start_with(auth).await;
    let connect_vhost = |vhost: &str| {
        let mut config = ramqp::Config::default();
        config.connection.hostname = Some(format!("vhost:{vhost}"));
        ConnectionBuilder::new(format!("amqp://{addr}"))
            .config(config)
            .sasl(scram_profile("tenant-user", "pw"))
    };

    // Inside the granted vhost: full service.
    let conn = connect_vhost("tenant-a").connect().await.expect("granted");
    let sess = conn.begin_session().await.expect("sess");
    producer_send(&sess, "/queues/mine", "ok").await;
    conn.close().await.expect("close");

    // In any other vhost the attach is refused (surfaces on first use).
    let conn = connect_vhost("tenant-b")
        .connect()
        .await
        .expect("authn still succeeds");
    let sess = conn.begin_session().await.expect("sess");
    let mut denied = sess
        .create_consumer("/queues/mine")
        .await
        .expect("attach completes");
    let refusal = tokio::time::timeout(Duration::from_secs(5), denied.recv())
        .await
        .expect("refusal arrives");
    assert!(refusal.is_err(), "foreign-vhost attach must be refused");
    conn.close().await.expect("close");
    shutdown.shutdown();
}

/// HIGH-6 (issue #19): the storage key is `<vhost>/<name>`, so a client
/// must not be able to cross the separator — a default-vhost attach to
/// `/queues/tenant-a/secret` would otherwise land on tenant-a's `secret`
/// key, below the authz layer. Vhosts containing `/` are refused at open.
#[tokio::test]
async fn cross_tenant_addressing_is_refused() {
    let (addr, shutdown) = start_with(Arc::new(ramqp_broker::AllowAll)).await;
    let connect_vhost = |vhost: &str| {
        let mut config = ramqp::Config::default();
        config.connection.hostname = Some(format!("vhost:{vhost}"));
        ConnectionBuilder::new(format!("amqp://{addr}")).config(config)
    };

    // Tenant A owns /queues/secret and has a message in it.
    let conn_a = connect_vhost("tenant-a").connect().await.expect("a");
    let sess_a = conn_a.begin_session().await.expect("sess a");
    producer_send(&sess_a, "/queues/secret", "private").await;

    // A default-vhost client addressing across the separator is refused:
    // the attach completes with a null terminus + detach, so the refusal
    // surfaces on the first receive/send.
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .expect("default vhost connect");
    let sess = conn.begin_session().await.expect("sess");
    let mut crosser = sess
        .create_consumer("/queues/tenant-a/secret")
        .await
        .expect("attach completes");
    let denied = tokio::time::timeout(Duration::from_secs(5), crosser.recv())
        .await
        .expect("refusal arrives");
    assert!(
        denied.is_err(),
        "cross-tenant consumer must be refused: {denied:?}"
    );

    // A vhost containing '/' is refused at the open.
    assert!(
        connect_vhost("tenant/a").connect().await.is_err(),
        "vhost with '/' must fail the open"
    );

    // Tenant A's message is untouched and still its own.
    let mut cons = sess_a.create_consumer("/queues/secret").await.expect("ca");
    let d = cons.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "private");
    cons.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
    conn_a.close().await.expect("close a");
    shutdown.shutdown();
}

/// Vhosts: a hostname of `vhost:<name>` namespaces every queue — the same
/// address in two vhosts is two queues.
#[tokio::test]
async fn vhosts_isolate_queues() {
    let (addr, shutdown) = start_with(Arc::new(ramqp_broker::AllowAll)).await;

    let connect_vhost = |vhost: &str| {
        let mut config = ramqp::Config::default();
        config.connection.hostname = Some(format!("vhost:{vhost}"));
        ConnectionBuilder::new(format!("amqp://{addr}")).config(config)
    };

    let conn_a = connect_vhost("tenant-a").connect().await.expect("a");
    let conn_b = connect_vhost("tenant-b").connect().await.expect("b");
    let sess_a = conn_a.begin_session().await.expect("sess a");
    let sess_b = conn_b.begin_session().await.expect("sess b");

    producer_send(&sess_a, "/queues/inbox", "for a").await;
    producer_send(&sess_b, "/queues/inbox", "for b").await;

    let mut cons_b = sess_b.create_consumer("/queues/inbox").await.expect("cb");
    let d = cons_b.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "for b", "tenant B sees only its own message");
    cons_b.accept(&d).await.expect("accept");
    let extra = tokio::time::timeout(Duration::from_millis(300), cons_b.recv()).await;
    assert!(extra.is_err(), "tenant A's message must not leak into B");

    let mut cons_a = sess_a.create_consumer("/queues/inbox").await.expect("ca");
    let d = cons_a.recv().await.expect("delivery");
    assert_eq!(text_of(&d), "for a");
    cons_a.accept(&d).await.expect("accept");

    conn_a.close().await.expect("close a");
    conn_b.close().await.expect("close b");
    shutdown.shutdown();
}
