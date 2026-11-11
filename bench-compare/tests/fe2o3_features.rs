//! Cross-implementation interop for the 0.10 broker features: the
//! independent `fe2o3-amqp` client against `ramqp-broker` configured the way
//! a RabbitMQ replacement is — user logins (PLAIN and SCRAM checked against
//! stored verifiers), RabbitMQ 4.x exchange addresses, and `amqps`.

use std::sync::Arc;
use std::time::Duration;

use fe2o3_amqp::link::receiver::CreditMode;
use fe2o3_amqp::sasl_profile::{SaslProfile, SaslScramSha256, SaslScramSha512};
use fe2o3_amqp::types::messaging::{Message, Properties};
use fe2o3_amqp::{Connection, Receiver, Sender, Session};
use ramqp_broker::{
    Broker, BrokerConfig, ExchangeKind, Permission, ShutdownHandle, StaticUsers, TlsServerConfig,
};

const USER: &str = "interop";
const PASSWORD: &str = "interop-pw";

struct Fixture {
    plain: std::net::SocketAddr,
    tls: std::net::SocketAddr,
    ca_pem: String,
    shutdown: ShutdownHandle,
    _dir: tempfile_dir::Dir,
}

/// A tiny temp dir (the workspace already avoids extra deps here).
mod tempfile_dir {
    pub struct Dir(pub std::path::PathBuf);
    impl Dir {
        pub fn new() -> Self {
            let p = std::env::temp_dir().join(format!(
                "fe2o3-features-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
            std::fs::create_dir_all(&p).unwrap();
            Dir(p)
        }
    }
    impl Drop for Dir {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
}

/// A CA plus a `localhost`/`127.0.0.1` server certificate on disk.
fn pki(dir: &std::path::Path) -> (String, std::path::PathBuf, std::path::PathBuf) {
    use rcgen::{BasicConstraints, CertificateParams, IsCa, Issuer, KeyPair, KeyUsagePurpose};
    let ca_key = KeyPair::generate().unwrap();
    let mut ca = CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    ca.key_usages = vec![
        KeyUsagePurpose::KeyCertSign,
        KeyUsagePurpose::DigitalSignature,
    ];
    let ca_pem = ca.self_signed(&ca_key).unwrap().pem();
    let issuer = Issuer::new(ca, ca_key);
    let key = KeyPair::generate().unwrap();
    let leaf = CertificateParams::new(vec!["localhost".into(), "127.0.0.1".into()])
        .unwrap()
        .signed_by(&key, &issuer)
        .unwrap();
    let (c, k) = (dir.join("server.crt"), dir.join("server.key"));
    std::fs::write(&c, leaf.pem()).unwrap();
    std::fs::write(&k, key.serialize_pem()).unwrap();
    (ca_pem, c, k)
}

async fn start() -> Fixture {
    let dir = tempfile_dir::Dir::new();
    let (ca_pem, cert, key) = pki(&dir.0);
    // A password user gets SCRAM-SHA-256 and -512 verifiers (and the broker
    // offers only mechanisms every user has).
    let users = StaticUsers::new()
        .with_password(USER, PASSWORD)
        .with_permission(Permission::new("*", "", "*", true, true));
    let mut config = BrokerConfig::default();
    config.tls = Some(TlsServerConfig::new(cert, key));
    config.tls_listen = Some("127.0.0.1:0".into());
    let bound = Broker::new(config)
        .with_authenticator(Arc::new(users))
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let broker = bound.broker().clone();
    broker
        .declare_exchange("", "events", ExchangeKind::Topic, None)
        .await
        .unwrap();
    broker
        .bind_queue("", "events", "/queues/fe2o3-orders", "order.#")
        .await
        .unwrap();
    let (plain, tls) = (bound.local_addr(), bound.tls_local_addr().unwrap());
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    Fixture {
        plain,
        tls,
        ca_pem,
        shutdown,
        _dir: dir,
    }
}

async fn round_trip(conn: &mut fe2o3_amqp::connection::ConnectionHandle<()>, queue: &str) {
    let mut session = Session::begin(conn).await.expect("session");
    let mut sender = Sender::attach(&mut session, "s", queue)
        .await
        .expect("sender");
    sender.send("ping").await.expect("send accepted");
    sender.close().await.expect("close");
    let mut receiver = Receiver::attach(&mut session, "r", queue)
        .await
        .expect("receiver");
    let d = tokio::time::timeout(Duration::from_secs(5), receiver.recv::<String>())
        .await
        .expect("in time")
        .expect("delivery");
    assert_eq!(d.body(), "ping");
    receiver.accept(&d).await.expect("accept");
    receiver.close().await.expect("close");
    session.end().await.expect("end");
}

#[tokio::test]
async fn fe2o3_plain_and_scram_logins() {
    let f = start().await;
    let url = format!("amqp://{}", f.plain);
    // PLAIN (checked against the stored SCRAM verifier).
    let mut conn = Connection::builder()
        .container_id("fe2o3-plain")
        .sasl_profile(SaslProfile::Plain {
            username: USER.into(),
            password: PASSWORD.into(),
        })
        .open(url.as_str())
        .await
        .expect("PLAIN login");
    round_trip(&mut conn, "/queues/fe2o3-plain").await;
    conn.close().await.unwrap();
    // SCRAM-SHA-256 and SCRAM-SHA-512.
    let mut conn = Connection::builder()
        .container_id("fe2o3-scram256")
        .sasl_profile(SaslScramSha256::new(USER, PASSWORD))
        .open(url.as_str())
        .await
        .expect("SCRAM-SHA-256 login");
    round_trip(&mut conn, "/queues/fe2o3-scram").await;
    conn.close().await.unwrap();
    let mut conn = Connection::builder()
        .container_id("fe2o3-scram512")
        .sasl_profile(SaslScramSha512::new(USER, PASSWORD))
        .open(url.as_str())
        .await
        .expect("SCRAM-SHA-512 login");
    round_trip(&mut conn, "/queues/fe2o3-scram").await;
    conn.close().await.unwrap();
    // Wrong password and anonymous: refused.
    assert!(
        Connection::builder()
            .container_id("fe2o3-bad")
            .sasl_profile(SaslProfile::Plain {
                username: USER.into(),
                password: "nope".into(),
            })
            .open(url.as_str())
            .await
            .is_err()
    );
    assert!(
        Connection::builder()
            .container_id("fe2o3-anon")
            .sasl_profile(SaslProfile::Anonymous)
            .open(url.as_str())
            .await
            .is_err()
    );
    f.shutdown.shutdown();
}

#[tokio::test]
async fn fe2o3_publishes_through_exchanges() {
    let f = start().await;
    let mut conn = Connection::builder()
        .container_id("fe2o3-exchanges")
        .sasl_profile(SaslProfile::Plain {
            username: USER.into(),
            password: PASSWORD.into(),
        })
        .open(format!("amqp://{}", f.plain).as_str())
        .await
        .expect("login");
    let mut session = Session::begin(&mut conn).await.unwrap();
    // Routing key in the address.
    let mut keyed = Sender::attach(&mut session, "k", "/exchanges/events/order.created")
        .await
        .expect("attach to an exchange address");
    keyed.send("keyed").await.expect("accepted");
    // Keyless: routing key from properties.subject.
    let mut keyless = Sender::attach(&mut session, "nk", "/exchanges/events")
        .await
        .unwrap();
    let msg = Message::builder()
        .properties(Properties::builder().subject("order.paid").build())
        .value("by-subject")
        .build();
    keyless.send(msg).await.expect("accepted");
    // Unroutable: settled `released` (RabbitMQ 4.x).
    let mut nowhere = Sender::attach(&mut session, "n", "/exchanges/events/billing.x")
        .await
        .unwrap();
    let outcome = nowhere.send("dropped").await;
    assert!(
        matches!(
            outcome,
            Ok(fe2o3_amqp::types::messaging::Outcome::Released(_))
        ),
        "unroutable is released: {outcome:?}"
    );
    // An unknown exchange is refused at attach. Checked on its own session:
    // fe2o3-amqp 0.18 mishandles the next attach right after a refused one
    // on the SAME session (it detaches the refused link twice and fails the
    // next attach locally with IllegalState) — it reproduces identically
    // against RabbitMQ 4.x, so it is a client issue, not a broker one.
    {
        let mut other = Session::begin(&mut conn).await.unwrap();
        assert!(
            Sender::attach(&mut other, "u", "/exchanges/missing/k")
                .await
                .is_err()
        );
    }
    let mut receiver = Receiver::builder()
        .name("orders")
        .source("/queues/fe2o3-orders")
        .credit_mode(CreditMode::Auto(10))
        .attach(&mut session)
        .await
        .unwrap();
    let mut got = Vec::new();
    for _ in 0..2 {
        let d = tokio::time::timeout(Duration::from_secs(5), receiver.recv::<String>())
            .await
            .expect("in time")
            .expect("delivery");
        got.push(d.body().clone());
        receiver.accept(&d).await.unwrap();
    }
    got.sort();
    assert_eq!(got, ["by-subject", "keyed"]);
    let extra = tokio::time::timeout(Duration::from_millis(300), receiver.recv::<String>()).await;
    assert!(extra.is_err(), "the unroutable message went nowhere");
    session.end().await.unwrap();
    conn.close().await.unwrap();
    f.shutdown.shutdown();
}

#[tokio::test]
async fn fe2o3_over_amqps() {
    use rustls::pki_types::pem::PemObject;
    let f = start().await;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(f.ca_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let config = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let connector = tokio_rustls::TlsConnector::from(Arc::new(config));
    let mut conn = Connection::builder()
        .container_id("fe2o3-tls")
        .tls_connector(connector)
        .sasl_profile(SaslProfile::Plain {
            username: USER.into(),
            password: PASSWORD.into(),
        })
        .open(format!("amqps://localhost:{}", f.tls.port()).as_str())
        .await
        .expect("amqps login");
    round_trip(&mut conn, "/queues/fe2o3-tls").await;
    conn.close().await.unwrap();
    f.shutdown.shutdown();
}
