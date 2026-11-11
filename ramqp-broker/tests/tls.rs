//! Server TLS end to end (issue #37 §4; `tls` or `fips` feature): the
//! `amqps` listener, mutual TLS, certificate hot reload, the management
//! endpoint over TLS, and mutual TLS on the cluster fabric. Certificates are
//! minted per test by a throwaway CA.
#![cfg(any(feature = "tls", feature = "fips"))]

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ramqp::transport::TlsConfig;
use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig, ClusterMemberConfig, TlsServerConfig};
use rcgen::{
    BasicConstraints, CertificateParams, DnType, ExtendedKeyUsagePurpose, IsCa, Issuer, KeyPair,
    KeyUsagePurpose,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

/// A throwaway CA that writes leaf certificates to a temp dir.
struct Pki {
    dir: tempfile::TempDir,
    issuer: Issuer<'static, KeyPair>,
    ca_pem: String,
}

impl Pki {
    fn new(name: &str) -> Self {
        let key = KeyPair::generate().unwrap();
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        params.distinguished_name.push(DnType::CommonName, name);
        params.key_usages = vec![
            KeyUsagePurpose::KeyCertSign,
            KeyUsagePurpose::CrlSign,
            KeyUsagePurpose::DigitalSignature,
        ];
        let ca_pem = params.self_signed(&key).unwrap().pem();
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ca.crt"), &ca_pem).unwrap();
        Pki {
            dir,
            issuer: Issuer::new(params, key),
            ca_pem,
        }
    }

    fn ca_file(&self) -> PathBuf {
        self.dir.path().join("ca.crt")
    }

    /// Mint a leaf for `127.0.0.1`/`localhost` named `cn`; returns PEMs.
    fn leaf(&self, cn: &str) -> (String, String) {
        self.leaf_for(cn, &["127.0.0.1", "localhost"])
    }

    /// Mint a leaf with these subject alternative names.
    fn leaf_for(&self, cn: &str, sans: &[&str]) -> (String, String) {
        let key = KeyPair::generate().unwrap();
        let mut params =
            CertificateParams::new(sans.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
                .unwrap();
        params.distinguished_name.push(DnType::CommonName, cn);
        params.extended_key_usages = vec![
            ExtendedKeyUsagePurpose::ServerAuth,
            ExtendedKeyUsagePurpose::ClientAuth,
        ];
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        (cert.pem(), key.serialize_pem())
    }

    /// Mint a leaf into `<name>.crt` / `<name>.key`.
    fn leaf_files(&self, name: &str) -> (PathBuf, PathBuf) {
        let (cert, key) = self.leaf(name);
        let (c, k) = (
            self.dir.path().join(format!("{name}.crt")),
            self.dir.path().join(format!("{name}.key")),
        );
        std::fs::write(&c, cert).unwrap();
        std::fs::write(&k, key).unwrap();
        (c, k)
    }
}

fn client_tls(pki: &Pki, client: Option<(&Path, &Path)>) -> TlsConfig {
    TlsConfig {
        root_ca_pem: vec![pki.ca_pem.clone().into_bytes()],
        webpki_roots: false,
        client_auth_pem: client
            .map(|(c, k)| (std::fs::read(c).unwrap(), std::fs::read(k).unwrap())),
        ..Default::default()
    }
}

fn text_of(delivery: &ramqp::Delivery) -> String {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    match delivery.message().expect("decodable").body {
        Body::Value(Value::String(s)) => s,
        other => panic!("expected text body, got {other:?}"),
    }
}

async fn start(config: BrokerConfig) -> (std::net::SocketAddr, ramqp_broker::ShutdownHandle) {
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let tls_addr = bound.tls_local_addr().expect("TLS listener");
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (tls_addr, shutdown)
}

async fn round_trip(addr: std::net::SocketAddr, tls: TlsConfig, queue: &str) -> Result<(), String> {
    let conn = ConnectionBuilder::new(format!("amqps://localhost:{}", addr.port()))
        .tls(tls)
        .connect()
        .await
        .map_err(|e| e.to_string())?;
    let session = conn.begin_session().await.map_err(|e| e.to_string())?;
    let p = session
        .create_producer(queue)
        .await
        .map_err(|e| e.to_string())?;
    p.send(Message::text("over tls"))
        .await
        .map_err(|e| e.to_string())?;
    let mut c = session
        .create_consumer(queue)
        .await
        .map_err(|e| e.to_string())?;
    let d = c.recv().await.map_err(|e| e.to_string())?;
    assert_eq!(text_of(&d), "over tls");
    c.accept(&d).await.map_err(|e| e.to_string())?;
    conn.close().await.map_err(|e| e.to_string())
}

/// A `fips` build really runs on the FIPS module.
#[cfg(feature = "fips")]
#[test]
fn fips_build_reports_fips_mode() {
    assert!(ramqp_broker::fips_enabled());
}

#[tokio::test]
async fn amqps_listener_serves_tls_clients() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let mut config = BrokerConfig::default();
    config.tls = Some(TlsServerConfig::new(cert, key));
    config.tls_listen = Some("127.0.0.1:0".to_owned());
    let (addr, shutdown) = start(config).await;
    round_trip(addr, client_tls(&pki, None), "/queues/tls")
        .await
        .expect("amqps round trip");
    // A client that does not trust the CA is refused at the handshake.
    let other = Pki::new("other-ca");
    assert!(
        round_trip(addr, client_tls(&other, None), "/queues/tls")
            .await
            .is_err()
    );
    shutdown.shutdown();
}

#[tokio::test]
async fn mutual_tls_requires_a_client_certificate() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let (ccert, ckey) = pki.leaf_files("client");
    let mut tls = TlsServerConfig::new(cert, key);
    tls.client_ca_file = Some(pki.ca_file());
    let mut config = BrokerConfig::default();
    config.tls = Some(tls);
    config.tls_listen = Some("127.0.0.1:0".to_owned());
    let (addr, shutdown) = start(config).await;
    assert!(
        round_trip(addr, client_tls(&pki, None), "/queues/m")
            .await
            .is_err(),
        "no client certificate: refused"
    );
    // A certificate from a different CA is refused too.
    let rogue = Pki::new("rogue");
    let (rc, rk) = rogue.leaf_files("client");
    assert!(
        round_trip(addr, client_tls(&pki, Some((&rc, &rk))), "/queues/m")
            .await
            .is_err(),
        "foreign client certificate: refused"
    );
    round_trip(addr, client_tls(&pki, Some((&ccert, &ckey))), "/queues/m")
        .await
        .expect("trusted client certificate: served");
    shutdown.shutdown();
}

/// The leaf certificate a TLS server presents to a fresh handshake.
async fn presented_cert(addr: std::net::SocketAddr, ca_pem: &str) -> Vec<u8> {
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(ca_pem.as_bytes()) {
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
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = connector
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .expect("handshake");
    tls.get_ref().1.peer_certificates().unwrap()[0].to_vec()
}

#[tokio::test]
async fn certificates_reload_without_a_restart() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let mut tls = TlsServerConfig::new(cert.clone(), key.clone());
    tls.reload_interval = Duration::from_millis(100);
    let mut config = BrokerConfig::default();
    config.tls = Some(tls);
    config.tls_listen = Some("127.0.0.1:0".to_owned());
    let (addr, shutdown) = start(config).await;
    let first = presented_cert(addr, &pki.ca_pem).await;

    // Rotate in place (as cert-manager does): a new key and certificate.
    let (new_cert, new_key) = pki.leaf("server-rotated");
    std::fs::write(&key, &new_key).unwrap();
    std::fs::write(&cert, &new_cert).unwrap();
    let mut rotated = first.clone();
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        rotated = presented_cert(addr, &pki.ca_pem).await;
        if rotated != first {
            break;
        }
    }
    assert_ne!(
        rotated, first,
        "new handshakes present the rotated certificate"
    );

    // A broken file (e.g. mid-rotation) keeps the current certificate.
    std::fs::write(&cert, "not a certificate").unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert_eq!(presented_cert(addr, &pki.ca_pem).await, rotated);
    round_trip(addr, client_tls(&pki, None), "/queues/r")
        .await
        .expect("still serving");
    shutdown.shutdown();
}

#[tokio::test]
async fn management_endpoint_over_tls() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mgmt = l.local_addr().unwrap();
    drop(l);
    let mut config = BrokerConfig::default();
    config.tls = Some(TlsServerConfig::new(cert, key));
    config.management_listen = Some(mgmt.to_string());
    config.management_tls = true;
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    // Plain HTTP gets nothing useful.
    let mut plain = tokio::net::TcpStream::connect(mgmt).await.unwrap();
    plain
        .write_all(b"GET /metrics HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = plain.read_to_end(&mut buf).await;
    assert!(!String::from_utf8_lossy(&buf).contains("ramqp_connections"));

    // HTTPS works.
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(pki.ca_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_no_client_auth();
    let tcp = tokio::net::TcpStream::connect(mgmt).await.unwrap();
    let mut tls = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .expect("handshake");
    tls.write_all(b"GET /metrics HTTP/1.1\r\nhost: x\r\n\r\n")
        .await
        .unwrap();
    let mut buf = Vec::new();
    let _ = tls.read_to_end(&mut buf).await;
    let text = String::from_utf8_lossy(&buf);
    assert!(text.starts_with("HTTP/1.1 200"), "{text}");
    assert!(text.contains("ramqp_connections"));
    shutdown.shutdown();
}

/// A 3-node cluster whose fabric runs mutual TLS: it forms, and any node
/// serves any quorum queue through the encrypted fabric.
#[tokio::test(flavor = "multi_thread")]
async fn cluster_fabric_over_mutual_tls() {
    let pki = Pki::new("cluster-ca");
    let mut fabric_addrs = Vec::new();
    for _ in 0..3 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        fabric_addrs.push(l.local_addr().unwrap().to_string());
    }
    let seeds: Vec<(u64, String)> = (1..=3u64).zip(fabric_addrs).collect();
    let mut nodes = Vec::new();
    for (id, fabric) in &seeds {
        let (cert, key) = pki.leaf_files(&format!("node{id}"));
        let mut tls = TlsServerConfig::new(cert, key);
        tls.client_ca_file = Some(pki.ca_file());
        let mut member = ClusterMemberConfig::new(*id, fabric.clone(), seeds.clone());
        member.tls = Some(tls);
        let mut config = BrokerConfig::default();
        config.cluster = Some(member);
        let broker = Broker::new(config);
        let bound = broker.clone().bind("127.0.0.1:0").await.expect("bind");
        let addr = bound.local_addr();
        let shutdown = bound.shutdown_handle();
        tokio::spawn(bound.run());
        nodes.push((broker, addr, shutdown));
    }
    for (broker, _, _) in &nodes {
        assert!(
            broker.cluster_formed(Duration::from_secs(20)).await,
            "cluster formed over mutual TLS"
        );
    }
    let conn = ConnectionBuilder::new(format!("amqp://{}", nodes[0].1))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let p = s.create_producer("/quorum/secure").await.unwrap();
    p.send(Message::text("via fabric")).await.unwrap();
    conn.close().await.unwrap();
    let conn = ConnectionBuilder::new(format!("amqp://{}", nodes[2].1))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let mut c = s.create_consumer("/quorum/secure").await.unwrap();
    let d = tokio::time::timeout(Duration::from_secs(10), c.recv())
        .await
        .expect("delivery in time")
        .unwrap();
    assert_eq!(text_of(&d), "via fabric");
    c.accept(&d).await.unwrap();
    conn.close().await.unwrap();

    // A plain-TCP peer speaking fabric frames gets nowhere: the TLS
    // acceptor refuses it before any frame is read.
    let mut rogue = tokio::net::TcpStream::connect(&seeds[0].1).await.unwrap();
    let _ = rogue.write_all(&[0, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0, 0]).await;
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), rogue.read(&mut buf))
        .await
        .expect("closed promptly")
        .unwrap_or(0);
    assert!(n == 0 || buf[0] == 0x15, "plaintext fabric peer is refused");
    for (_, _, shutdown) in &nodes {
        shutdown.shutdown();
    }
}

/// Review H2: a fabric peer needs more than a certificate from the cluster
/// CA — it must name one of the seed hosts. A same-CA certificate for some
/// other name (an application cert, say) is refused after the handshake.
#[tokio::test(flavor = "multi_thread")]
async fn fabric_refuses_same_ca_certificates_that_name_no_seed() {
    let pki = Pki::new("cluster-ca");
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let fabric = l.local_addr().unwrap().to_string();
    drop(l);
    let (cert, key) = pki.leaf_files("node1");
    let mut tls = TlsServerConfig::new(cert, key);
    tls.client_ca_file = Some(pki.ca_file());
    let mut member = ClusterMemberConfig::new(1, fabric.clone(), vec![(1, fabric.clone())]);
    member.tls = Some(tls);
    let mut config = BrokerConfig::default();
    config.cluster = Some(member);
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    // A client certificate from the SAME CA, naming an application host.
    let (app_cert, app_key) = pki.leaf_for("app", &["app.example"]);
    use rustls::pki_types::pem::PemObject;
    let mut roots = rustls::RootCertStore::empty();
    for c in rustls::pki_types::CertificateDer::pem_slice_iter(pki.ca_pem.as_bytes()) {
        roots.add(c.unwrap()).unwrap();
    }
    let chain: Vec<_> = rustls::pki_types::CertificateDer::pem_slice_iter(app_cert.as_bytes())
        .map(Result::unwrap)
        .collect();
    let key = rustls::pki_types::PrivateKeyDer::from_pem_slice(app_key.as_bytes()).unwrap();
    let client = rustls::ClientConfig::builder_with_provider(Arc::new(
        rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .unwrap()
    .with_root_certificates(roots)
    .with_client_auth_cert(chain, key)
    .unwrap();
    let tcp = tokio::net::TcpStream::connect(&fabric).await.unwrap();
    let mut stream = tokio_rustls::TlsConnector::from(Arc::new(client))
        .connect("127.0.0.1".try_into().unwrap(), tcp)
        .await
        .expect("TLS itself succeeds: the chain is valid");
    let _ = stream
        .write_all(&[0, 0, 0, 8, 0, 2, 0, 0, 0, 0, 0, 0])
        .await;
    let mut buf = [0u8; 16];
    let n = tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf))
        .await
        .expect("closed promptly")
        .unwrap_or(0);
    assert_eq!(n, 0, "a non-member certificate is dropped, never served");
    shutdown.shutdown();
}

/// Review M5: the client CA bundle reloads like the certificate does — a
/// rotated CA is trusted without a restart.
#[tokio::test]
async fn client_ca_reloads_without_a_restart() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let next_ca = Pki::new("next-ca");
    let (ncert, nkey) = next_ca.leaf_files("client");
    let ca_path = pki.dir.path().join("clients-ca.crt");
    std::fs::write(&ca_path, &pki.ca_pem).unwrap();
    let mut tls = TlsServerConfig::new(cert, key);
    tls.client_ca_file = Some(ca_path.clone());
    tls.reload_interval = Duration::from_millis(100);
    let mut config = BrokerConfig::default();
    config.tls = Some(tls);
    config.tls_listen = Some("127.0.0.1:0".to_owned());
    let (addr, shutdown) = start(config).await;
    // A client of the next CA is refused…
    assert!(
        round_trip(addr, client_tls(&pki, Some((&ncert, &nkey))), "/queues/ca")
            .await
            .is_err()
    );
    // …until the CA bundle is rotated to include it.
    std::fs::write(&ca_path, format!("{}{}", pki.ca_pem, next_ca.ca_pem)).unwrap();
    let mut ok = false;
    for _ in 0..50 {
        tokio::time::sleep(Duration::from_millis(100)).await;
        if round_trip(addr, client_tls(&pki, Some((&ncert, &nkey))), "/queues/ca")
            .await
            .is_ok()
        {
            ok = true;
            break;
        }
    }
    assert!(ok, "the rotated client CA is trusted without a restart");
    shutdown.shutdown();
}

/// Review L9: TLS-only — no plain listener at all.
#[tokio::test]
async fn tls_only_mode_binds_no_plain_listener() {
    let pki = Pki::new("ca");
    let (cert, key) = pki.leaf_files("server");
    let mut config = BrokerConfig::default();
    config.tls = Some(TlsServerConfig::new(cert, key));
    config.tls_listen = Some("127.0.0.1:0".to_owned());
    let bound = Broker::new(config).bind("").await.expect("TLS-only bind");
    let addr = bound.local_addr();
    assert_eq!(
        bound.tls_local_addr(),
        Some(addr),
        "the only listener is TLS"
    );
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    round_trip(addr, client_tls(&pki, None), "/queues/only")
        .await
        .expect("served over TLS");
    // Without a TLS listener, an empty address is refused.
    assert!(Broker::new(BrokerConfig::default()).bind("").await.is_err());
    shutdown.shutdown();
}
