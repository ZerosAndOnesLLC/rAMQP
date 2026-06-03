//! Live TLS (`amqps://`) interop. Requires the `rustls` feature and a
//! TLS-enabled broker. Gated on env so a featureless CI stays green:
//!
//! ```sh
//! RAMQP_TLS_BROKER_URL=amqps://guest:guest@localhost:5671 \
//! RAMQP_TLS_CA_PEM=tests/docker/certs/ca.crt \
//! RAMQP_TLS_ADDRESS=/queues/ramqp_it \
//!   cargo test --features rustls --test tls -- --test-threads=1
//! ```
#![cfg(feature = "rustls")]

use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};

fn url() -> Option<String> {
    std::env::var("RAMQP_TLS_BROKER_URL").ok()
}

fn address() -> String {
    std::env::var("RAMQP_TLS_ADDRESS").unwrap_or_else(|_| "/queues/ramqp_it".into())
}

/// Connect over `amqps` trusting *only* the broker's private CA (webpki roots
/// disabled), proving the custom-trust-anchor path end to end.
#[tokio::test]
async fn amqps_roundtrip_with_private_ca() {
    let Some(url) = url() else {
        eprintln!("skipping tls test: set RAMQP_TLS_BROKER_URL");
        return;
    };
    let ca_path = std::env::var("RAMQP_TLS_CA_PEM")
        .expect("set RAMQP_TLS_CA_PEM to the broker CA pem path");
    let ca = std::fs::read(ca_path).expect("read CA pem");

    let conn = ConnectionBuilder::new(&url)
        .add_root_ca_pem(ca)
        .webpki_roots(false)
        .connect()
        .await
        .expect("amqps connect with private CA");

    let address = address();
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(&address).await.expect("producer");
    let outcome = producer
        .send(Message::text("tls-hello"))
        .await
        .expect("send over tls");
    assert!(matches!(outcome, DeliveryState::Accepted(_)), "got {outcome:?}");

    let mut consumer = session.create_consumer(&address).await.expect("consumer");
    let d = consumer.recv().await.expect("recv over tls");
    assert_eq!(d.message().unwrap().body, Message::text("tls-hello").body);
    consumer.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
}

/// The verification-bypass path (`danger_accept_invalid_certs`) connects without
/// any CA configured — exercising the custom no-verify verifier.
#[tokio::test]
async fn amqps_roundtrip_insecure_bypass() {
    let Some(url) = url() else {
        eprintln!("skipping tls test: set RAMQP_TLS_BROKER_URL");
        return;
    };
    let conn = ConnectionBuilder::new(&url)
        .danger_accept_invalid_certs(true)
        .connect()
        .await
        .expect("amqps connect with verification bypass");

    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(&address()).await.expect("producer");
    let outcome = producer.send(Message::text("tls-insecure")).await.expect("send");
    assert!(matches!(outcome, DeliveryState::Accepted(_)), "got {outcome:?}");
    // clean up
    let mut c = session.create_consumer(&address()).await.expect("consumer");
    let d = c.recv().await.expect("recv");
    c.accept(&d).await.ok();
    conn.close().await.expect("close");
}
