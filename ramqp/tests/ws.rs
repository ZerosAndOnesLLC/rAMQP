//! Live AMQP-over-WebSocket (`ws://`) interop. Requires the `ws` feature.
//!
//! Most brokers (including RabbitMQ) do not expose AMQP 1.0 over WebSocket
//! directly, so this test stands up an in-process WebSocket→TCP bridge in front
//! of a plain-AMQP broker: the client speaks `ws://` to the bridge, the bridge
//! relays the AMQP byte stream to the broker's TCP port. This exercises the real
//! client WebSocket transport (handshake, binary-message framing, reassembly)
//! end to end against a live broker.
//!
//! ```sh
//! RAMQP_WS_BROKER_TCP=127.0.0.1:5672 \
//! RAMQP_WS_ADDRESS=/queues/ramqp_it \
//!   cargo test --features ws --test ws -- --test-threads=1
//! ```
#![cfg(feature = "ws")]

use ramqp::transport::ws::WsByteStream;
use ramqp::types::messaging::DeliveryState;
use ramqp::{ConnectionBuilder, Message};
use tokio::net::{TcpListener, TcpStream};

/// Accept WebSocket connections and relay their AMQP byte stream to `broker`
/// (a `host:port` plain-AMQP endpoint). Returns the `ws://` URL to dial.
// The subprotocol-select callback's `Result<_, ErrorResponse>` error type is
// dictated by tungstenite's handshake callback trait, not ours.
#[allow(clippy::result_large_err)]
async fn spawn_ws_bridge(broker: String) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    tokio::spawn(async move {
        while let Ok((sock, _)) = listener.accept().await {
            let broker = broker.clone();
            tokio::spawn(async move {
                use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
                use tokio_tungstenite::tungstenite::http::{HeaderValue, header};
                // A compliant AMQP-WS server must select the `amqp` subprotocol;
                // the client rejects the handshake otherwise.
                let select_amqp = |_req: &Request, mut resp: Response| {
                    resp.headers_mut().insert(
                        header::SEC_WEBSOCKET_PROTOCOL,
                        HeaderValue::from_static("amqp"),
                    );
                    Ok(resp)
                };
                let ws = match tokio_tungstenite::accept_hdr_async(sock, select_amqp).await {
                    Ok(ws) => ws,
                    Err(_) => return,
                };
                let mut client = WsByteStream::new(ws);
                let Ok(mut upstream) = TcpStream::connect(&broker).await else {
                    return;
                };
                let _ = tokio::io::copy_bidirectional(&mut client, &mut upstream).await;
            });
        }
    });
    format!("ws://127.0.0.1:{port}/")
}

#[tokio::test]
async fn ws_roundtrip_via_bridge() {
    let Ok(broker_tcp) = std::env::var("RAMQP_WS_BROKER_TCP") else {
        eprintln!("skipping ws test: set RAMQP_WS_BROKER_TCP (host:port of a plain-AMQP broker)");
        return;
    };
    let address = std::env::var("RAMQP_WS_ADDRESS").unwrap_or_else(|_| "/queues/ramqp_it".into());
    let user = std::env::var("RAMQP_WS_USER").unwrap_or_else(|_| "guest".into());
    let pass = std::env::var("RAMQP_WS_PASS").unwrap_or_else(|_| "guest".into());

    let bridge = spawn_ws_bridge(broker_tcp).await;
    // Splice credentials into the ws URL so SASL PLAIN runs over the WS stream.
    let url = bridge.replace("ws://", &format!("ws://{user}:{pass}@"));

    let conn = ConnectionBuilder::new(&url)
        .connect()
        .await
        .expect("ws connect");
    let session = conn.begin_session().await.expect("session");
    let producer = session.create_producer(&address).await.expect("producer");
    let outcome = producer
        .send(Message::text("ws-hello"))
        .await
        .expect("send over websocket");
    assert!(
        matches!(outcome, DeliveryState::Accepted(_)),
        "got {outcome:?}"
    );

    let mut consumer = session.create_consumer(&address).await.expect("consumer");
    let d = consumer.recv().await.expect("recv over websocket");
    assert_eq!(d.message().unwrap().body, Message::text("ws-hello").body);
    consumer.accept(&d).await.expect("accept");

    conn.close().await.expect("close");
}
