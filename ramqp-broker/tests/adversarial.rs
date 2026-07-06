//! Adversarial / raw-socket tests: a hand-driven peer that violates the
//! protocol after a valid handshake, asserting the broker answers with a
//! `close{error}` carrying an AMQP condition rather than a bare TCP reset.

use tokio::io::AsyncReadExt;

use ramqp_broker::{Broker, BrokerConfig};
use ramqp_core::config::ConnectionConfig;
use ramqp_core::transport::frame::{FrameBody, FramedTransport};
use ramqp_core::transport::header::ProtocolHeader;
use ramqp_core::types::performatives::{Open, Performative};

async fn start() -> (std::net::SocketAddr, ramqp_broker::ShutdownHandle) {
    let bound = Broker::new(BrokerConfig::default())
        .bind("127.0.0.1:0")
        .await
        .expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

/// Drive the header + `open` exchange by hand (bare AMQP — the default broker
/// offers it), leaving an established connection ready to misbehave on.
async fn handshake(addr: std::net::SocketAddr) -> FramedTransport<tokio::net::TcpStream> {
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    ProtocolHeader::AMQP
        .negotiate(&mut stream)
        .await
        .expect("header negotiation");
    let mut transport = FramedTransport::new(stream, 65536);
    let mut open = Open::new("adversary");
    open.max_frame_size = 65536;
    transport
        .send_amqp(0, &Performative::Open(open), None)
        .await
        .expect("send open");
    // Consume the broker's open (skipping any empty keep-alive frames).
    loop {
        match transport.read_frame().await.expect("read broker open").body {
            FrameBody::Amqp(Performative::Open(_), _) => break,
            FrameBody::Empty => continue,
            other => panic!("expected broker open, got {other:?}"),
        }
    }
    transport
}

/// Read frames until a `close` arrives; returns whether it carried an error.
async fn wait_for_close(transport: &mut FramedTransport<tokio::net::TcpStream>) -> Option<bool> {
    loop {
        match transport.read_frame().await {
            Ok(frame) => match frame.body {
                FrameBody::Amqp(Performative::Close(c), _) => return Some(c.error.is_some()),
                _ => continue,
            },
            Err(_) => return None, // socket closed with no close frame
        }
    }
}

/// A duplicate `open` is a connection-level protocol violation. The broker must
/// answer with `close{error}` (framing-error) before the socket drops, not a
/// silent reset.
#[tokio::test]
async fn duplicate_open_gets_close_with_error() {
    let (addr, shutdown) = start().await;
    let mut transport = handshake(addr).await;

    // Violation: a second open on an already-open connection.
    transport
        .send_amqp(0, &Performative::Open(Open::new("dup")), None)
        .await
        .expect("send duplicate open");

    match wait_for_close(&mut transport).await {
        Some(has_error) => assert!(has_error, "close must carry an error condition"),
        None => panic!("broker dropped the socket without a close performative"),
    }

    shutdown.shutdown();
}

/// Slow-loris guard: a peer that connects then sends nothing must be dropped
/// once the inbound-handshake timeout fires, rather than pinning a task
/// forever. We observe the drop as EOF on our end.
#[tokio::test]
async fn stalled_handshake_is_timed_out() {
    let config = BrokerConfig {
        connection: ConnectionConfig {
            connect_timeout: Some(std::time::Duration::from_millis(200)),
            ..Default::default()
        },
        ..Default::default()
    };
    let bound = Broker::new(config).bind("127.0.0.1:0").await.expect("bind");
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());

    // Connect but never send the protocol header (the slow-loris).
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    let mut buf = [0u8; 1];
    // The broker times out the handshake and drops the socket → EOF (Ok(0)).
    let observed = tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buf))
        .await
        .expect("broker must drop the stalled handshake well before 2s");
    assert!(
        matches!(observed, Ok(0)),
        "expected EOF from the timed-out handshake, got {observed:?}"
    );

    shutdown.shutdown();
}

/// A frame on a channel that was never begun is a protocol violation; it, too,
/// earns a `close{error}` rather than a bare disconnect.
#[tokio::test]
async fn frame_on_unmapped_channel_gets_close_with_error() {
    let (addr, shutdown) = start().await;
    let mut transport = handshake(addr).await;

    // Violation: a detach naming a link on a session (channel 7) we never
    // began. Unlike End (which tolerates an end/end race), a link frame on an
    // unmapped channel is a hard framing error.
    use ramqp_core::types::performatives::Detach;
    transport
        .send_amqp(
            7,
            &Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
            None,
        )
        .await
        .expect("send detach on unmapped channel");

    match wait_for_close(&mut transport).await {
        Some(has_error) => assert!(has_error, "close must carry an error condition"),
        None => panic!("broker dropped the socket without a close performative"),
    }

    shutdown.shutdown();
}
