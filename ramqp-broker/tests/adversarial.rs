//! Adversarial / raw-socket tests: a hand-driven peer that violates the
//! protocol after a valid handshake, asserting the broker answers with a
//! `close{error}` carrying an AMQP condition rather than a bare TCP reset.

use bytes::BytesMut;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use ramqp_broker::{Broker, BrokerConfig};
use ramqp_core::config::ConnectionConfig;
use ramqp_core::transport::frame::{Frame, FrameBody, FramedTransport, decode_frame};
use ramqp_core::transport::header::ProtocolHeader;
use ramqp_core::types::performatives::{Begin, Open, Performative};

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

/// Encode one AMQP frame to its exact wire bytes (via a scratch transport).
async fn encode_frame(channel: u16, performative: &Performative) -> Vec<u8> {
    let (a, mut b) = tokio::io::duplex(1 << 16);
    let mut t = FramedTransport::new(a, 1 << 16);
    t.send_amqp(channel, performative, None)
        .await
        .expect("encode frame");
    let mut buf = vec![0u8; 1 << 16];
    let n = b.read(&mut buf).await.expect("read encoded frame");
    buf.truncate(n);
    buf
}

/// Read one whole AMQP frame from a raw stream, decoding with a generous limit.
async fn read_raw_frame(stream: &mut tokio::net::TcpStream, buf: &mut BytesMut) -> Frame {
    loop {
        if let Some(frame) = decode_frame(buf, 1 << 20).expect("decode") {
            return frame;
        }
        let mut chunk = [0u8; 4096];
        let n = stream.read(&mut chunk).await.expect("read");
        assert!(n > 0, "stream closed before a full frame");
        buf.extend_from_slice(&chunk[..n]);
    }
}

/// max-frame-size is directional (spec §2.7.1): a client may advertise a small
/// receive limit yet legally send frames as large as the BROKER advertised.
/// The broker's inbound decode must honor its own advertised max, not the
/// negotiated min — otherwise it kills a spec-legal oversized frame.
#[tokio::test]
async fn oversized_inbound_frame_from_small_advertiser_is_accepted() {
    // Broker advertises the default (128 KiB); the raw client advertises 4 KiB.
    let (addr, shutdown) = start().await;
    let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
    ProtocolHeader::AMQP
        .negotiate(&mut stream)
        .await
        .expect("header");

    let mut small_open = Open::new("small-advertiser");
    small_open.max_frame_size = 4096;
    stream
        .write_all(&encode_frame(0, &Performative::Open(small_open)).await)
        .await
        .expect("send open");

    let mut buf = BytesMut::new();
    // Consume the broker's open.
    loop {
        match read_raw_frame(&mut stream, &mut buf).await.body {
            FrameBody::Amqp(Performative::Open(_), _) => break,
            FrameBody::Empty => continue,
            other => panic!("expected broker open, got {other:?}"),
        }
    }

    // Build an 8 KiB Begin frame (> the client's advertised 4 KiB, <= the
    // broker's 128 KiB): pad the encoded Begin and fix its size header. The
    // padding decodes as ignored trailing payload.
    let begin = Begin {
        next_outgoing_id: 0,
        incoming_window: 8,
        outgoing_window: 8,
        handle_max: 16,
        ..Default::default()
    };
    let mut big = encode_frame(0, &Performative::Begin(begin)).await;
    assert!(big.len() < 8192);
    big.resize(8192, 0);
    big[0..4].copy_from_slice(&(8192u32).to_be_bytes());
    stream.write_all(&big).await.expect("send big begin");

    // The broker must ACCEPT the oversized frame — a Begin response, not a
    // close{error: framing-error} from an over-strict inbound limit.
    match read_raw_frame(&mut stream, &mut buf).await.body {
        FrameBody::Amqp(Performative::Begin(_), _) => {}
        FrameBody::Amqp(Performative::Close(c), _) => {
            panic!(
                "broker rejected a spec-legal oversized frame: {:?}",
                c.error
            )
        }
        other => panic!("expected a begin response, got {other:?}"),
    }

    shutdown.shutdown();
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
