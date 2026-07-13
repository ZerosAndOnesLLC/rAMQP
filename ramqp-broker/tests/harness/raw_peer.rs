//! Hand-driven raw-frame AMQP peer: negotiate the header + `open`, then send
//! arbitrary (including spec-illegal) performatives and read the broker's
//! replies. This is the surface conformance tests use to assert the broker's
//! exact wire reaction to protocol violations.

use bytes::BytesMut;
use tokio::io::AsyncReadExt;

use ramqp_core::transport::frame::{Frame, FrameBody, FramedTransport, decode_frame};
use ramqp_core::transport::header::ProtocolHeader;
use ramqp_core::types::definitions::Error as WireError;
use ramqp_core::types::performatives::{Open, Performative};

/// What a peer observed while waiting for the connection to close.
#[derive(Debug)]
pub enum CloseOutcome {
    /// A `close` performative arrived carrying this error condition.
    Error(WireError),
    /// A `close` performative arrived with no error (graceful close).
    Clean,
    /// The socket dropped with no `close` performative at all.
    Dropped,
}

/// A hand-driven AMQP peer over a real TCP socket to the broker.
pub struct RawPeer {
    transport: FramedTransport<tokio::net::TcpStream>,
}

impl RawPeer {
    /// Connect and negotiate only the protocol header (no `open` yet).
    pub async fn connect(addr: std::net::SocketAddr, max_frame: u32) -> Self {
        let mut stream = tokio::net::TcpStream::connect(addr).await.expect("connect");
        ProtocolHeader::AMQP
            .negotiate(&mut stream)
            .await
            .expect("header negotiation");
        RawPeer {
            transport: FramedTransport::new(stream, max_frame),
        }
    }

    /// Connect, negotiate the header, send `open`, and consume the broker's
    /// `open` — leaving an established connection ready to be driven or abused.
    pub async fn open(addr: std::net::SocketAddr, container_id: &str, max_frame: u32) -> Self {
        let mut peer = Self::connect(addr, max_frame).await;
        let mut open = Open::new(container_id);
        open.max_frame_size = max_frame;
        peer.send(0, Performative::Open(open)).await;
        peer.expect_open().await;
        peer
    }

    /// Send one performative on `channel`.
    pub async fn send(&mut self, channel: u16, perf: Performative) {
        self.transport
            .send_amqp(channel, &perf, None)
            .await
            .expect("send performative");
    }

    /// Read the next frame (panics on transport error).
    pub async fn read(&mut self) -> Frame {
        self.transport.read_frame().await.expect("read frame")
    }

    /// Consume frames until the broker's `open` arrives (skipping keep-alives).
    pub async fn expect_open(&mut self) {
        loop {
            match self.read().await.body {
                FrameBody::Amqp(Performative::Open(_), _) => return,
                FrameBody::Empty => continue,
                other => panic!("expected broker open, got {other:?}"),
            }
        }
    }

    /// Read until a `close` arrives (or the socket drops), reporting what
    /// happened so a test can assert the exact error condition.
    pub async fn wait_for_close(&mut self) -> CloseOutcome {
        loop {
            match self.transport.read_frame().await {
                Ok(frame) => match frame.body {
                    FrameBody::Amqp(Performative::Close(c), _) => {
                        return match c.error {
                            Some(e) => CloseOutcome::Error(e),
                            None => CloseOutcome::Clean,
                        };
                    }
                    _ => continue,
                },
                Err(_) => return CloseOutcome::Dropped,
            }
        }
    }
}

/// Encode one AMQP frame to its exact wire bytes (via a scratch duplex
/// transport). Used by byte-level framing tests that must hand-craft or mutate
/// the wire form before writing it to a raw socket.
pub async fn encode_frame(channel: u16, perf: &Performative) -> Vec<u8> {
    let (a, mut b) = tokio::io::duplex(1 << 16);
    let mut t = FramedTransport::new(a, 1 << 16);
    t.send_amqp(channel, perf, None)
        .await
        .expect("encode frame");
    let mut buf = vec![0u8; 1 << 16];
    let n = b.read(&mut buf).await.expect("read encoded frame");
    buf.truncate(n);
    buf
}

/// Read one whole AMQP frame from a raw stream, decoding with a generous limit.
pub async fn read_raw_frame(stream: &mut tokio::net::TcpStream, buf: &mut BytesMut) -> Frame {
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
