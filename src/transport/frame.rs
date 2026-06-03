//! The length-delimited AMQP frame codec and the buffered [`FramedTransport`].
//!
//! Encoding is single-pass: the 4-byte size is written as a placeholder, the
//! performative is serialized exactly once, and the size is backpatched — the
//! `fe2o3-amqp` "re-serialize to probe size" pattern is never used. Decoding is
//! zero-copy: the frame body is a [`Bytes`] slice of the read buffer.

use bytes::{Buf, BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::codec::{Decode, Encode};
use crate::error::{ConnectError, ErrorKind};
use crate::types::performatives::Performative;
use crate::types::sasl::SaslFrame;

use super::IoStream;

/// Frame type byte for an AMQP frame.
pub const FRAME_TYPE_AMQP: u8 = 0x00;
/// Frame type byte for a SASL frame.
pub const FRAME_TYPE_SASL: u8 = 0x01;
/// The fixed frame header length (size + doff + type + channel).
pub const FRAME_HEADER_LEN: usize = 8;

/// The decoded body of a frame.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum FrameBody {
    /// An AMQP performative with an optional trailing payload (transfer body).
    Amqp(Performative, Option<Bytes>),
    /// A SASL negotiation frame.
    Sasl(SaslFrame),
    /// An empty (heartbeat / keepalive) frame.
    Empty,
}

/// A decoded AMQP or SASL frame.
#[derive(Debug, Clone, PartialEq)]
pub struct Frame {
    /// The connection channel (0 for SASL frames).
    pub channel: u16,
    /// The frame body.
    pub body: FrameBody,
}

/// Encode one AMQP frame (single-pass, backpatched size).
pub fn encode_amqp_frame(
    buf: &mut BytesMut,
    channel: u16,
    performative: &Performative,
    payload: Option<&[u8]>,
) {
    let size_pos = buf.len();
    buf.put_u32(0); // size placeholder
    buf.put_u8(2); // doff = 2 words (8 bytes), no extended header
    buf.put_u8(FRAME_TYPE_AMQP);
    buf.put_u16(channel);
    performative.encode(buf); // serialized exactly once
    if let Some(p) = payload {
        buf.put_slice(p);
    }
    let size = (buf.len() - size_pos) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode one SASL frame.
pub fn encode_sasl_frame(buf: &mut BytesMut, frame: &SaslFrame) {
    let size_pos = buf.len();
    buf.put_u32(0);
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_SASL);
    buf.put_u16(0); // SASL frames ignore the channel field
    frame.encode(buf);
    let size = (buf.len() - size_pos) as u32;
    buf[size_pos..size_pos + 4].copy_from_slice(&size.to_be_bytes());
}

/// Encode an empty (heartbeat) frame on the given channel.
pub fn encode_empty_frame(buf: &mut BytesMut, channel: u16) {
    buf.put_u32(8); // size = header only
    buf.put_u8(2);
    buf.put_u8(FRAME_TYPE_AMQP);
    buf.put_u16(channel);
}

/// Maximum payload bytes that fit in one frame, given the negotiated
/// `max_frame_size` and the encoded performative size. Used by the sender to
/// split a large message into multi-frame transfers without re-encoding.
pub fn max_payload_for_frame(max_frame_size: usize, performative_size: usize) -> usize {
    max_frame_size.saturating_sub(FRAME_HEADER_LEN + performative_size)
}

fn proto_err(msg: impl Into<String>) -> ConnectError {
    ConnectError::msg(ErrorKind::ProtocolViolation, msg)
}

/// Try to decode a single frame from `src`, returning `Ok(None)` if more bytes
/// are needed. On success the consumed bytes are removed from `src`.
pub fn decode_frame(
    src: &mut BytesMut,
    max_frame_size: usize,
) -> Result<Option<Frame>, ConnectError> {
    if src.len() < FRAME_HEADER_LEN {
        return Ok(None);
    }
    let size = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if size < FRAME_HEADER_LEN {
        return Err(proto_err(format!("frame size {size} below header length")));
    }
    if size > max_frame_size {
        return Err(proto_err(format!(
            "frame size {size} exceeds max-frame-size {max_frame_size}"
        )));
    }
    if src.len() < size {
        return Ok(None); // incomplete
    }

    let mut frame = src.split_to(size).freeze();
    let _size = frame.get_u32();
    let doff = frame.get_u8();
    let ftype = frame.get_u8();
    let channel = frame.get_u16();

    let header_len = doff as usize * 4;
    if header_len < FRAME_HEADER_LEN || header_len > size {
        return Err(proto_err(format!("invalid data-offset {doff}")));
    }
    let extended = header_len - FRAME_HEADER_LEN;
    if frame.len() < extended {
        return Err(proto_err("truncated extended header"));
    }
    frame.advance(extended);

    let body = match ftype {
        FRAME_TYPE_AMQP => decode_amqp_body(frame)?,
        FRAME_TYPE_SASL => {
            let mut b = frame;
            FrameBody::Sasl(SaslFrame::decode(&mut b)?)
        }
        other => return Err(proto_err(format!("unknown frame type {other:#04x}"))),
    };
    Ok(Some(Frame { channel, body }))
}

fn decode_amqp_body(mut body: Bytes) -> Result<FrameBody, ConnectError> {
    if body.is_empty() {
        return Ok(FrameBody::Empty);
    }
    let performative = Performative::decode(&mut body)?;
    let payload = if body.is_empty() { None } else { Some(body) };
    Ok(FrameBody::Amqp(performative, payload))
}

/// A stream wrapper that reads/writes whole frames with buffered, batched IO.
#[derive(Debug)]
pub struct FramedTransport<S> {
    stream: S,
    read_buf: BytesMut,
    write_buf: BytesMut,
    max_frame_size: usize,
    last_read_size: usize,
}

impl<S: IoStream> FramedTransport<S> {
    /// Wrap `stream`, accepting frames up to `max_frame_size` bytes.
    pub fn new(stream: S, max_frame_size: u32) -> Self {
        let cap = (max_frame_size as usize).clamp(4096, 1 << 20);
        FramedTransport {
            stream,
            read_buf: BytesMut::with_capacity(cap),
            write_buf: BytesMut::with_capacity(cap),
            max_frame_size: max_frame_size as usize,
            last_read_size: 0,
        }
    }

    /// The byte length of the most recently decoded frame (for metrics).
    pub fn last_read_size(&self) -> usize {
        self.last_read_size
    }

    /// Update the negotiated maximum frame size (after `open`).
    pub fn set_max_frame_size(&mut self, max_frame_size: u32) {
        self.max_frame_size = max_frame_size as usize;
    }

    /// Borrow the underlying stream (e.g. for the protocol-header handshake).
    pub fn stream_mut(&mut self) -> &mut S {
        &mut self.stream
    }

    /// Unwrap the underlying stream.
    pub fn into_inner(self) -> S {
        self.stream
    }

    /// Read and decode the next frame. Cancel-safe: partial reads accumulate in
    /// the internal buffer, so a cancelled call loses no data.
    pub async fn read_frame(&mut self) -> Result<Frame, ConnectError> {
        loop {
            let before = self.read_buf.len();
            if let Some(frame) = decode_frame(&mut self.read_buf, self.max_frame_size)? {
                self.last_read_size = before - self.read_buf.len();
                return Ok(frame);
            }
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(if self.read_buf.is_empty() {
                    ConnectError::msg(ErrorKind::PeerClosed, "connection closed by peer")
                } else {
                    proto_err("connection closed mid-frame")
                });
            }
        }
    }

    /// Queue an AMQP frame in the write buffer (does not flush).
    pub fn queue_amqp(&mut self, channel: u16, performative: &Performative, payload: Option<&[u8]>) {
        encode_amqp_frame(&mut self.write_buf, channel, performative, payload);
    }

    /// Queue a SASL frame in the write buffer.
    pub fn queue_sasl(&mut self, frame: &SaslFrame) {
        encode_sasl_frame(&mut self.write_buf, frame);
    }

    /// Queue an empty heartbeat frame.
    pub fn queue_empty(&mut self, channel: u16) {
        encode_empty_frame(&mut self.write_buf, channel);
    }

    /// Number of queued, unflushed bytes.
    pub fn pending_bytes(&self) -> usize {
        self.write_buf.len()
    }

    /// Flush all queued frames in a single write + flush.
    pub async fn flush(&mut self) -> Result<(), ConnectError> {
        if self.write_buf.is_empty() {
            return Ok(());
        }
        self.stream.write_all(&self.write_buf).await?;
        self.write_buf.clear();
        self.stream.flush().await?;
        Ok(())
    }

    /// Convenience: queue one AMQP frame and flush immediately.
    pub async fn send_amqp(
        &mut self,
        channel: u16,
        performative: &Performative,
        payload: Option<&[u8]>,
    ) -> Result<(), ConnectError> {
        self.queue_amqp(channel, performative, payload);
        self.flush().await
    }

    /// Convenience: queue one SASL frame and flush immediately.
    pub async fn send_sasl(&mut self, frame: &SaslFrame) -> Result<(), ConnectError> {
        self.queue_sasl(frame);
        self.flush().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::performatives::{Close, Open};

    #[test]
    fn amqp_frame_round_trip() {
        let mut buf = BytesMut::new();
        let perf = Performative::Open(Open::new("c1"));
        encode_amqp_frame(&mut buf, 0, &perf, None);
        // header size field equals the buffer length
        assert_eq!(u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize, buf.len());

        let frame = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
        assert_eq!(frame.channel, 0);
        assert_eq!(frame.body, FrameBody::Amqp(perf, None));
        assert!(buf.is_empty());
    }

    #[test]
    fn transfer_payload_preserved() {
        use crate::types::performatives::Transfer;
        let mut buf = BytesMut::new();
        let perf = Performative::Transfer(Transfer {
            handle: 1,
            delivery_id: Some(7),
            ..Default::default()
        });
        let payload = b"the message body bytes";
        encode_amqp_frame(&mut buf, 3, &perf, Some(payload));
        let frame = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
        assert_eq!(frame.channel, 3);
        match frame.body {
            FrameBody::Amqp(p, Some(body)) => {
                assert_eq!(p, perf);
                assert_eq!(&body[..], payload);
            }
            other => panic!("unexpected {other:?}"),
        }
    }

    #[test]
    fn empty_frame_is_heartbeat() {
        let mut buf = BytesMut::new();
        encode_empty_frame(&mut buf, 0);
        assert_eq!(buf.len(), 8);
        let frame = decode_frame(&mut buf, 1 << 20).unwrap().unwrap();
        assert_eq!(frame.body, FrameBody::Empty);
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut full = BytesMut::new();
        encode_amqp_frame(&mut full, 0, &Performative::Close(Close::default()), None);
        let mut partial = full.clone();
        partial.truncate(full.len() - 1);
        assert!(decode_frame(&mut partial, 1 << 20).unwrap().is_none());
        // and the whole thing decodes
        assert!(decode_frame(&mut full, 1 << 20).unwrap().is_some());
    }

    #[test]
    fn oversize_frame_rejected() {
        let mut buf = BytesMut::new();
        encode_amqp_frame(&mut buf, 0, &Performative::Open(Open::new("x")), None);
        assert!(decode_frame(&mut buf, 4).is_err());
    }

    #[test]
    fn malformed_frames_never_panic() {
        use crate::types::performatives::Transfer;
        // A valid frame, then every truncation and single-byte corruption of it,
        // must decode to Ok/Err — never panic (fault-injection robustness).
        let mut full = BytesMut::new();
        encode_amqp_frame(
            &mut full,
            0,
            &Performative::Transfer(Transfer {
                handle: 1,
                delivery_id: Some(1),
                delivery_tag: Some(bytes::Bytes::from_static(b"t")),
                ..Default::default()
            }),
            Some(b"a multi-byte payload"),
        );
        let full = full.freeze();

        for cut in 0..=full.len() {
            let mut buf = BytesMut::from(&full[..cut]);
            let _ = decode_frame(&mut buf, 1 << 20);
        }
        for i in 0..full.len() {
            let mut v = full.to_vec();
            v[i] ^= 0xff;
            let mut buf = BytesMut::from(&v[..]);
            let _ = decode_frame(&mut buf, 1 << 20);
        }
    }

    #[tokio::test]
    async fn framed_transport_over_duplex() {
        let (client, server) = tokio::io::duplex(4096);
        let mut ct = FramedTransport::new(client, 1 << 16);
        let mut st = FramedTransport::new(server, 1 << 16);

        let open = Performative::Open(Open::new("peer-a"));
        ct.send_amqp(0, &open, None).await.unwrap();

        let frame = st.read_frame().await.unwrap();
        assert_eq!(frame.body, FrameBody::Amqp(open, None));

        // batch two frames, flush once
        st.queue_amqp(0, &Performative::Close(Close::default()), None);
        st.queue_empty(0);
        assert!(st.pending_bytes() > 0);
        st.flush().await.unwrap();

        assert!(matches!(
            ct.read_frame().await.unwrap().body,
            FrameBody::Amqp(Performative::Close(_), None)
        ));
        assert_eq!(ct.read_frame().await.unwrap().body, FrameBody::Empty);
    }
}
