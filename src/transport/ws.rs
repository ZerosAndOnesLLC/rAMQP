//! AMQP-over-WebSocket transport (WP-1.3): the RFC 6455 WebSocket binding.
//!
//! AMQP frames travel inside **binary** WebSocket messages (subprotocol `amqp`,
//! per the OASIS *AMQP WebSocket Binding* specification). The rest of the crate
//! works on a plain `AsyncRead + AsyncWrite` byte stream, so this module adapts
//! a [`tokio_tungstenite::WebSocketStream`] into exactly that: [`WsByteStream`]
//! flattens the message-framed WebSocket into a continuous byte stream, hiding
//! ping/pong control traffic and message boundaries from the framing codec.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{SinkExt, StreamExt, ready};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::WebSocketStream;
use tokio_tungstenite::tungstenite::Error as WsError;
use tokio_tungstenite::tungstenite::Message;

use crate::error::{ConnectError, ErrorKind};

/// Maximum reassembled inbound WebSocket message size (defense against a hostile
/// server; the AMQP layer enforces its own, smaller, negotiated frame size).
const MAX_WS_MESSAGE_SIZE: usize = 64 << 20; // 64 MiB
/// Maximum single inbound WebSocket frame size.
const MAX_WS_FRAME_SIZE: usize = 16 << 20; // 16 MiB

/// A plain byte-stream adapter over a [`WebSocketStream`].
///
/// Implements [`AsyncRead`] and [`AsyncWrite`] by carrying AMQP bytes inside
/// binary WebSocket messages. Reads transparently span message boundaries
/// (leftover bytes from a partially consumed message are buffered), and writes
/// emit one binary message per [`poll_write`](AsyncWrite::poll_write) call.
/// Incoming ping/pong/text frames are ignored; a close frame (or end of stream)
/// surfaces as end-of-file.
pub struct WsByteStream<S> {
    /// The underlying WebSocket message stream.
    inner: WebSocketStream<S>,
    /// Bytes from a binary message not yet handed to a `poll_read` caller.
    leftover: bytes::Bytes,
    /// `true` once a close frame or end-of-stream has been observed.
    eof: bool,
}

impl<S> std::fmt::Debug for WsByteStream<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WsByteStream")
            .field("leftover", &self.leftover.len())
            .field("eof", &self.eof)
            .finish_non_exhaustive()
    }
}

impl<S> WsByteStream<S> {
    /// Wrap an already-established [`WebSocketStream`] as a byte stream.
    pub fn new(inner: WebSocketStream<S>) -> Self {
        Self {
            inner,
            leftover: bytes::Bytes::new(),
            eof: false,
        }
    }

    /// Borrow the underlying [`WebSocketStream`].
    pub fn get_ref(&self) -> &WebSocketStream<S> {
        &self.inner
    }

    /// Mutably borrow the underlying [`WebSocketStream`].
    pub fn get_mut(&mut self) -> &mut WebSocketStream<S> {
        &mut self.inner
    }

    /// Consume the adapter, returning the underlying [`WebSocketStream`].
    pub fn into_inner(self) -> WebSocketStream<S> {
        self.inner
    }

    /// Copy as many leftover bytes as fit into `buf`, retaining the remainder.
    /// Returns `true` if any bytes were written (so the read can complete).
    fn drain_leftover(&mut self, buf: &mut ReadBuf<'_>) -> bool {
        if self.leftover.is_empty() {
            return false;
        }
        let n = self.leftover.len().min(buf.remaining());
        buf.put_slice(&self.leftover[..n]);
        self.leftover = self.leftover.slice(n..);
        true
    }
}

/// Translate a tungstenite error into a classified [`io::Error`].
fn ws_to_io(e: WsError) -> io::Error {
    match e {
        WsError::Io(io) => io,
        other => io::Error::other(other),
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        // `WebSocketStream<S>: Unpin` (S: Unpin), so we can project by ref.
        let this = self.get_mut();

        // 1. Serve any buffered bytes from a prior binary message first.
        if this.drain_leftover(buf) {
            return Poll::Ready(Ok(()));
        }
        if this.eof {
            return Poll::Ready(Ok(()));
        }

        // 2. Pull WebSocket messages until we get data (or hit EOF / pending).
        loop {
            match ready!(this.inner.poll_next_unpin(cx)) {
                Some(Ok(Message::Binary(data))) => {
                    if data.is_empty() {
                        // An empty binary frame carries no AMQP bytes; keep going.
                        continue;
                    }
                    this.leftover = bytes::Bytes::from(data);
                    this.drain_leftover(buf);
                    return Poll::Ready(Ok(()));
                }
                // Control / text frames carry no AMQP payload: skip them.
                Some(Ok(Message::Ping(_)))
                | Some(Ok(Message::Pong(_)))
                | Some(Ok(Message::Text(_)))
                | Some(Ok(Message::Frame(_))) => continue,
                // A close frame, or the end of the stream, is end-of-file.
                Some(Ok(Message::Close(_))) | None => {
                    this.eof = true;
                    return Poll::Ready(Ok(()));
                }
                Some(Err(e)) => return Poll::Ready(Err(ws_to_io(e))),
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        let this = self.get_mut();
        // Wait for the sink to accept an item, then enqueue one binary message.
        ready!(this.inner.poll_ready_unpin(cx)).map_err(ws_to_io)?;
        // tungstenite 0.24 `Message::Binary` takes `Vec<u8>` (later versions
        // switched to `bytes::Bytes`), so copy the slice into an owned buffer.
        let msg = Message::Binary(buf.to_vec());
        match this.inner.start_send_unpin(msg) {
            Ok(()) => Poll::Ready(Ok(buf.len())),
            Err(e) => Poll::Ready(Err(ws_to_io(e))),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.inner.poll_flush_unpin(cx).map_err(ws_to_io)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        this.inner.poll_close_unpin(cx).map_err(ws_to_io)
    }
}

/// Perform the WebSocket client handshake over an established byte stream and
/// return a byte-stream adapter ready for AMQP framing.
///
/// The handshake requests the `amqp` subprotocol (`Sec-WebSocket-Protocol`),
/// as required by the AMQP WebSocket binding. `stream` is an already-connected
/// transport (TCP or TLS); `url` is the `ws://` / `wss://` endpoint.
pub async fn connect_ws<S>(stream: S, url: &str) -> Result<WsByteStream<S>, ConnectError>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    use http::Uri;
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;

    // Parse the endpoint and build a base handshake request: this fills in the
    // mandatory RFC 6455 headers (Host, Upgrade, Connection, Sec-WebSocket-Key,
    // Sec-WebSocket-Version).
    let uri: Uri = url.parse().map_err(|e: http::uri::InvalidUri| {
        ConnectError::msg(
            ErrorKind::ProtocolViolation,
            format!("invalid ws url: {url}"),
        )
        .with_source(e)
    })?;
    let mut request = uri.into_client_request().map_err(|e| {
        ConnectError::msg(ErrorKind::ProtocolViolation, "invalid websocket request")
            .with_source(ws_to_io(e))
    })?;

    // Advertise the AMQP WebSocket subprotocol.
    request.headers_mut().insert(
        http::header::SEC_WEBSOCKET_PROTOCOL,
        http::HeaderValue::from_static("amqp"),
    );

    // Bound inbound WebSocket message/frame sizes so a hostile server cannot
    // drive unbounded buffering at the transport layer (the AMQP layer separately
    // enforces the negotiated max-frame-size on decode).
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(MAX_WS_MESSAGE_SIZE),
        max_frame_size: Some(MAX_WS_FRAME_SIZE),
        ..Default::default()
    };

    let (ws, response) = tokio_tungstenite::client_async_with_config(request, stream, Some(config))
        .await
        .map_err(|e| match e {
            WsError::Io(io) => ConnectError::new(ErrorKind::Io).with_source(io),
            other => ConnectError::msg(ErrorKind::ProtocolViolation, "websocket handshake failed")
                .with_source(other),
        })?;

    // Per RFC 6455 §4.1 and the AMQP WebSocket Binding, the server MUST select the
    // `amqp` subprotocol. If it negotiated something else (or nothing), the byte
    // stream is not AMQP — reject it instead of feeding non-AMQP data to the codec.
    let agreed = response
        .headers()
        .get(http::header::SEC_WEBSOCKET_PROTOCOL)
        .and_then(|v| v.to_str().ok());
    match agreed {
        Some(p) if p.eq_ignore_ascii_case("amqp") => {}
        other => {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!("server did not select the `amqp` websocket subprotocol (got {other:?})"),
            ));
        }
    }

    Ok(WsByteStream::new(ws))
}
