//! Transport layer (Phase 1): byte streams, the length-delimited frame codec,
//! protocol-header negotiation, and optional TLS/WebSocket wrappers.

pub mod frame;
pub mod header;

#[cfg(any(feature = "rustls", feature = "native-tls"))]
pub mod tls;

#[cfg(feature = "ws")]
pub mod ws;

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::error::{ConnectError, ErrorKind};

/// Any bidirectional async byte stream usable as an AMQP transport.
///
/// A blanket impl covers every `AsyncRead + AsyncWrite + Unpin + Send`, so TCP,
/// TLS, and WebSocket streams all qualify without per-type boilerplate.
pub trait IoStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> IoStream for T {}

/// The URL scheme of a connection address.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scheme {
    /// `amqp://` — plaintext TCP.
    Amqp,
    /// `amqps://` — TLS over TCP.
    Amqps,
    /// `ws://` — AMQP over WebSocket.
    Ws,
    /// `wss://` — AMQP over WebSocket over TLS.
    Wss,
}

impl Scheme {
    /// Whether the scheme implies a TLS layer.
    pub fn is_tls(self) -> bool {
        matches!(self, Scheme::Amqps | Scheme::Wss)
    }

    /// Whether the scheme is WebSocket-based.
    pub fn is_websocket(self) -> bool {
        matches!(self, Scheme::Ws | Scheme::Wss)
    }

    fn default_port(self) -> u16 {
        match self {
            Scheme::Amqp => 5672,
            Scheme::Amqps => 5671,
            Scheme::Ws => 80,
            Scheme::Wss => 443,
        }
    }
}

/// A parsed connection address (`amqp[s]://[user[:pass]@]host[:port][/path]`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Address {
    /// The URL scheme.
    pub scheme: Scheme,
    /// The host to connect to.
    pub host: String,
    /// The (defaulted) port.
    pub port: u16,
    /// Optional SASL username.
    pub username: Option<String>,
    /// Optional SASL password.
    pub password: Option<String>,
    /// The path component (virtual host / node address); empty if none.
    pub path: String,
}

impl Address {
    /// Parse a connection URL.
    pub fn parse(url: &str) -> Result<Self, ConnectError> {
        let parsed = url::Url::parse(url).map_err(|e| {
            ConnectError::msg(ErrorKind::ProtocolViolation, format!("invalid url: {e}"))
        })?;
        let scheme = match parsed.scheme() {
            "amqp" => Scheme::Amqp,
            "amqps" => Scheme::Amqps,
            "ws" => Scheme::Ws,
            "wss" => Scheme::Wss,
            other => {
                return Err(ConnectError::msg(
                    ErrorKind::ProtocolViolation,
                    format!("unsupported scheme `{other}`"),
                ));
            }
        };
        let host = parsed
            .host_str()
            .ok_or_else(|| ConnectError::msg(ErrorKind::ProtocolViolation, "url has no host"))?
            .to_owned();
        let port = parsed.port().unwrap_or_else(|| scheme.default_port());
        let username = match parsed.username() {
            "" => None,
            u => Some(
                percent_decode(u),
            ),
        };
        let password = parsed.password().map(percent_decode);
        let path = parsed.path().trim_start_matches('/').to_owned();
        Ok(Address {
            scheme,
            host,
            port,
            username,
            password,
            path,
        })
    }
}

fn percent_decode(s: &str) -> String {
    // Minimal percent-decoding for userinfo (host/port already handled by `url`).
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn hex_val(b: u8) -> Option<u8> {
    match b {
        b'0'..=b'9' => Some(b - b'0'),
        b'a'..=b'f' => Some(b - b'a' + 10),
        b'A'..=b'F' => Some(b - b'A' + 10),
        _ => None,
    }
}

/// Establish a plain TCP connection to `addr` (TCP_NODELAY set for latency).
pub async fn connect_tcp(addr: &Address) -> Result<TcpStream, ConnectError> {
    let stream = TcpStream::connect((addr.host.as_str(), addr.port)).await?;
    let _ = stream.set_nodelay(true);
    Ok(stream)
}

/// A concrete AMQP byte transport, type-erasing the underlying stream variant
/// so the runtime can be monomorphized over a single `FramedTransport<Transport>`.
///
/// Every variant is `Unpin`, so the [`AsyncRead`]/[`AsyncWrite`] impls delegate
/// through `get_mut()` with no `unsafe`.
#[derive(Debug)]
#[allow(clippy::large_enum_variant)]
pub enum Transport {
    /// Plain TCP (`amqp://`).
    Tcp(TcpStream),
    /// `rustls` TLS (`amqps://`).
    #[cfg(feature = "rustls")]
    Rustls(tokio_rustls::client::TlsStream<TcpStream>),
    /// `native-tls` TLS (`amqps://`).
    #[cfg(feature = "native-tls")]
    NativeTls(tokio_native_tls::TlsStream<TcpStream>),
    /// AMQP-over-WebSocket on a plain stream (`ws://`).
    #[cfg(feature = "ws")]
    Ws(ws::WsByteStream<TcpStream>),
    /// AMQP-over-WebSocket on a `rustls` stream (`wss://`).
    #[cfg(all(feature = "ws", feature = "rustls"))]
    Wss(ws::WsByteStream<tokio_rustls::client::TlsStream<TcpStream>>),
}

impl AsyncRead for Transport {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "rustls")]
            Transport::Rustls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "native-tls")]
            Transport::NativeTls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "ws")]
            Transport::Ws(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(all(feature = "ws", feature = "rustls"))]
            Transport::Wss(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Transport {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        data: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_write(cx, data),
            #[cfg(feature = "rustls")]
            Transport::Rustls(s) => Pin::new(s).poll_write(cx, data),
            #[cfg(feature = "native-tls")]
            Transport::NativeTls(s) => Pin::new(s).poll_write(cx, data),
            #[cfg(feature = "ws")]
            Transport::Ws(s) => Pin::new(s).poll_write(cx, data),
            #[cfg(all(feature = "ws", feature = "rustls"))]
            Transport::Wss(s) => Pin::new(s).poll_write(cx, data),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "rustls")]
            Transport::Rustls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "native-tls")]
            Transport::NativeTls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "ws")]
            Transport::Ws(s) => Pin::new(s).poll_flush(cx),
            #[cfg(all(feature = "ws", feature = "rustls"))]
            Transport::Wss(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        match self.get_mut() {
            Transport::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "rustls")]
            Transport::Rustls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "native-tls")]
            Transport::NativeTls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "ws")]
            Transport::Ws(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(all(feature = "ws", feature = "rustls"))]
            Transport::Wss(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

/// Connect to `addr`, performing TLS and/or WebSocket layering per its scheme.
pub async fn connect(addr: &Address) -> Result<Transport, ConnectError> {
    match addr.scheme {
        Scheme::Amqp => Ok(Transport::Tcp(connect_tcp(addr).await?)),
        Scheme::Amqps => {
            let tcp = connect_tcp(addr).await?;
            connect_tls(tcp, &addr.host).await
        }
        Scheme::Ws => connect_ws_plain(addr).await,
        Scheme::Wss => connect_ws_tls(addr).await,
    }
}

#[cfg(feature = "rustls")]
async fn connect_tls(tcp: TcpStream, host: &str) -> Result<Transport, ConnectError> {
    Ok(Transport::Rustls(tls::connect_rustls(tcp, host).await?))
}

#[cfg(all(feature = "native-tls", not(feature = "rustls")))]
async fn connect_tls(tcp: TcpStream, host: &str) -> Result<Transport, ConnectError> {
    Ok(Transport::NativeTls(tls::connect_native_tls(tcp, host).await?))
}

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
async fn connect_tls(_tcp: TcpStream, _host: &str) -> Result<Transport, ConnectError> {
    Err(ConnectError::msg(
        ErrorKind::Tls,
        "amqps:// requires the `rustls` or `native-tls` feature",
    ))
}

#[cfg(feature = "ws")]
async fn connect_ws_plain(addr: &Address) -> Result<Transport, ConnectError> {
    let tcp = connect_tcp(addr).await?;
    let url = format!("ws://{}:{}/{}", addr.host, addr.port, addr.path);
    Ok(Transport::Ws(ws::connect_ws(tcp, &url).await?))
}

#[cfg(not(feature = "ws"))]
async fn connect_ws_plain(_addr: &Address) -> Result<Transport, ConnectError> {
    Err(ConnectError::msg(
        ErrorKind::Io,
        "ws:// requires the `ws` feature",
    ))
}

#[cfg(all(feature = "ws", feature = "rustls"))]
async fn connect_ws_tls(addr: &Address) -> Result<Transport, ConnectError> {
    let tcp = connect_tcp(addr).await?;
    let tls = tls::connect_rustls(tcp, &addr.host).await?;
    let url = format!("wss://{}:{}/{}", addr.host, addr.port, addr.path);
    Ok(Transport::Wss(ws::connect_ws(tls, &url).await?))
}

#[cfg(not(all(feature = "ws", feature = "rustls")))]
async fn connect_ws_tls(_addr: &Address) -> Result<Transport, ConnectError> {
    Err(ConnectError::msg(
        ErrorKind::Tls,
        "wss:// requires both the `ws` and `rustls` features",
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_amqp_urls() {
        let a = Address::parse("amqp://guest:secret@broker.example:5673/vhost").unwrap();
        assert_eq!(a.scheme, Scheme::Amqp);
        assert_eq!(a.host, "broker.example");
        assert_eq!(a.port, 5673);
        assert_eq!(a.username.as_deref(), Some("guest"));
        assert_eq!(a.password.as_deref(), Some("secret"));
        assert_eq!(a.path, "vhost");
    }

    #[test]
    fn defaults_ports_per_scheme() {
        assert_eq!(Address::parse("amqp://h").unwrap().port, 5672);
        assert_eq!(Address::parse("amqps://h").unwrap().port, 5671);
        assert!(Address::parse("amqps://h").unwrap().scheme.is_tls());
        assert!(Address::parse("ws://h").unwrap().scheme.is_websocket());
    }

    #[test]
    fn percent_decodes_userinfo() {
        let a = Address::parse("amqp://user%40domain:p%3Fw@h").unwrap();
        assert_eq!(a.username.as_deref(), Some("user@domain"));
        assert_eq!(a.password.as_deref(), Some("p?w"));
    }

    #[test]
    fn rejects_unknown_scheme() {
        assert!(Address::parse("http://h").is_err());
    }
}
