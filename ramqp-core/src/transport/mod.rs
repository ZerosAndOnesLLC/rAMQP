//! Role-neutral transport layer: byte-stream abstraction, the length-delimited
//! frame codec, protocol-header read/write, and address parsing.
//!
//! Dial-side concerns (TCP/TLS/WebSocket connectors and the client `Transport`
//! enum) live in the `ramqp` client; accept-side concerns live in
//! `ramqp-broker`. Both drive the same [`frame::FramedTransport`] over any
//! [`IoStream`].

pub mod frame;
pub mod header;

use tokio::io::{AsyncRead, AsyncWrite};

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

    /// The IANA default port for the scheme.
    pub fn default_port(self) -> u16 {
        match self {
            Scheme::Amqp => 5672,
            Scheme::Amqps => 5671,
            Scheme::Ws => 80,
            Scheme::Wss => 443,
        }
    }
}

/// A parsed connection address (`amqp[s]://[user[:pass]@]host[:port][/path]`).
#[derive(Clone, PartialEq, Eq)]
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

impl std::fmt::Debug for Address {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print URL credentials: show only whether they are present.
        f.debug_struct("Address")
            .field("scheme", &self.scheme)
            .field("host", &self.host)
            .field("port", &self.port)
            .field("username", &self.username.as_ref().map(|_| "***"))
            .field("password", &self.password.as_ref().map(|_| "***"))
            .field("path", &self.path)
            .finish()
    }
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
            u => Some(percent_decode(u)),
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
        if bytes[i] == b'%' && i + 2 < bytes.len()
            && let (Some(h), Some(l)) = (hex_val(bytes[i + 1]), hex_val(bytes[i + 2])) {
                out.push(h * 16 + l);
                i += 3;
                continue;
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

    #[test]
    fn debug_redacts_credentials() {
        let a = Address::parse("amqp://guest:hunter2@broker.example:5673/vhost").unwrap();
        let dbg = format!("{a:?}");
        assert!(!dbg.contains("hunter2"), "password leaked in Debug: {dbg}");
        assert!(!dbg.contains("guest"), "username leaked in Debug: {dbg}");
        // Non-secret fields remain visible.
        assert!(dbg.contains("broker.example"));
        assert!(dbg.contains("vhost"));
    }
}
