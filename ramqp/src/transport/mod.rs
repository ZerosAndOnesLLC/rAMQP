//! Transport layer: dial-side connectors (TCP, TLS, WebSocket) over the
//! role-neutral byte-stream/framing layer re-exported from `ramqp-core`.

// Role-neutral pieces live in ramqp-core; re-export them so existing
// `ramqp::transport::...` / `crate::transport::...` paths keep resolving.
pub use ramqp_core::transport::{Address, IoStream, Scheme, frame, header};

#[cfg(any(feature = "rustls", feature = "native-tls"))]
pub mod tls;

#[cfg(feature = "ws")]
pub mod ws;

use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::error::ConnectError;

/// TLS trust and identity configuration for `amqps://` / `wss://`.
///
/// By default the client trusts the bundled Mozilla webpki root set. Use this to
/// add a private CA, present a client certificate for mutual TLS, override the
/// verified server name, or — for testing only — disable verification.
///
/// The fields are usually set through the [`ConnectionBuilder`](crate::ConnectionBuilder)
/// helpers (`add_root_ca_pem`, `client_auth_pem`, `tls_server_name`, …), which
/// require the `rustls` or `native-tls` feature.
///
/// # Examples
/// Connect to a broker behind a private CA:
/// ```no_run
/// # async fn ex() -> Result<(), Box<dyn std::error::Error>> {
/// use ramqp::ConnectionBuilder;
///
/// let ca = std::fs::read("internal-ca.pem")?;
/// let conn = ConnectionBuilder::new("amqps://broker.internal:5671")
///     .add_root_ca_pem(ca)      // trust our CA in addition to the webpki roots
///     .connect()
///     .await?;
/// # let _ = conn; Ok(()) }
/// ```
#[derive(Clone)]
pub struct TlsConfig {
    /// Additional trust-anchor CA certificates, PEM-encoded (one or more certs
    /// per entry). Trusted in addition to the webpki roots when those are on.
    pub root_ca_pem: Vec<Vec<u8>>,
    /// Also trust the bundled Mozilla webpki roots (default: `true`).
    pub webpki_roots: bool,
    /// Client certificate chain + private key, PEM-encoded, for mutual TLS.
    pub client_auth_pem: Option<(Vec<u8>, Vec<u8>)>,
    /// Override the server name used for SNI and certificate verification
    /// (otherwise the URL host is used).
    pub server_name: Option<String>,
    /// **DANGER** — accept any server certificate without verification. This
    /// also disables hostname verification (both TLS backends: rustls skips the
    /// server name, native-tls additionally sets `danger_accept_invalid_hostnames`),
    /// so an enabled connection is fully open to man-in-the-middle. Intended only
    /// for tests against self-signed brokers; never enable in production.
    pub danger_accept_invalid_certs: bool,
}

impl Default for TlsConfig {
    fn default() -> Self {
        TlsConfig {
            root_ca_pem: Vec::new(),
            webpki_roots: true,
            client_auth_pem: None,
            server_name: None,
            danger_accept_invalid_certs: false,
        }
    }
}

impl std::fmt::Debug for TlsConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Never print key material.
        f.debug_struct("TlsConfig")
            .field(
                "root_ca_pem",
                &format_args!("[{} CA blob(s)]", self.root_ca_pem.len()),
            )
            .field("webpki_roots", &self.webpki_roots)
            .field("client_auth_pem", &self.client_auth_pem.is_some())
            .field("server_name", &self.server_name)
            .field(
                "danger_accept_invalid_certs",
                &self.danger_accept_invalid_certs,
            )
            .finish()
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
/// `tls` supplies trust/identity material for the `amqps://` and `wss://` paths.
pub async fn connect(addr: &Address, tls: &TlsConfig) -> Result<Transport, ConnectError> {
    match addr.scheme {
        Scheme::Amqp => Ok(Transport::Tcp(connect_tcp(addr).await?)),
        Scheme::Amqps => {
            let tcp = connect_tcp(addr).await?;
            connect_tls(tcp, &addr.host, tls).await
        }
        Scheme::Ws => connect_ws_plain(addr).await,
        Scheme::Wss => connect_ws_tls(addr, tls).await,
    }
}

#[cfg(feature = "rustls")]
async fn connect_tls(
    tcp: TcpStream,
    host: &str,
    tls: &TlsConfig,
) -> Result<Transport, ConnectError> {
    Ok(Transport::Rustls(
        tls::connect_rustls(tcp, host, tls).await?,
    ))
}

#[cfg(all(feature = "native-tls", not(feature = "rustls")))]
async fn connect_tls(
    tcp: TcpStream,
    host: &str,
    tls: &TlsConfig,
) -> Result<Transport, ConnectError> {
    Ok(Transport::NativeTls(
        tls::connect_native_tls(tcp, host, tls).await?,
    ))
}

#[cfg(not(any(feature = "rustls", feature = "native-tls")))]
async fn connect_tls(
    _tcp: TcpStream,
    _host: &str,
    _tls: &TlsConfig,
) -> Result<Transport, ConnectError> {
    Err(ConnectError::msg(
        crate::error::ErrorKind::Tls,
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
        crate::error::ErrorKind::Io,
        "ws:// requires the `ws` feature",
    ))
}

#[cfg(all(feature = "ws", feature = "rustls"))]
async fn connect_ws_tls(addr: &Address, tls: &TlsConfig) -> Result<Transport, ConnectError> {
    let tcp = connect_tcp(addr).await?;
    let stream = tls::connect_rustls(tcp, &addr.host, tls).await?;
    let url = format!("wss://{}:{}/{}", addr.host, addr.port, addr.path);
    Ok(Transport::Wss(ws::connect_ws(stream, &url).await?))
}

#[cfg(not(all(feature = "ws", feature = "rustls")))]
async fn connect_ws_tls(_addr: &Address, _tls: &TlsConfig) -> Result<Transport, ConnectError> {
    Err(ConnectError::msg(
        crate::error::ErrorKind::Tls,
        "wss:// requires both the `ws` and `rustls` features",
    ))
}
