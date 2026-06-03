//! TLS transport (WP-1.2): `rustls` (default) and optional `native-tls`.
//!
//! Both entry points take an established [`TcpStream`] and a DNS name and return
//! a TLS stream that satisfies [`IoStream`](super::IoStream).

use tokio::net::TcpStream;

use crate::error::{ConnectError, ErrorKind};

/// Establish a `rustls` TLS session over `tcp` for `domain`, trusting the
/// Mozilla webpki root set. Uses the `ring` crypto provider explicitly (no
/// reliance on a process-global default provider).
#[cfg(feature = "rustls")]
pub async fn connect_rustls(
    tcp: TcpStream,
    domain: &str,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, ConnectError> {
    use std::sync::Arc;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    let config = ClientConfig::builder_with_provider(Arc::new(
        tokio_rustls::rustls::crypto::ring::default_provider(),
    ))
    .with_safe_default_protocol_versions()
    .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?
    .with_root_certificates(roots)
    .with_no_client_auth();

    let connector = TlsConnector::from(Arc::new(config));
    let server_name = ServerName::try_from(domain.to_owned())
        .map_err(|_| ConnectError::msg(ErrorKind::Tls, format!("invalid DNS name: {domain}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))
}

/// Establish a `native-tls` session over `tcp` for `domain`.
#[cfg(feature = "native-tls")]
pub async fn connect_native_tls(
    tcp: TcpStream,
    domain: &str,
) -> Result<tokio_native_tls::TlsStream<TcpStream>, ConnectError> {
    let native = native_tls::TlsConnector::new()
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;
    let connector = tokio_native_tls::TlsConnector::from(native);
    connector
        .connect(domain, tcp)
        .await
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))
}
