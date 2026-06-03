//! TLS transport (WP-1.2): `rustls` (default) and optional `native-tls`.
//!
//! Both entry points take an established [`TcpStream`] and a DNS name and return
//! a TLS stream that satisfies [`IoStream`](super::IoStream). Trust and identity
//! are driven by [`TlsConfig`](super::TlsConfig): the webpki root set by default,
//! plus optional private CAs, a client certificate for mutual TLS, an SNI
//! override, or (test-only) verification bypass.

use tokio::net::TcpStream;

use super::TlsConfig;
use crate::error::{ConnectError, ErrorKind};

/// Establish a `rustls` TLS session over `tcp`, applying `cfg`'s trust anchors,
/// client identity, and server-name policy. Uses the `ring` crypto provider
/// explicitly (no reliance on a process-global default provider).
#[cfg(feature = "rustls")]
pub async fn connect_rustls(
    tcp: TcpStream,
    domain: &str,
    cfg: &TlsConfig,
) -> Result<tokio_rustls::client::TlsStream<TcpStream>, ConnectError> {
    use std::sync::Arc;
    use tokio_rustls::TlsConnector;
    use tokio_rustls::rustls::pki_types::ServerName;
    use tokio_rustls::rustls::{ClientConfig, RootCertStore};

    let provider = Arc::new(tokio_rustls::rustls::crypto::ring::default_provider());

    let builder = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;

    // Choose the certificate-verification strategy.
    let wants_client_auth = if cfg.danger_accept_invalid_certs {
        builder
            .dangerous()
            .with_custom_certificate_verifier(Arc::new(danger::NoVerification(provider.clone())))
    } else {
        let mut roots = RootCertStore::empty();
        if cfg.webpki_roots {
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
        }
        for pem in &cfg.root_ca_pem {
            for cert in load_certs(pem)? {
                roots
                    .add(cert)
                    .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;
            }
        }
        builder.with_root_certificates(roots)
    };

    // Optional client identity (mutual TLS).
    let config = match &cfg.client_auth_pem {
        Some((cert_pem, key_pem)) => {
            let certs = load_certs(cert_pem)?;
            let key = load_key(key_pem)?;
            wants_client_auth
                .with_client_auth_cert(certs, key)
                .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?
        }
        None => wants_client_auth.with_no_client_auth(),
    };

    let connector = TlsConnector::from(Arc::new(config));
    let name = cfg.server_name.as_deref().unwrap_or(domain).to_owned();
    let server_name = ServerName::try_from(name.clone())
        .map_err(|_| ConnectError::msg(ErrorKind::Tls, format!("invalid DNS name: {name}")))?;
    connector
        .connect(server_name, tcp)
        .await
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))
}

/// Parse a PEM blob into a chain of DER certificates.
#[cfg(feature = "rustls")]
fn load_certs(
    pem: &[u8],
) -> Result<Vec<tokio_rustls::rustls::pki_types::CertificateDer<'static>>, ConnectError> {
    let mut reader = std::io::Cursor::new(pem);
    rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))
}

/// Parse a PEM blob into a single private key (PKCS#8 / PKCS#1 / SEC1).
#[cfg(feature = "rustls")]
fn load_key(
    pem: &[u8],
) -> Result<tokio_rustls::rustls::pki_types::PrivateKeyDer<'static>, ConnectError> {
    let mut reader = std::io::Cursor::new(pem);
    rustls_pemfile::private_key(&mut reader)
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?
        .ok_or_else(|| ConnectError::msg(ErrorKind::Tls, "no private key found in PEM"))
}

/// A certificate verifier that accepts any chain. Test-only; gated behind
/// [`TlsConfig::danger_accept_invalid_certs`](super::TlsConfig).
#[cfg(feature = "rustls")]
mod danger {
    use std::sync::Arc;

    use tokio_rustls::rustls::client::danger::{
        HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
    };
    use tokio_rustls::rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
    use tokio_rustls::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use tokio_rustls::rustls::{DigitallySignedStruct, Error, SignatureScheme};

    #[derive(Debug)]
    pub struct NoVerification(pub Arc<CryptoProvider>);

    impl ServerCertVerifier for NoVerification {
        fn verify_server_cert(
            &self,
            _end_entity: &CertificateDer<'_>,
            _intermediates: &[CertificateDer<'_>],
            _server_name: &ServerName<'_>,
            _ocsp: &[u8],
            _now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }
}

/// Establish a `native-tls` session over `tcp` for `domain`.
///
/// `native-tls` uses the platform trust store; the [`TlsConfig`] fields for
/// extra CAs / client auth / verification bypass are honored where the backend
/// supports them.
#[cfg(feature = "native-tls")]
pub async fn connect_native_tls(
    tcp: TcpStream,
    domain: &str,
    cfg: &TlsConfig,
) -> Result<tokio_native_tls::TlsStream<TcpStream>, ConnectError> {
    let mut builder = native_tls::TlsConnector::builder();
    for pem in &cfg.root_ca_pem {
        let cert = native_tls::Certificate::from_pem(pem)
            .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;
        builder.add_root_certificate(cert);
    }
    if let Some((cert_pem, key_pem)) = &cfg.client_auth_pem {
        let identity = native_tls::Identity::from_pkcs8(cert_pem, key_pem)
            .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;
        builder.identity(identity);
    }
    if cfg.danger_accept_invalid_certs {
        builder.danger_accept_invalid_certs(true);
        builder.danger_accept_invalid_hostnames(true);
    }
    let native = builder
        .build()
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))?;
    let connector = tokio_native_tls::TlsConnector::from(native);
    let name = cfg.server_name.as_deref().unwrap_or(domain);
    connector
        .connect(name, tcp)
        .await
        .map_err(|e| ConnectError::new(ErrorKind::Tls).with_source(e))
}
