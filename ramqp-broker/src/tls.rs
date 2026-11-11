//! Server TLS (issue #37 §4): rustls on the `ring` provider (`tls`) or on
//! AWS-LC's FIPS 140-3 module (`fips`), with certificates reloaded from disk
//! when they change and optional client-certificate verification (mTLS).
//!
//! The same machinery serves the AMQP TLS listener, the management
//! endpoint, and the cluster fabric (which also dials out with a client
//! config built from the same files).

use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::SystemTime;

use rustls::crypto::CryptoProvider;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::config::TlsServerConfig;

/// The crypto provider for this build: AWS-LC FIPS under `fips` (which
/// wins if both features are on), else ring.
pub(crate) fn provider() -> Arc<CryptoProvider> {
    #[cfg(feature = "fips")]
    {
        Arc::new(rustls::crypto::default_fips_provider())
    }
    #[cfg(not(feature = "fips"))]
    {
        Arc::new(rustls::crypto::ring::default_provider())
    }
}

fn load_certs(path: &Path) -> Result<Vec<CertificateDer<'static>>, String> {
    let certs: Vec<_> = CertificateDer::pem_file_iter(path)
        .map_err(|e| format!("{}: {e}", path.display()))?
        .collect::<Result<_, _>>()
        .map_err(|e| format!("{}: {e}", path.display()))?;
    if certs.is_empty() {
        return Err(format!("{}: no PEM certificates", path.display()));
    }
    Ok(certs)
}

fn load_key(path: &Path) -> Result<PrivateKeyDer<'static>, String> {
    PrivateKeyDer::from_pem_file(path).map_err(|e| format!("{}: {e}", path.display()))
}

fn load_roots(path: &Path) -> Result<RootCertStore, String> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(path)? {
        roots
            .add(cert)
            .map_err(|e| format!("{}: {e}", path.display()))?;
    }
    Ok(roots)
}

/// Load a certificate chain + key into a signing identity, checking that
/// the key matches the leaf.
fn certified_key(
    provider: &CryptoProvider,
    cert: &Path,
    key: &Path,
) -> Result<CertifiedKey, String> {
    let chain = load_certs(cert)?;
    let key_der = load_key(key)?;
    let ck = CertifiedKey::from_der(chain, key_der, provider)
        .map_err(|e| format!("{} / {}: {e}", cert.display(), key.display()))?;
    ck.keys_match()
        .map_err(|e| format!("{} does not match {}: {e}", key.display(), cert.display()))?;
    Ok(ck)
}

/// The current certificate, swapped in place on reload; presented as the
/// server certificate (listeners) or the client certificate (fabric dials).
#[derive(Debug)]
pub(crate) struct ReloadingResolver {
    current: RwLock<Arc<CertifiedKey>>,
}

impl ReloadingResolver {
    fn current(&self) -> Arc<CertifiedKey> {
        self.current.read().expect("cert lock").clone()
    }
}

impl ResolvesServerCert for ReloadingResolver {
    fn resolve(&self, _hello: ClientHello<'_>) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }
}

impl rustls::client::ResolvesClientCert for ReloadingResolver {
    fn resolve(
        &self,
        _root_hint_subjects: &[&[u8]],
        _sigschemes: &[rustls::SignatureScheme],
    ) -> Option<Arc<CertifiedKey>> {
        Some(self.current())
    }

    fn has_certs(&self) -> bool {
        true
    }
}

/// A resolver for `tls`'s files with its reloader running.
fn reloading(
    provider: &Arc<CryptoProvider>,
    tls: &TlsServerConfig,
) -> Result<Arc<ReloadingResolver>, String> {
    let initial = certified_key(provider, &tls.cert_file, &tls.key_file)?;
    let resolver = Arc::new(ReloadingResolver {
        current: RwLock::new(Arc::new(initial)),
    });
    spawn_reloader(
        Arc::downgrade(&resolver),
        provider.clone(),
        tls.cert_file.clone(),
        tls.key_file.clone(),
        tls.reload_interval,
    );
    Ok(resolver)
}

/// A file's change fingerprint (mtime + length; a rotation that rewrites
/// in place changes at least the mtime).
fn fingerprint(paths: &[&PathBuf]) -> Vec<Option<(SystemTime, u64)>> {
    paths
        .iter()
        .map(|p| {
            std::fs::metadata(p)
                .ok()
                .and_then(|m| Some((m.modified().ok()?, m.len())))
        })
        .collect()
}

/// Build the server config (and start its reloader) for a listener.
pub(crate) fn server_config(tls: &TlsServerConfig) -> Result<Arc<ServerConfig>, String> {
    let provider = provider();
    let resolver = reloading(&provider, tls)?;
    let builder = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?;
    let builder = match &tls.client_ca_file {
        Some(ca) => {
            let verifier = WebPkiClientVerifier::builder_with_provider(
                Arc::new(load_roots(ca)?),
                provider.clone(),
            )
            .build()
            .map_err(|e| format!("client CA {}: {e}", ca.display()))?;
            builder.with_client_cert_verifier(verifier)
        }
        None => builder.with_no_client_auth(),
    };
    let config = builder.with_cert_resolver(resolver);
    if cfg!(feature = "fips") && !config.fips() {
        return Err("the `fips` build produced a non-FIPS TLS configuration".to_owned());
    }
    Ok(Arc::new(config))
}

/// Poll the certificate files; on change, load and swap (a failed load
/// keeps the previous certificate). Stops when the resolver is dropped.
fn spawn_reloader(
    resolver: std::sync::Weak<ReloadingResolver>,
    provider: Arc<CryptoProvider>,
    cert: PathBuf,
    key: PathBuf,
    interval: std::time::Duration,
) {
    let interval = interval.max(std::time::Duration::from_millis(100));
    tokio::spawn(async move {
        let mut seen = fingerprint(&[&cert, &key]);
        loop {
            tokio::time::sleep(interval).await;
            let Some(resolver) = resolver.upgrade() else {
                return;
            };
            let now = fingerprint(&[&cert, &key]);
            if now == seen {
                continue;
            }
            match certified_key(&provider, &cert, &key) {
                Ok(ck) => {
                    *resolver.current.write().expect("cert lock") = Arc::new(ck);
                    seen = now;
                    tracing::info!(cert = %cert.display(), "TLS certificate reloaded");
                }
                // Mid-rotation (cert written, key not yet) or broken: keep
                // serving the old pair and try again next tick.
                Err(e) => {
                    tracing::warn!(error = %e, "TLS certificate reload failed; keeping the previous one")
                }
            }
        }
    });
}

/// The acceptor for a TLS listener.
pub(crate) fn acceptor(tls: &TlsServerConfig) -> Result<tokio_rustls::TlsAcceptor, String> {
    Ok(tokio_rustls::TlsAcceptor::from(server_config(tls)?))
}

/// The client side of the fabric's mutual TLS: trust the cluster CA and
/// present this node's (reloading) certificate.
pub(crate) fn fabric_connector(
    tls: &TlsServerConfig,
) -> Result<tokio_rustls::TlsConnector, String> {
    let ca = tls
        .client_ca_file
        .as_ref()
        .ok_or("fabric TLS needs `client_ca_file` (the cluster CA)")?;
    let provider = provider();
    let resolver = reloading(&provider, tls)?;
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(load_roots(ca)?)
        .with_client_cert_resolver(resolver);
    if cfg!(feature = "fips") && !config.fips() {
        return Err("the `fips` build produced a non-FIPS TLS configuration".to_owned());
    }
    Ok(tokio_rustls::TlsConnector::from(Arc::new(config)))
}

/// The TLS server name for a peer's `host:port` fabric address.
pub(crate) fn server_name(addr: &str) -> Result<ServerName<'static>, String> {
    let host = match addr.rsplit_once(':') {
        Some((h, _)) => h.trim_start_matches('[').trim_end_matches(']'),
        None => addr,
    };
    ServerName::try_from(host.to_owned()).map_err(|e| format!("peer address {addr}: {e}"))
}
