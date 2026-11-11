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

use rustls::client::WebPkiServerVerifier;
use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::crypto::CryptoProvider;
use rustls::pki_types::UnixTime;
use rustls::pki_types::pem::PemObject;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{ClientHello, ResolvesServerCert, WebPkiClientVerifier};
use rustls::sign::CertifiedKey;
use rustls::{ClientConfig, RootCertStore, ServerConfig};
use rustls::{DigitallySignedStruct, DistinguishedName, SignatureScheme};

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
    let (provider, cert, key) = (
        provider.clone(),
        tls.cert_file.clone(),
        tls.key_file.clone(),
    );
    spawn_watch(
        vec![tls.cert_file.clone(), tls.key_file.clone()],
        tls.reload_interval,
        Arc::downgrade(&resolver),
        move |r: &ReloadingResolver| {
            let ck = certified_key(&provider, &cert, &key)?;
            *r.current.write().expect("cert lock") = Arc::new(ck);
            tracing::info!(cert = %cert.display(), "TLS certificate reloaded");
            Ok(())
        },
    );
    Ok(resolver)
}

/// Poll `paths`; when any changes, run `rebuild` on the target (a failure —
/// e.g. mid-rotation, cert written but key not yet — keeps the current state
/// and retries next tick). Stops when the target is dropped.
fn spawn_watch<T, F>(
    paths: Vec<PathBuf>,
    interval: std::time::Duration,
    target: std::sync::Weak<T>,
    rebuild: F,
) where
    T: Send + Sync + 'static,
    F: Fn(&T) -> Result<(), String> + Send + 'static,
{
    let interval = interval.max(std::time::Duration::from_millis(100));
    tokio::spawn(async move {
        let refs = |p: &Vec<PathBuf>| fingerprint(&p.iter().collect::<Vec<_>>());
        let mut seen = refs(&paths);
        loop {
            tokio::time::sleep(interval).await;
            let Some(target) = target.upgrade() else {
                return;
            };
            let now = refs(&paths);
            if now == seen {
                continue;
            }
            match rebuild(&target) {
                Ok(()) => seen = now,
                Err(e) => {
                    tracing::warn!(error = %e, "TLS material reload failed; keeping the previous one")
                }
            }
        }
    });
}

/// The client-certificate verifier, rebuilt in place when the CA bundle
/// changes (cert-manager CA rotation needs no restart).
#[derive(Debug)]
struct ReloadingClientVerifier {
    inner: RwLock<Arc<dyn ClientCertVerifier>>,
}

impl ReloadingClientVerifier {
    fn current(&self) -> Arc<dyn ClientCertVerifier> {
        self.inner.read().expect("verifier lock").clone()
    }
}

fn build_client_verifier(
    provider: &Arc<CryptoProvider>,
    ca: &Path,
) -> Result<Arc<dyn ClientCertVerifier>, String> {
    WebPkiClientVerifier::builder_with_provider(Arc::new(load_roots(ca)?), provider.clone())
        .build()
        .map_err(|e| format!("client CA {}: {e}", ca.display()))
}

impl ClientCertVerifier for ReloadingClientVerifier {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        // Hints would borrow from a swappable value; clients send their
        // certificate without them.
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        self.current()
            .verify_client_cert(end_entity, intermediates, now)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }
}

/// The fabric dialer's server verifier (cluster CA), rebuilt in place when
/// the CA bundle changes.
#[derive(Debug)]
struct ReloadingServerVerifier {
    inner: RwLock<Arc<dyn ServerCertVerifier>>,
}

impl ReloadingServerVerifier {
    fn current(&self) -> Arc<dyn ServerCertVerifier> {
        self.inner.read().expect("verifier lock").clone()
    }
}

fn build_server_verifier(
    provider: &Arc<CryptoProvider>,
    ca: &Path,
) -> Result<Arc<dyn ServerCertVerifier>, String> {
    let v: Arc<dyn ServerCertVerifier> =
        WebPkiServerVerifier::builder_with_provider(Arc::new(load_roots(ca)?), provider.clone())
            .build()
            .map_err(|e| format!("cluster CA {}: {e}", ca.display()))?;
    Ok(v)
}

impl ServerCertVerifier for ReloadingServerVerifier {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        server_name: &ServerName<'_>,
        ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        self.current().verify_server_cert(
            end_entity,
            intermediates,
            server_name,
            ocsp_response,
            now,
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.current().verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.current().supported_verify_schemes()
    }
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
            let verifier = Arc::new(ReloadingClientVerifier {
                inner: RwLock::new(build_client_verifier(&provider, ca)?),
            });
            let (p, ca_path) = (provider.clone(), ca.clone());
            spawn_watch(
                vec![ca.clone()],
                tls.reload_interval,
                Arc::downgrade(&verifier),
                move |v: &ReloadingClientVerifier| {
                    *v.inner.write().expect("verifier lock") = build_client_verifier(&p, &ca_path)?;
                    tracing::info!(ca = %ca_path.display(), "client CA reloaded");
                    Ok(())
                },
            );
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
    let verifier = Arc::new(ReloadingServerVerifier {
        inner: RwLock::new(build_server_verifier(&provider, ca)?),
    });
    let (p, ca_path) = (provider.clone(), ca.clone());
    spawn_watch(
        vec![ca.clone()],
        tls.reload_interval,
        Arc::downgrade(&verifier),
        move |v: &ReloadingServerVerifier| {
            *v.inner.write().expect("verifier lock") = build_server_verifier(&p, &ca_path)?;
            tracing::info!(ca = %ca_path.display(), "cluster CA reloaded");
            Ok(())
        },
    );
    let config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .dangerous()
        .with_custom_certificate_verifier(verifier)
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

/// Whether `cert` (already chain-verified against the cluster CA) is valid
/// for at least one of `hosts` — a fabric peer must be a cluster member,
/// not merely hold some certificate from the same CA.
pub(crate) fn names_one_of(cert: &CertificateDer<'_>, hosts: &[ServerName<'static>]) -> bool {
    let Ok(ee) = webpki::EndEntityCert::try_from(cert) else {
        return false;
    };
    hosts
        .iter()
        .any(|h| ee.verify_is_valid_for_subject_name(h).is_ok())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cert_for(sans: &[&str]) -> CertificateDer<'static> {
        let key = rcgen::KeyPair::generate().unwrap();
        let params =
            rcgen::CertificateParams::new(sans.iter().map(|s| (*s).to_owned()).collect::<Vec<_>>())
                .unwrap();
        params.self_signed(&key).unwrap().der().clone()
    }

    /// Review H2: fabric membership is decided by the peer certificate
    /// naming a seed host (DNS or IP), not just by its CA.
    #[test]
    fn fabric_identity_requires_a_seed_host_name() {
        let seeds = vec![
            server_name("10.0.0.1:7472").unwrap(),
            server_name("broker-1.broker:7472").unwrap(),
        ];
        assert!(names_one_of(&cert_for(&["10.0.0.1"]), &seeds));
        assert!(names_one_of(&cert_for(&["broker-1.broker"]), &seeds));
        assert!(!names_one_of(&cert_for(&["app.example"]), &seeds));
        assert!(!names_one_of(&cert_for(&["10.0.0.9"]), &seeds));
        assert!(!names_one_of(&CertificateDer::from(vec![1, 2, 3]), &seeds));
    }
}
