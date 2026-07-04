//! Server-side SASL primitives: PLAIN response parsing and (behind `scram`)
//! the RFC 5802 SCRAM server state machine.
//!
//! These are transport-agnostic: the broker's connection driver reads
//! `sasl-init`/`sasl-response` frames (types in [`crate::types::sasl`]) and
//! feeds the payloads through here, mapping the results to `sasl-outcome`
//! codes. Credential *storage* is the caller's concern — SCRAM needs only a
//! per-user [`ScramVerifier`] (salt, iteration count, and the derived keys),
//! never the plaintext password.

/// Parse a PLAIN initial response: `[authzid] NUL authcid NUL passwd`
/// (RFC 4616). Returns `(authzid, authcid, passwd)`; `None` if malformed.
pub fn parse_plain_response(data: &[u8]) -> Option<(Option<&str>, &str, &str)> {
    let mut parts = data.split(|&b| b == 0);
    let authzid = parts.next()?;
    let authcid = parts.next()?;
    let passwd = parts.next()?;
    if parts.next().is_some() {
        return None; // more than two NULs
    }
    let authzid = match authzid {
        [] => None,
        z => Some(std::str::from_utf8(z).ok()?),
    };
    Some((
        authzid,
        std::str::from_utf8(authcid).ok()?,
        std::str::from_utf8(passwd).ok()?,
    ))
}

#[cfg(feature = "scram")]
pub use scram_server::{ScramServer, ScramServerError, ScramVerifier};

#[cfg(feature = "scram")]
mod scram_server {
    use bytes::Bytes;

    use crate::sasl::scram::{ScramMechanism, ct_eq, gen_nonce, saslprep, unescape_username};

    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;

    /// Why a SCRAM exchange failed, split so the caller can map to the right
    /// `sasl-code`: credential failures ([`BadProof`](ScramServerError::BadProof),
    /// [`Saslprep`](ScramServerError::Saslprep)) → `auth`; everything else →
    /// a system error.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ScramServerError {
        /// The client message violates the RFC 5802 grammar or the exchange order.
        Malformed(&'static str),
        /// The client requested something we do not support (e.g. channel binding).
        Unsupported(&'static str),
        /// The client's proof does not verify against the stored key.
        BadProof,
        /// The username contains characters prohibited by SASLprep (RFC 4013).
        Saslprep,
    }

    impl std::fmt::Display for ScramServerError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            match self {
                ScramServerError::Malformed(m) => write!(f, "malformed scram message: {m}"),
                ScramServerError::Unsupported(m) => write!(f, "unsupported scram request: {m}"),
                ScramServerError::BadProof => f.write_str("scram client proof does not verify"),
                ScramServerError::Saslprep => {
                    f.write_str("username contains characters prohibited by SASLprep")
                }
            }
        }
    }

    impl std::error::Error for ScramServerError {}

    /// The per-user SCRAM credential material a server stores (RFC 5802 §9):
    /// the salt/iteration-count and the *derived* `StoredKey`/`ServerKey` —
    /// never the password itself.
    #[derive(Debug, Clone, PartialEq, Eq)]
    pub struct ScramVerifier {
        /// The user's salt.
        pub salt: Vec<u8>,
        /// The PBKDF2 iteration count.
        pub iterations: u32,
        /// `H(ClientKey)` — verifies the client's proof.
        pub stored_key: Vec<u8>,
        /// `HMAC(SaltedPassword, "Server Key")` — signs the server-final.
        pub server_key: Vec<u8>,
    }

    impl ScramVerifier {
        /// Derive a verifier from a plaintext password (provisioning-time
        /// helper; the password is SASLprep-prepared first). Returns `None`
        /// if SASLprep rejects the password.
        pub fn derive(
            mechanism: ScramMechanism,
            password: &str,
            salt: &[u8],
            iterations: u32,
        ) -> Option<Self> {
            let password = saslprep(password)?;
            let salted = mechanism.pbkdf2(password.as_bytes(), salt, iterations);
            let client_key = mechanism.hmac(&salted, b"Client Key");
            Some(ScramVerifier {
                salt: salt.to_vec(),
                iterations,
                stored_key: mechanism.h(&client_key),
                server_key: mechanism.hmac(&salted, b"Server Key"),
            })
        }
    }

    enum Phase {
        ExpectClientFirst,
        ExpectVerifier,
        ExpectClientFinal,
        Done,
    }

    /// The RFC 5802 server state machine.
    ///
    /// Flow: [`on_client_first`](ScramServer::on_client_first) yields the
    /// (SASLprep-prepared) username → the caller looks up that user's
    /// [`ScramVerifier`] → [`server_first`](ScramServer::server_first) builds
    /// the challenge → [`on_client_final`](ScramServer::on_client_final)
    /// verifies the proof and returns the `v=...` server-final to carry in the
    /// outcome's `additional-data`.
    ///
    /// Channel binding is not supported: a client demanding it (`p=...`) is
    /// rejected with [`ScramServerError::Unsupported`].
    pub struct ScramServer {
        mechanism: ScramMechanism,
        server_nonce_suffix: String,
        phase: Phase,
        username: Option<String>,
        client_first_bare: String,
        combined_nonce: String,
        expected_channel_binding: String,
        server_first_msg: String,
        verifier: Option<ScramVerifier>,
    }

    impl std::fmt::Debug for ScramServer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            // Never print nonces or key material.
            f.debug_struct("ScramServer")
                .field("mechanism", &self.mechanism)
                .field("username", &self.username)
                .finish_non_exhaustive()
        }
    }

    impl ScramServer {
        /// Create a server exchange with a fresh random server nonce.
        pub fn new(mechanism: ScramMechanism) -> Self {
            Self::with_nonce(mechanism, gen_nonce())
        }

        /// Create with a fixed server-nonce suffix (deterministic tests).
        pub fn with_nonce(mechanism: ScramMechanism, server_nonce_suffix: String) -> Self {
            ScramServer {
                mechanism,
                server_nonce_suffix,
                phase: Phase::ExpectClientFirst,
                username: None,
                client_first_bare: String::new(),
                combined_nonce: String::new(),
                expected_channel_binding: String::new(),
                server_first_msg: String::new(),
                verifier: None,
            }
        }

        /// The mechanism this exchange runs.
        pub fn mechanism(&self) -> ScramMechanism {
            self.mechanism
        }

        /// The authenticated username (available after
        /// [`on_client_first`](ScramServer::on_client_first)).
        pub fn username(&self) -> Option<&str> {
            self.username.as_deref()
        }

        /// Consume the client-first message; returns the SASLprep-prepared
        /// username so the caller can fetch that user's [`ScramVerifier`].
        pub fn on_client_first(&mut self, data: &[u8]) -> Result<&str, ScramServerError> {
            if !matches!(self.phase, Phase::ExpectClientFirst) {
                return Err(ScramServerError::Malformed("client-first out of order"));
            }
            let msg = std::str::from_utf8(data)
                .map_err(|_| ScramServerError::Malformed("client-first is not UTF-8"))?;

            // gs2-header: "n," | "y," (+ optional "a=authzid") + "," then bare.
            let (gs2_cbind, rest) = msg
                .split_at_checked(2)
                .ok_or(ScramServerError::Malformed("truncated gs2 header"))?;
            match gs2_cbind {
                "n," | "y," => {}
                _ if gs2_cbind.starts_with("p=") || msg.starts_with("p=") => {
                    return Err(ScramServerError::Unsupported("channel binding"));
                }
                _ => return Err(ScramServerError::Malformed("bad gs2 header")),
            }
            let bare_start = rest
                .find(',')
                .ok_or(ScramServerError::Malformed("gs2 header missing terminator"))?;
            let gs2_header = &msg[..2 + bare_start + 1];
            let bare = &rest[bare_start + 1..];

            let mut username = None;
            let mut client_nonce = None;
            for attr in bare.split(',') {
                match attr.split_once('=') {
                    Some(("n", v)) => username = Some(v),
                    Some(("r", v)) => client_nonce = Some(v),
                    Some(("m", _)) => {
                        return Err(ScramServerError::Unsupported("mandatory extension"));
                    }
                    _ => {}
                }
            }
            let username = username.ok_or(ScramServerError::Malformed("client-first missing n"))?;
            let client_nonce =
                client_nonce.ok_or(ScramServerError::Malformed("client-first missing r"))?;
            if client_nonce.is_empty() {
                return Err(ScramServerError::Malformed("empty client nonce"));
            }

            let unescaped = unescape_username(username)
                .ok_or(ScramServerError::Malformed("bad username escape"))?;
            let prepared = saslprep(&unescaped).ok_or(ScramServerError::Saslprep)?;

            self.client_first_bare = bare.to_owned();
            self.combined_nonce = format!("{client_nonce}{}", self.server_nonce_suffix);
            // client-final's c= carries base64(gs2-header) verbatim.
            self.expected_channel_binding = STANDARD.encode(gs2_header.as_bytes());
            self.username = Some(prepared);
            self.phase = Phase::ExpectVerifier;
            Ok(self.username.as_deref().expect("just set"))
        }

        /// Build the server-first challenge from the user's verifier.
        pub fn server_first(&mut self, verifier: ScramVerifier) -> Bytes {
            debug_assert!(matches!(self.phase, Phase::ExpectVerifier));
            self.server_first_msg = format!(
                "r={},s={},i={}",
                self.combined_nonce,
                STANDARD.encode(&verifier.salt),
                verifier.iterations
            );
            self.verifier = Some(verifier);
            self.phase = Phase::ExpectClientFinal;
            Bytes::from(self.server_first_msg.clone().into_bytes())
        }

        /// Verify the client-final proof. On success returns the `v=...`
        /// server-final bytes (carried in the outcome's `additional-data`).
        pub fn on_client_final(&mut self, data: &[u8]) -> Result<Bytes, ScramServerError> {
            if !matches!(self.phase, Phase::ExpectClientFinal) {
                return Err(ScramServerError::Malformed("client-final out of order"));
            }
            let msg = std::str::from_utf8(data)
                .map_err(|_| ScramServerError::Malformed("client-final is not UTF-8"))?;

            let mut channel_binding = None;
            let mut nonce = None;
            let mut proof_b64 = None;
            for attr in msg.split(',') {
                match attr.split_once('=') {
                    Some(("c", v)) => channel_binding = Some(v),
                    Some(("r", v)) => nonce = Some(v),
                    Some(("p", v)) => proof_b64 = Some(v),
                    _ => {}
                }
            }
            let channel_binding =
                channel_binding.ok_or(ScramServerError::Malformed("client-final missing c"))?;
            let nonce = nonce.ok_or(ScramServerError::Malformed("client-final missing r"))?;
            let proof_b64 =
                proof_b64.ok_or(ScramServerError::Malformed("client-final missing p"))?;

            if channel_binding != self.expected_channel_binding {
                return Err(ScramServerError::Malformed("channel-binding mismatch"));
            }
            if nonce != self.combined_nonce {
                return Err(ScramServerError::Malformed(
                    "nonce does not match server-first",
                ));
            }
            let proof = STANDARD
                .decode(proof_b64)
                .map_err(|_| ScramServerError::Malformed("proof is not base64"))?;

            let verifier = self
                .verifier
                .as_ref()
                .expect("verifier present in ExpectClientFinal");
            let m = self.mechanism;
            if proof.len() != verifier.stored_key.len() {
                return Err(ScramServerError::BadProof);
            }

            // client-final-without-proof is everything before ",p=".
            let without_proof = &msg[..msg.rfind(",p=").expect("p= parsed above")];
            let auth_message = format!(
                "{},{},{}",
                self.client_first_bare, self.server_first_msg, without_proof
            );
            let client_signature = m.hmac(&verifier.stored_key, auth_message.as_bytes());
            let client_key: Vec<u8> = proof
                .iter()
                .zip(client_signature.iter())
                .map(|(a, b)| a ^ b)
                .collect();
            if !ct_eq(&m.h(&client_key), &verifier.stored_key) {
                return Err(ScramServerError::BadProof);
            }

            let server_signature = m.hmac(&verifier.server_key, auth_message.as_bytes());
            self.phase = Phase::Done;
            Ok(Bytes::from(
                format!("v={}", STANDARD.encode(server_signature)).into_bytes(),
            ))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn plain_parsing() {
        assert_eq!(
            parse_plain_response(b"\0user\0pw"),
            Some((None, "user", "pw"))
        );
        assert_eq!(
            parse_plain_response(b"admin\0user\0pw"),
            Some((Some("admin"), "user", "pw"))
        );
        // Passwords may be empty; structure must still hold.
        assert_eq!(parse_plain_response(b"\0user\0"), Some((None, "user", "")));
        assert_eq!(parse_plain_response(b"no-nuls"), None);
        assert_eq!(parse_plain_response(b"\0only-one-nul"), None);
        assert_eq!(parse_plain_response(b"\0a\0b\0c"), None);
        assert_eq!(parse_plain_response(b"\0user\0\xff\xfe"), None);
    }

    #[cfg(feature = "scram")]
    mod scram {
        use super::super::{ScramServer, ScramServerError, ScramVerifier};
        use crate::sasl::scram::ScramMechanism;
        use base64::Engine;
        use base64::engine::general_purpose::STANDARD;

        // RFC 5802 §5 worked example, server side.
        const CLIENT_FIRST: &[u8] = b"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL";
        const SERVER_NONCE: &str = "3rfcNHYJY1ZVvWVs7j";
        const CLIENT_FINAL: &[u8] =
            b"c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=";

        fn rfc_verifier() -> ScramVerifier {
            let salt = STANDARD.decode("QSXCR+Q6sek8bf92").unwrap();
            ScramVerifier::derive(ScramMechanism::Sha1, "pencil", &salt, 4096).unwrap()
        }

        #[test]
        fn rfc5802_sha1_server_vector() {
            let mut s = ScramServer::with_nonce(ScramMechanism::Sha1, SERVER_NONCE.into());
            let user = s.on_client_first(CLIENT_FIRST).unwrap();
            assert_eq!(user, "user");

            let challenge = s.server_first(rfc_verifier());
            assert_eq!(
                &challenge[..],
                b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096"
            );

            let server_final = s.on_client_final(CLIENT_FINAL).unwrap();
            assert_eq!(&server_final[..], b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=");
        }

        #[test]
        fn wrong_password_is_bad_proof_not_panic() {
            let mut s = ScramServer::with_nonce(ScramMechanism::Sha1, SERVER_NONCE.into());
            s.on_client_first(CLIENT_FIRST).unwrap();
            // Verifier derived from a DIFFERENT password.
            let salt = STANDARD.decode("QSXCR+Q6sek8bf92").unwrap();
            let wrong =
                ScramVerifier::derive(ScramMechanism::Sha1, "not-pencil", &salt, 4096).unwrap();
            s.server_first(wrong);
            assert_eq!(
                s.on_client_final(CLIENT_FINAL),
                Err(ScramServerError::BadProof)
            );
        }

        #[test]
        fn nonce_and_channel_binding_are_enforced() {
            let mut s = ScramServer::with_nonce(ScramMechanism::Sha1, SERVER_NONCE.into());
            s.on_client_first(CLIENT_FIRST).unwrap();
            s.server_first(rfc_verifier());
            // Tampered nonce.
            let err = s
                .on_client_final(b"c=biws,r=tampered,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts=")
                .unwrap_err();
            assert!(matches!(err, ScramServerError::Malformed(_)));
        }

        #[test]
        fn channel_binding_demand_is_rejected() {
            let mut s = ScramServer::new(ScramMechanism::Sha256);
            let err = s
                .on_client_first(b"p=tls-unique,,n=user,r=abc")
                .unwrap_err();
            assert_eq!(err, ScramServerError::Unsupported("channel binding"));
        }

        #[test]
        fn escaped_usernames_unescape() {
            let mut s = ScramServer::new(ScramMechanism::Sha256);
            let user = s.on_client_first(b"n,,n=who=2Cami=3D,r=abc").unwrap();
            assert_eq!(user, "who,ami=");
        }
    }
}
