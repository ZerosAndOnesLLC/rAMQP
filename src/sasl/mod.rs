//! SASL authentication (WP-1.4): the client-side negotiation state machine and
//! the ANONYMOUS / PLAIN / EXTERNAL mechanisms, plus SCRAM-SHA-1/256/512 behind
//! the `scram` feature.

use bytes::Bytes;

use crate::codec::Symbol;
use crate::error::{ConnectError, ErrorKind};
use crate::transport::IoStream;
use crate::transport::frame::{Frame, FrameBody, FramedTransport};
use crate::types::sasl::{SaslCode, SaslFrame, SaslInit, SaslResponse};

#[cfg(feature = "scram")]
use scram::ScramClient;

/// A SASL authentication profile selected by the client.
#[derive(Debug, Clone)]
pub enum SaslProfile {
    /// The ANONYMOUS mechanism (no credentials).
    Anonymous,
    /// The PLAIN mechanism (`authcid` + password).
    Plain {
        /// Authentication identity (username).
        authcid: String,
        /// Password.
        passwd: String,
    },
    /// The EXTERNAL mechanism (identity established by the transport, e.g. mTLS).
    External {
        /// Optional authorization identity.
        authzid: Option<String>,
    },
    /// A SCRAM mechanism.
    #[cfg(feature = "scram")]
    Scram {
        /// Which SCRAM hash to use.
        mechanism: ScramMechanism,
        /// Username.
        username: String,
        /// Password.
        password: String,
    },
}

impl SaslProfile {
    /// Choose a profile from optional URL credentials: PLAIN when both a
    /// username and password are present, otherwise ANONYMOUS.
    pub fn from_credentials(username: Option<String>, password: Option<String>) -> Self {
        match (username, password) {
            (Some(authcid), Some(passwd)) => SaslProfile::Plain { authcid, passwd },
            _ => SaslProfile::Anonymous,
        }
    }

    /// The SASL mechanism name advertised on the wire.
    pub fn mechanism_name(&self) -> &str {
        match self {
            SaslProfile::Anonymous => "ANONYMOUS",
            SaslProfile::Plain { .. } => "PLAIN",
            SaslProfile::External { .. } => "EXTERNAL",
            #[cfg(feature = "scram")]
            SaslProfile::Scram { mechanism, .. } => mechanism.name(),
        }
    }

    /// Produce the `sasl-init` initial response and the per-mechanism state used
    /// to answer any challenges.
    fn start(&self) -> (Option<Bytes>, MechState) {
        match self {
            SaslProfile::Anonymous => (Some(Bytes::from_static(b"anonymous")), MechState::Simple),
            SaslProfile::Plain { authcid, passwd } => {
                (Some(plain_response(authcid, passwd)), MechState::Simple)
            }
            SaslProfile::External { authzid } => (
                Some(Bytes::from(
                    authzid.clone().unwrap_or_default().into_bytes(),
                )),
                MechState::Simple,
            ),
            #[cfg(feature = "scram")]
            SaslProfile::Scram {
                mechanism,
                username,
                password,
            } => {
                let mut client = ScramClient::new(*mechanism, username, password);
                let first = client.client_first();
                (Some(first), MechState::Scram(client))
            }
        }
    }
}

fn plain_response(authcid: &str, passwd: &str) -> Bytes {
    let mut v = Vec::with_capacity(authcid.len() + passwd.len() + 2);
    v.push(0);
    v.extend_from_slice(authcid.as_bytes());
    v.push(0);
    v.extend_from_slice(passwd.as_bytes());
    Bytes::from(v)
}

enum MechState {
    Simple,
    #[cfg(feature = "scram")]
    Scram(ScramClient),
}

impl MechState {
    #[cfg_attr(not(feature = "scram"), allow(unused_variables))]
    fn respond(&mut self, challenge: &[u8]) -> Result<Bytes, ConnectError> {
        match self {
            MechState::Simple => Err(ConnectError::msg(
                ErrorKind::Sasl,
                "server issued a SASL challenge for a non-challenge mechanism",
            )),
            #[cfg(feature = "scram")]
            MechState::Scram(client) => client.respond(challenge),
        }
    }

    fn verify_outcome(&mut self, _additional: Option<&[u8]>) -> Result<(), ConnectError> {
        #[cfg(feature = "scram")]
        if let (MechState::Scram(client), Some(data)) = (self, _additional) {
            return client.verify_server_final(data);
        }
        Ok(())
    }
}

/// Run the client side of SASL negotiation over `transport` (the SASL protocol
/// header must already have been exchanged). Returns once the server reports a
/// successful outcome; the caller then exchanges the AMQP protocol header.
pub async fn negotiate<S: IoStream>(
    transport: &mut FramedTransport<S>,
    profile: &SaslProfile,
    hostname: Option<&str>,
) -> Result<(), ConnectError> {
    // 1. Server advertises its mechanisms.
    let mechanisms = match transport.read_frame().await? {
        Frame {
            body: FrameBody::Sasl(SaslFrame::Mechanisms(m)),
            ..
        } => m.sasl_server_mechanisms,
        other => {
            return Err(ConnectError::msg(
                ErrorKind::Sasl,
                format!("expected sasl-mechanisms, got {other:?}"),
            ));
        }
    };
    let chosen = profile.mechanism_name();
    if !mechanisms
        .iter()
        .any(|m| m.as_str().eq_ignore_ascii_case(chosen))
    {
        return Err(ConnectError::msg(
            ErrorKind::Sasl,
            format!("server does not offer {chosen}; offers {mechanisms:?}"),
        ));
    }

    // 2. Send sasl-init.
    let (initial_response, mut state) = profile.start();
    transport
        .send_sasl(&SaslFrame::Init(SaslInit {
            mechanism: Symbol::new(chosen),
            initial_response,
            hostname: hostname.map(str::to_owned),
        }))
        .await?;

    // 3. Challenge/response until an outcome.
    loop {
        match transport.read_frame().await?.body {
            FrameBody::Sasl(SaslFrame::Challenge(c)) => {
                let response = state.respond(&c.challenge)?;
                transport
                    .send_sasl(&SaslFrame::Response(SaslResponse { response }))
                    .await?;
            }
            FrameBody::Sasl(SaslFrame::Outcome(o)) => {
                return match o.code {
                    SaslCode::Ok => {
                        state.verify_outcome(o.additional_data.as_deref())?;
                        Ok(())
                    }
                    code => Err(ConnectError::msg(
                        ErrorKind::Sasl,
                        format!("SASL authentication failed: {code:?}"),
                    )),
                };
            }
            other => {
                return Err(ConnectError::msg(
                    ErrorKind::Sasl,
                    format!("unexpected frame during SASL negotiation: {other:?}"),
                ));
            }
        }
    }
}

// ---------------------------------------------------------------------------
// SCRAM (RFC 5802), feature-gated.
// ---------------------------------------------------------------------------

#[cfg(feature = "scram")]
pub use scram::ScramMechanism;

#[cfg(feature = "scram")]
mod scram {
    use super::*;
    use base64::Engine;
    use base64::engine::general_purpose::{STANDARD, STANDARD_NO_PAD};
    use hmac::{Hmac, Mac};
    use sha1::Sha1;
    use sha2::{Digest, Sha256, Sha512};

    /// Which SCRAM hash function to use.
    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    pub enum ScramMechanism {
        /// SCRAM-SHA-1.
        Sha1,
        /// SCRAM-SHA-256.
        Sha256,
        /// SCRAM-SHA-512.
        Sha512,
    }

    impl ScramMechanism {
        /// The wire mechanism name.
        pub fn name(self) -> &'static str {
            match self {
                ScramMechanism::Sha1 => "SCRAM-SHA-1",
                ScramMechanism::Sha256 => "SCRAM-SHA-256",
                ScramMechanism::Sha512 => "SCRAM-SHA-512",
            }
        }

        fn h(self, data: &[u8]) -> Vec<u8> {
            match self {
                ScramMechanism::Sha1 => Sha1::digest(data).to_vec(),
                ScramMechanism::Sha256 => Sha256::digest(data).to_vec(),
                ScramMechanism::Sha512 => Sha512::digest(data).to_vec(),
            }
        }

        fn hmac(self, key: &[u8], msg: &[u8]) -> Vec<u8> {
            match self {
                ScramMechanism::Sha1 => {
                    let mut mac = Hmac::<Sha1>::new_from_slice(key).expect("hmac key");
                    mac.update(msg);
                    mac.finalize().into_bytes().to_vec()
                }
                ScramMechanism::Sha256 => {
                    let mut mac = Hmac::<Sha256>::new_from_slice(key).expect("hmac key");
                    mac.update(msg);
                    mac.finalize().into_bytes().to_vec()
                }
                ScramMechanism::Sha512 => {
                    let mut mac = Hmac::<Sha512>::new_from_slice(key).expect("hmac key");
                    mac.update(msg);
                    mac.finalize().into_bytes().to_vec()
                }
            }
        }

        fn pbkdf2(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
            match self {
                ScramMechanism::Sha1 => {
                    let mut out = vec![0u8; 20];
                    pbkdf2::pbkdf2_hmac::<Sha1>(password, salt, iterations, &mut out);
                    out
                }
                ScramMechanism::Sha256 => {
                    let mut out = vec![0u8; 32];
                    pbkdf2::pbkdf2_hmac::<Sha256>(password, salt, iterations, &mut out);
                    out
                }
                ScramMechanism::Sha512 => {
                    let mut out = vec![0u8; 64];
                    pbkdf2::pbkdf2_hmac::<Sha512>(password, salt, iterations, &mut out);
                    out
                }
            }
        }
    }

    /// The RFC 5802 client state machine.
    pub(super) struct ScramClient {
        mechanism: ScramMechanism,
        username: String,
        password: String,
        client_nonce: String,
        client_first_bare: String,
        server_signature: Option<Vec<u8>>,
    }

    fn gen_nonce() -> String {
        let a = uuid::Uuid::new_v4();
        let b = uuid::Uuid::new_v4();
        let mut bytes = Vec::with_capacity(32);
        bytes.extend_from_slice(a.as_bytes());
        bytes.extend_from_slice(b.as_bytes());
        STANDARD_NO_PAD.encode(bytes)
    }

    fn saslprep(s: &str) -> String {
        stringprep::saslprep(s)
            .map(|c| c.into_owned())
            .unwrap_or_else(|_| s.to_owned())
    }

    fn escape_username(s: &str) -> String {
        s.replace('=', "=3D").replace(',', "=2C")
    }

    impl ScramClient {
        pub(super) fn new(mechanism: ScramMechanism, username: &str, password: &str) -> Self {
            Self::with_nonce(mechanism, username, password, gen_nonce())
        }

        fn with_nonce(
            mechanism: ScramMechanism,
            username: &str,
            password: &str,
            client_nonce: String,
        ) -> Self {
            ScramClient {
                mechanism,
                username: saslprep(username),
                password: saslprep(password),
                client_nonce,
                client_first_bare: String::new(),
                server_signature: None,
            }
        }

        pub(super) fn client_first(&mut self) -> Bytes {
            self.client_first_bare = format!(
                "n={},r={}",
                escape_username(&self.username),
                self.client_nonce
            );
            // gs2 header "n,," = no channel binding.
            Bytes::from(format!("n,,{}", self.client_first_bare).into_bytes())
        }

        pub(super) fn respond(&mut self, challenge: &[u8]) -> Result<Bytes, ConnectError> {
            // A server-final ("v=...") may arrive as a challenge in some servers.
            if challenge.starts_with(b"v=") {
                self.verify_server_final(challenge)?;
                return Ok(Bytes::new());
            }
            let msg = std::str::from_utf8(challenge)
                .map_err(|_| sasl_err("scram server-first is not UTF-8"))?;
            let (mut nonce, mut salt_b64, mut iter_s) = (None, None, None);
            for attr in msg.split(',') {
                match attr.split_once('=') {
                    Some(("r", v)) => nonce = Some(v.to_owned()),
                    Some(("s", v)) => salt_b64 = Some(v.to_owned()),
                    Some(("i", v)) => iter_s = Some(v.to_owned()),
                    _ => {}
                }
            }
            let nonce = nonce.ok_or_else(|| sasl_err("scram server-first missing r"))?;
            let salt = STANDARD
                .decode(salt_b64.ok_or_else(|| sasl_err("scram server-first missing s"))?)
                .map_err(|_| sasl_err("scram salt is not base64"))?;
            let iterations: u32 = iter_s
                .ok_or_else(|| sasl_err("scram server-first missing i"))?
                .parse()
                .map_err(|_| sasl_err("scram iteration count is not a number"))?;
            // Reject an absurd iteration count: a hostile server could otherwise
            // pin the client in PBKDF2 (compute DoS).
            if iterations == 0 || iterations > 10_000_000 {
                return Err(sasl_err("scram iteration count out of range"));
            }
            if !nonce.starts_with(&self.client_nonce) {
                return Err(sasl_err("scram server nonce does not extend client nonce"));
            }

            let m = self.mechanism;
            let salted = m.pbkdf2(self.password.as_bytes(), &salt, iterations);
            let client_key = m.hmac(&salted, b"Client Key");
            let stored_key = m.h(&client_key);
            let server_key = m.hmac(&salted, b"Server Key");

            let client_final_no_proof = format!("c=biws,r={nonce}");
            let auth_message = format!(
                "{},{},{}",
                self.client_first_bare, msg, client_final_no_proof
            );
            let client_signature = m.hmac(&stored_key, auth_message.as_bytes());
            let proof: Vec<u8> = client_key
                .iter()
                .zip(client_signature.iter())
                .map(|(a, b)| a ^ b)
                .collect();
            self.server_signature = Some(m.hmac(&server_key, auth_message.as_bytes()));

            let client_final = format!("{client_final_no_proof},p={}", STANDARD.encode(proof));
            Ok(Bytes::from(client_final.into_bytes()))
        }

        pub(super) fn verify_server_final(&mut self, data: &[u8]) -> Result<(), ConnectError> {
            let msg = std::str::from_utf8(data)
                .map_err(|_| sasl_err("scram server-final is not UTF-8"))?;
            let v = msg
                .split(',')
                .find_map(|a| a.strip_prefix("v="))
                .ok_or_else(|| sasl_err("scram server-final missing v"))?;
            let got = STANDARD
                .decode(v)
                .map_err(|_| sasl_err("scram server signature is not base64"))?;
            match &self.server_signature {
                Some(expected) if ct_eq(expected, &got) => Ok(()),
                Some(_) => Err(sasl_err(
                    "scram server signature mismatch (server not authentic)",
                )),
                None => Err(sasl_err("scram server-final received before server-first")),
            }
        }
    }

    /// Constant-time byte-slice equality (avoids leaking the server signature
    /// via comparison timing).
    fn ct_eq(a: &[u8], b: &[u8]) -> bool {
        if a.len() != b.len() {
            return false;
        }
        let mut diff = 0u8;
        for (x, y) in a.iter().zip(b.iter()) {
            diff |= x ^ y;
        }
        diff == 0
    }

    fn sasl_err(msg: &str) -> ConnectError {
        ConnectError::msg(ErrorKind::Sasl, msg)
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        // RFC 5802 §5 worked example (SCRAM-SHA-1, user "user", password "pencil").
        #[test]
        fn rfc5802_sha1_vector() {
            let mut c = ScramClient::with_nonce(
                ScramMechanism::Sha1,
                "user",
                "pencil",
                "fyko+d2lbbFgONRv9qkxdawL".to_owned(),
            );
            let first = c.client_first();
            assert_eq!(&first[..], b"n,,n=user,r=fyko+d2lbbFgONRv9qkxdawL");

            let server_first =
                b"r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096";
            let client_final = c.respond(server_first).unwrap();
            assert_eq!(
                std::str::from_utf8(&client_final).unwrap(),
                "c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,p=v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
            );

            // server-final verifier from the RFC.
            c.verify_server_final(b"v=rmF9pqV8S7suAoZWja4dJRkFsKQ=")
                .unwrap();
            assert!(c.verify_server_final(b"v=AAAA").is_err());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transport::frame::FramedTransport;
    use crate::types::sasl::{SaslMechanisms, SaslOutcome};

    #[test]
    fn plain_response_layout() {
        let r = plain_response("user", "pw");
        assert_eq!(&r[..], b"\0user\0pw");
    }

    #[tokio::test]
    async fn plain_negotiation_succeeds() {
        let (client, server) = tokio::io::duplex(4096);
        let mut ct = FramedTransport::new(client, 1 << 16);
        let mut st = FramedTransport::new(server, 1 << 16);

        let server_task = tokio::spawn(async move {
            // offer mechanisms
            st.send_sasl(&SaslFrame::Mechanisms(SaslMechanisms {
                sasl_server_mechanisms: vec![Symbol::new("PLAIN"), Symbol::new("ANONYMOUS")],
            }))
            .await
            .unwrap();
            // expect init
            let init = st.read_frame().await.unwrap();
            assert!(matches!(init.body, FrameBody::Sasl(SaslFrame::Init(_))));
            // success
            st.send_sasl(&SaslFrame::Outcome(SaslOutcome {
                code: SaslCode::Ok,
                additional_data: None,
            }))
            .await
            .unwrap();
        });

        let profile = SaslProfile::Plain {
            authcid: "guest".into(),
            passwd: "guest".into(),
        };
        negotiate(&mut ct, &profile, Some("vhost")).await.unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_unoffered_mechanism() {
        let (client, server) = tokio::io::duplex(4096);
        let mut ct = FramedTransport::new(client, 1 << 16);
        let mut st = FramedTransport::new(server, 1 << 16);
        let _server = tokio::spawn(async move {
            st.send_sasl(&SaslFrame::Mechanisms(SaslMechanisms {
                sasl_server_mechanisms: vec![Symbol::new("EXTERNAL")],
            }))
            .await
            .unwrap();
        });
        let err = negotiate(&mut ct, &SaslProfile::Anonymous, None)
            .await
            .unwrap_err();
        assert_eq!(err.kind(), ErrorKind::Sasl);
    }
}
