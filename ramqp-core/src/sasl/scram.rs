//! SCRAM (RFC 5802) primitives, feature-gated behind `scram`.
//!
//! The hash/HMAC/PBKDF2 math and the string-preparation/nonce/comparison
//! helpers are direction-agnostic: the client state machine (`ramqp`) derives
//! proofs from a password, and the server side (`ramqp-broker`) verifies them
//! against stored keys. Both use exactly these primitives.

use base64::Engine;
use base64::engine::general_purpose::STANDARD_NO_PAD;
use hmac::{Hmac, KeyInit, Mac};
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

    /// The mechanism's hash function `H(data)`.
    pub fn h(self, data: &[u8]) -> Vec<u8> {
        match self {
            ScramMechanism::Sha1 => Sha1::digest(data).to_vec(),
            ScramMechanism::Sha256 => Sha256::digest(data).to_vec(),
            ScramMechanism::Sha512 => Sha512::digest(data).to_vec(),
        }
    }

    /// The mechanism's `HMAC(key, msg)`.
    pub fn hmac(self, key: &[u8], msg: &[u8]) -> Vec<u8> {
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

    /// `Hi(password, salt, iterations)` — PBKDF2 over the mechanism's HMAC.
    pub fn pbkdf2(self, password: &[u8], salt: &[u8], iterations: u32) -> Vec<u8> {
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

/// Generate a fresh random nonce (256 bits of UUIDv4 entropy, base64, no
/// padding — safe for the SCRAM attribute grammar).
pub fn gen_nonce() -> String {
    let a = uuid::Uuid::new_v4();
    let b = uuid::Uuid::new_v4();
    let mut bytes = Vec::with_capacity(32);
    bytes.extend_from_slice(a.as_bytes());
    bytes.extend_from_slice(b.as_bytes());
    STANDARD_NO_PAD.encode(bytes)
}

/// Apply the SASLprep stringprep profile (RFC 4013) to a username/password.
///
/// RFC 5802 mandates that a SASLprep failure abort authentication rather than
/// silently using the unprepared input, so this returns `None` on prohibited
/// input and the caller must fail the exchange.
pub fn saslprep(s: &str) -> Option<String> {
    stringprep::saslprep(s).ok().map(|c| c.into_owned())
}

/// Escape a username for the `n=` attribute (RFC 5802 §5.1: `=` → `=3D`,
/// `,` → `=2C`).
pub fn escape_username(s: &str) -> String {
    s.replace('=', "=3D").replace(',', "=2C")
}

/// Unescape a username from the `n=` attribute (inverse of
/// [`escape_username`]). Returns `None` on a malformed escape (RFC 5802
/// requires rejecting it).
pub fn unescape_username(s: &str) -> Option<String> {
    if !s.contains('=') {
        return Some(s.to_owned());
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(idx) = rest.find('=') {
        out.push_str(&rest[..idx]);
        match rest.get(idx..idx + 3) {
            Some("=3D") => out.push('='),
            Some("=2C") => out.push(','),
            _ => return None,
        }
        rest = &rest[idx + 3..];
    }
    out.push_str(rest);
    Some(out)
}

/// Constant-time byte-slice equality (avoids leaking signatures/proofs via
/// comparison timing).
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    // RFC 5802 §5 worked example (SCRAM-SHA-1, user "user", password "pencil"):
    // the primitives must reproduce the published ClientProof.
    #[test]
    fn rfc5802_sha1_primitives_vector() {
        let m = ScramMechanism::Sha1;
        let salt = base64::engine::general_purpose::STANDARD
            .decode("QSXCR+Q6sek8bf92")
            .unwrap();
        let salted = m.pbkdf2(b"pencil", &salt, 4096);
        let client_key = m.hmac(&salted, b"Client Key");
        let stored_key = m.h(&client_key);
        let auth_message = "n=user,r=fyko+d2lbbFgONRv9qkxdawL,\
             r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j,s=QSXCR+Q6sek8bf92,i=4096,\
             c=biws,r=fyko+d2lbbFgONRv9qkxdawL3rfcNHYJY1ZVvWVs7j";
        let client_signature = m.hmac(&stored_key, auth_message.as_bytes());
        let proof: Vec<u8> = client_key
            .iter()
            .zip(client_signature.iter())
            .map(|(a, b)| a ^ b)
            .collect();
        assert_eq!(
            base64::engine::general_purpose::STANDARD.encode(proof),
            "v0X8v3Bz2T0CJGbJQyF0X+HI4Ts="
        );
    }

    #[test]
    fn username_escaping_round_trips() {
        for name in ["plain", "with=eq", "with,comma", "=2C,=3D=="] {
            let escaped = escape_username(name);
            assert_eq!(unescape_username(&escaped).as_deref(), Some(name));
        }
        // Malformed escapes are rejected, not silently passed through.
        assert_eq!(unescape_username("bad=xx"), None);
        assert_eq!(unescape_username("truncated="), None);
    }

    #[test]
    fn ct_eq_basic() {
        assert!(ct_eq(b"abc", b"abc"));
        assert!(!ct_eq(b"abc", b"abd"));
        assert!(!ct_eq(b"abc", b"ab"));
    }

    #[test]
    fn saslprep_rejects_prohibited() {
        assert_eq!(saslprep("user").as_deref(), Some("user"));
        // U+0000 is prohibited by RFC 4013.
        assert!(saslprep("nul\u{0}byte").is_none());
    }
}
