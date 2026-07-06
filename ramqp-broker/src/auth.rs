//! Pluggable connection authentication.
//!
//! Phase 3 scope: ANONYMOUS and PLAIN. SCRAM verification (via
//! `ramqp_core::sasl::server::ScramServer` + a credential/verifier store)
//! arrives with the auth backend work (broker.md Phase 9).

use std::collections::HashMap;

/// Credentials presented by a connecting client.
#[derive(Debug, Clone, Copy)]
pub enum Credentials<'a> {
    /// The ANONYMOUS mechanism (or a bare-AMQP connection with no SASL layer).
    Anonymous,
    /// The PLAIN mechanism.
    Plain {
        /// Authentication identity (username).
        authcid: &'a str,
        /// Password.
        passwd: &'a str,
    },
}

/// Verifies connection credentials.
///
/// Synchronous for now: Phase 3 backends are in-memory. When a database/LDAP
/// backend lands (Phase 9) this becomes async.
pub trait Authenticator: Send + Sync + 'static {
    /// The SASL mechanisms to advertise, in preference order.
    fn mechanisms(&self) -> &[&'static str];
    /// Whether the presented credentials are valid.
    fn verify(&self, credentials: Credentials<'_>) -> bool;
    /// Whether a connection speaking bare AMQP (no SASL layer) is allowed.
    /// Defaults to whatever ANONYMOUS verification says.
    fn allow_unauthenticated(&self) -> bool {
        self.verify(Credentials::Anonymous)
    }
}

/// Accepts every connection (development / trusted-network use).
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAll;

impl Authenticator for AllowAll {
    fn mechanisms(&self) -> &[&'static str] {
        &["ANONYMOUS", "PLAIN"]
    }

    fn verify(&self, _credentials: Credentials<'_>) -> bool {
        true
    }
}

/// PLAIN authentication against a static in-memory user table.
#[derive(Debug, Default)]
pub struct StaticPlain {
    users: HashMap<String, String>,
}

impl StaticPlain {
    /// Create an empty user table.
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a user (builder-style).
    pub fn with_user(mut self, username: impl Into<String>, password: impl Into<String>) -> Self {
        self.users.insert(username.into(), password.into());
        self
    }
}

impl Authenticator for StaticPlain {
    fn mechanisms(&self) -> &[&'static str] {
        &["PLAIN"]
    }

    fn verify(&self, credentials: Credentials<'_>) -> bool {
        match credentials {
            Credentials::Anonymous => false,
            Credentials::Plain { authcid, passwd } => {
                // Normalize timing between "no such user" and "wrong password":
                // always perform a comparison (against a placeholder for a
                // missing user) so a fast negative doesn't reveal that the
                // username exists. StaticPlain stores plaintext and is a
                // dev/testing helper — for production use an Authenticator that
                // verifies against salted hashes (e.g. via ScramServer).
                const PLACEHOLDER: &str = "\0no-such-user-placeholder\0";
                let expected = self.users.get(authcid).map(String::as_str);
                let ok = ct_str_eq(expected.unwrap_or(PLACEHOLDER), passwd);
                expected.is_some() && ok
            }
        }
    }
}

/// Constant-time string comparison (no early exit on the first differing
/// byte, so password checks don't leak match length via timing).
fn ct_str_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
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

    #[test]
    fn allow_all_accepts_everything() {
        assert!(AllowAll.verify(Credentials::Anonymous));
        assert!(AllowAll.verify(Credentials::Plain {
            authcid: "x",
            passwd: "y"
        }));
        assert!(AllowAll.allow_unauthenticated());
    }

    #[test]
    fn static_plain_checks_users() {
        let auth = StaticPlain::new().with_user("alice", "secret");
        assert!(auth.verify(Credentials::Plain {
            authcid: "alice",
            passwd: "secret"
        }));
        assert!(!auth.verify(Credentials::Plain {
            authcid: "alice",
            passwd: "wrong"
        }));
        assert!(!auth.verify(Credentials::Plain {
            authcid: "bob",
            passwd: "secret"
        }));
        assert!(!auth.verify(Credentials::Anonymous));
        assert!(!auth.allow_unauthenticated());
    }
}
