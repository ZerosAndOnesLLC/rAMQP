//! Pluggable connection authentication and authorization.
//!
//! Mechanisms: ANONYMOUS, PLAIN, and SCRAM-SHA-1/-256/-512 (via
//! `ramqp_core::sasl::server::ScramServer` against verifier-based storage —
//! no plaintext at rest). Authorization is per-address: every link attach
//! asks [`Authenticator::authorize`] before a queue is resolved.

use std::collections::HashMap;

use ramqp_core::sasl::scram::ScramMechanism;
use ramqp_core::sasl::server::ScramVerifier;

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

/// What a link wants to do with an address (authorization checks).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operation {
    /// Publish into the address (a producer attach — or the transaction
    /// coordinator, whose pseudo-address is `$coordinator`).
    Send,
    /// Consume from the address (a consumer attach).
    Receive,
}

/// Verifies connection credentials and authorizes link attaches.
///
/// Synchronous for now: the built-in backends are in-memory. When a
/// database/LDAP backend lands this becomes async.
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
    /// The stored SCRAM verifier for `username` (advertising `SCRAM-*`
    /// mechanisms requires implementing this). The username arrives
    /// SASLprep-prepared.
    fn scram_verifier(&self, mechanism: ScramMechanism, username: &str) -> Option<ScramVerifier> {
        let _ = (mechanism, username);
        None
    }
    /// May `identity` (the authenticated username; `None` for anonymous)
    /// perform `operation` on `address` within `vhost`? Called at link
    /// attach — never per message.
    ///
    /// **The default allows everything.** Authentication alone provides no
    /// access control: the vhost is client-asserted (`open.hostname`), so
    /// without an `authorize` override (or vhost grants on the static
    /// authenticators) any authenticated user can select any vhost and any
    /// queue.
    fn authorize(
        &self,
        identity: Option<&str>,
        vhost: &str,
        address: &str,
        operation: Operation,
    ) -> bool {
        let _ = (identity, vhost, address, operation);
        true
    }
}

/// Per-user vhost grants shared by the static authenticators: a user with a
/// grant list may only attach within those vhosts; a user without one may
/// use any vhost (the backward-compatible default — grant every user for
/// real tenant isolation).
#[derive(Debug, Default)]
struct VhostGrants {
    grants: HashMap<String, Vec<String>>,
}

impl VhostGrants {
    fn grant(&mut self, username: impl Into<String>, vhosts: &[&str]) {
        self.grants.insert(
            username.into(),
            vhosts.iter().map(|v| (*v).to_owned()).collect(),
        );
    }

    fn allows(&self, identity: Option<&str>, vhost: &str) -> bool {
        match identity.and_then(|u| self.grants.get(u)) {
            Some(allowed) => allowed.iter().any(|v| v == vhost),
            None => true,
        }
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
///
/// **Development / testing helper only.** Passwords are held in plaintext in
/// memory, and while [`verify`](StaticPlain::verify) normalizes the
/// present-vs-absent user timing, the constant-time compare short-circuits
/// on a length mismatch, so it can still leak the password *length* via
/// timing (LOW-17). For production, use an authenticator that verifies
/// against salted, iterated hashes — [`StaticScram`] (no plaintext at rest)
/// or a custom [`Authenticator`] backed by your credential store.
#[derive(Debug, Default)]
pub struct StaticPlain {
    users: HashMap<String, String>,
    vhosts: VhostGrants,
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

    /// Restrict `username` to these vhosts (builder-style). Users without a
    /// grant may use ANY vhost — grant every user for tenant isolation.
    pub fn with_user_vhosts(mut self, username: impl Into<String>, vhosts: &[&str]) -> Self {
        self.vhosts.grant(username, vhosts);
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

    fn authorize(
        &self,
        identity: Option<&str>,
        vhost: &str,
        _address: &str,
        _operation: Operation,
    ) -> bool {
        self.vhosts.allows(identity, vhost)
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

/// SCRAM authentication against a static, verifier-based user table
/// (passwords are salted + iterated at construction; no plaintext at rest).
#[derive(Debug, Default)]
pub struct StaticScram {
    users: HashMap<String, ScramVerifier>,
    vhosts: VhostGrants,
}

impl StaticScram {
    /// PBKDF2 iteration count for derived verifiers (RFC 7677 recommends
    /// at least 4096; this default trades a few ms at provisioning for a
    /// real brute-force cost).
    pub const ITERATIONS: u32 = 8192;

    /// An empty user table (SCRAM-SHA-256).
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a user (builder-style). The password is SASLprep-prepared and
    /// derived into a salted verifier immediately; panics on a password
    /// SASLprep prohibits (provisioning-time input).
    pub fn with_user(mut self, username: impl Into<String>, password: &str) -> Self {
        // A fresh random salt per user (nonce entropy re-used as salt).
        let salt = ramqp_core::sasl::scram::gen_nonce().into_bytes();
        let verifier = ScramVerifier::derive(
            ScramMechanism::Sha256,
            password,
            &salt[..16],
            Self::ITERATIONS,
        )
        .expect("password must survive SASLprep");
        self.users.insert(username.into(), verifier);
        self
    }

    /// Restrict `username` to these vhosts (builder-style). Users without a
    /// grant may use ANY vhost — grant every user for tenant isolation.
    pub fn with_user_vhosts(mut self, username: impl Into<String>, vhosts: &[&str]) -> Self {
        self.vhosts.grant(username, vhosts);
        self
    }
}

impl Authenticator for StaticScram {
    fn mechanisms(&self) -> &[&'static str] {
        &["SCRAM-SHA-256"]
    }

    fn verify(&self, _credentials: Credentials<'_>) -> bool {
        false // only SCRAM; PLAIN/ANONYMOUS are refused
    }

    fn scram_verifier(&self, mechanism: ScramMechanism, username: &str) -> Option<ScramVerifier> {
        (mechanism == ScramMechanism::Sha256)
            .then(|| self.users.get(username).cloned())
            .flatten()
    }

    fn authorize(
        &self,
        identity: Option<&str>,
        vhost: &str,
        _address: &str,
        _operation: Operation,
    ) -> bool {
        self.vhosts.allows(identity, vhost)
    }
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

    /// HIGH-10 (issue #19): the static authenticators can bind users to
    /// vhosts, so authentication comes with usable access control instead
    /// of a client-asserted vhost.
    #[test]
    fn vhost_grants_gate_authorization() {
        let auth = StaticPlain::new()
            .with_user("alice", "pw")
            .with_user("bob", "pw")
            .with_user_vhosts("alice", &["tenant-a"]);
        // Granted vhost: allowed; any other: refused.
        assert!(auth.authorize(Some("alice"), "tenant-a", "/queues/x", Operation::Send));
        assert!(!auth.authorize(Some("alice"), "tenant-b", "/queues/x", Operation::Send));
        assert!(!auth.authorize(Some("alice"), "", "/queues/x", Operation::Receive));
        // No grant recorded: any vhost (the backward-compatible default).
        assert!(auth.authorize(Some("bob"), "tenant-b", "/queues/x", Operation::Send));

        let scram = StaticScram::new().with_user_vhosts("carol", &["tenant-c", ""]);
        assert!(scram.authorize(Some("carol"), "", "/queues/x", Operation::Send));
        assert!(scram.authorize(Some("carol"), "tenant-c", "/queues/x", Operation::Send));
        assert!(!scram.authorize(Some("carol"), "tenant-a", "/queues/x", Operation::Send));
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
