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
    /// May `username` / `password` administer the broker through the
    /// management API (HTTP Basic)? **The default allows no one.**
    fn management_login(&self, username: &str, password: &str) -> bool {
        let _ = (username, password);
        false
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

/// Run a password check (PBKDF2 — milliseconds of CPU) on the blocking
/// pool, at most one per CPU at a time, so floods of PLAIN / Basic logins
/// queue up instead of pinning the async workers that carry messages.
pub(crate) async fn run_password_check<T: Send + 'static>(
    check: impl FnOnce() -> Option<T> + Send + 'static,
) -> Option<T> {
    use std::sync::OnceLock;
    static PERMITS: OnceLock<std::sync::Arc<tokio::sync::Semaphore>> = OnceLock::new();
    let permits = PERMITS.get_or_init(|| {
        let n = std::thread::available_parallelism().map_or(2, |n| n.get());
        std::sync::Arc::new(tokio::sync::Semaphore::new(n))
    });
    let _permit = permits.clone().acquire_owned().await.ok()?;
    tokio::task::spawn_blocking(check).await.ok().flatten()
}

/// Highest PBKDF2 iteration count a stored verifier may carry (a typo'd
/// verifier must not turn every login into seconds of CPU).
pub const MAX_ITERATIONS: u32 = 1_000_000;

/// The identity anonymous connections are matched as in
/// [`Permission::user`] (a configured user may not take this name).
pub const ANONYMOUS_USER: &str = "anonymous";

/// One access grant for [`StaticUsers`]: `user` may perform the allowed
/// operations on addresses matching `address` within `vhost`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Permission {
    /// The username, [`ANONYMOUS_USER`] for anonymous connections, or `*`
    /// for everyone (anonymous included).
    pub user: String,
    /// The vhost (empty = the default vhost), or `*` for every vhost.
    pub vhost: String,
    /// An address glob matched against the full attach address
    /// (`/queues/orders`, `/exchanges/events/order.created`, ...): `*`
    /// matches any run of characters (including `/`), `?` exactly one.
    pub address: String,
    /// May publish (producer attach).
    pub send: bool,
    /// May consume (consumer attach).
    pub receive: bool,
}

impl Permission {
    /// A grant of `send`/`receive` on `address` in `vhost` to `user`.
    pub fn new(
        user: impl Into<String>,
        vhost: impl Into<String>,
        address: impl Into<String>,
        send: bool,
        receive: bool,
    ) -> Self {
        Permission {
            user: user.into(),
            vhost: vhost.into(),
            address: address.into(),
            send,
            receive,
        }
    }

    fn matches_principal(&self, user: &str, vhost: &str) -> bool {
        (self.user == "*" || self.user == user) && (self.vhost == "*" || self.vhost == vhost)
    }

    fn allows(&self, operation: Operation) -> bool {
        match operation {
            Operation::Send => self.send,
            Operation::Receive => self.receive,
        }
    }
}

/// Glob match: `*` = any run of characters (including none), `?` = exactly
/// one character; everything else literal. Iterative with single-star
/// backtracking — linear in practice, no recursion blow-up.
pub(crate) fn glob_match(pattern: &str, text: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    let (mut pi, mut ti) = (0usize, 0usize);
    let mut star: Option<(usize, usize)> = None;
    while ti < t.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == t[ti]) {
            pi += 1;
            ti += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some((pi, ti));
            pi += 1;
        } else if let Some((sp, st)) = star {
            pi = sp + 1;
            ti = st + 1;
            star = Some((sp, st + 1));
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Parse an RFC 5803 SCRAM verifier string:
/// `SCRAM-SHA-256$<iterations>:<base64 salt>$<base64 StoredKey>:<base64 ServerKey>`
/// (`SCRAM-SHA-1` / `SCRAM-SHA-512` likewise). The same format PostgreSQL
/// stores; produce one with `ramqp-brokerd hash-password`.
pub fn parse_scram_verifier(s: &str) -> Result<(ScramMechanism, ScramVerifier), String> {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    let s = s.trim();
    let (mech, rest) = s
        .split_once('$')
        .ok_or("expected <mechanism>$<iterations>:<salt>$<stored-key>:<server-key>")?;
    let mechanism = match mech.to_ascii_uppercase().as_str() {
        "SCRAM-SHA-1" => ScramMechanism::Sha1,
        "SCRAM-SHA-256" => ScramMechanism::Sha256,
        "SCRAM-SHA-512" => ScramMechanism::Sha512,
        other => return Err(format!("unknown SCRAM mechanism {other:?}")),
    };
    let (params, keys) = rest
        .split_once('$')
        .ok_or("missing `$<stored-key>:<server-key>`")?;
    let (iterations, salt) = params
        .split_once(':')
        .ok_or("missing `<iterations>:<salt>`")?;
    let (stored, server) = keys
        .split_once(':')
        .ok_or("missing `<stored-key>:<server-key>`")?;
    let iterations: u32 = iterations
        .parse()
        .map_err(|_| format!("bad iteration count {iterations:?}"))?;
    if iterations < 4096 {
        return Err(format!(
            "iteration count {iterations} is below the RFC 7677 minimum of 4096"
        ));
    }
    if iterations > MAX_ITERATIONS {
        return Err(format!(
            "iteration count {iterations} is above the maximum of {MAX_ITERATIONS}"
        ));
    }
    let decode = |what: &str, v: &str| {
        STANDARD
            .decode(v)
            .map_err(|e| format!("bad base64 in {what}: {e}"))
    };
    let salt = decode("salt", salt)?;
    let stored_key = decode("stored key", stored)?;
    let server_key = decode("server key", server)?;
    let want = mechanism.h(b"").len();
    if stored_key.len() != want || server_key.len() != want {
        return Err(format!(
            "{} keys must be {want} bytes (got {} / {})",
            mechanism.name(),
            stored_key.len(),
            server_key.len()
        ));
    }
    if salt.len() < 8 {
        return Err("salt must be at least 8 bytes".to_owned());
    }
    Ok((
        mechanism,
        ScramVerifier {
            salt,
            iterations,
            stored_key,
            server_key,
        },
    ))
}

/// Format a verifier as an RFC 5803 string (inverse of
/// [`parse_scram_verifier`]).
pub fn format_scram_verifier(mechanism: ScramMechanism, verifier: &ScramVerifier) -> String {
    use base64::Engine;
    use base64::engine::general_purpose::STANDARD;
    format!(
        "{}${}:{}${}:{}",
        mechanism.name(),
        verifier.iterations,
        STANDARD.encode(&verifier.salt),
        STANDARD.encode(&verifier.stored_key),
        STANDARD.encode(&verifier.server_key),
    )
}

/// Derive a fresh SCRAM verifier (random 16-byte salt) from a password.
/// `None` if SASLprep rejects the password.
pub fn derive_scram_verifier(
    mechanism: ScramMechanism,
    password: &str,
    iterations: u32,
) -> Option<ScramVerifier> {
    let salt = ramqp_core::sasl::scram::gen_nonce().into_bytes();
    ScramVerifier::derive(mechanism, password, &salt[..16], iterations)
}

/// The production authenticator behind the daemon's config file: users held
/// as **SCRAM verifiers only** (salted + iterated, never plaintext), with
/// per-user × vhost × address-glob × operation [`Permission`]s.
///
/// - **SCRAM-SHA-1/256/512** is offered for each mechanism some user has a
///   verifier for.
/// - **PLAIN** (when enabled) is checked against the same verifier: the
///   presented password is run through the verifier's PBKDF2 and compared to
///   the StoredKey — so PLAIN-only clients work with no plaintext at rest.
///   PLAIN sends the password in the clear; pair it with TLS off-host.
/// - **ANONYMOUS** only when explicitly allowed; anonymous connections match
///   permissions for [`ANONYMOUS_USER`] (or `*`).
///
/// Authorization is deny-by-default: an attach is allowed only when some
/// permission matches the identity, vhost, address, and operation. The
/// transaction coordinator (`$coordinator`) is allowed to anyone holding a
/// `send` permission in the vhost.
#[derive(Debug, Default)]
pub struct StaticUsers {
    /// username → verifiers (one per mechanism).
    users: HashMap<String, Vec<(ScramMechanism, ScramVerifier)>>,
    admins: std::collections::HashSet<String>,
    permissions: Vec<Permission>,
    allow_plain: bool,
    allow_anonymous: bool,
    mechs: Vec<&'static str>,
}

impl StaticUsers {
    /// An empty directory: PLAIN enabled, anonymous refused.
    pub fn new() -> Self {
        let mut s = StaticUsers {
            allow_plain: true,
            ..Default::default()
        };
        s.recompute_mechs();
        s
    }

    fn recompute_mechs(&mut self) {
        let has = |m: ScramMechanism| {
            self.users
                .values()
                .any(|vs| vs.iter().any(|(vm, _)| *vm == m))
        };
        let mut mechs = Vec::new();
        // Strongest first: clients pick the first mutually supported one.
        for m in [
            ScramMechanism::Sha512,
            ScramMechanism::Sha256,
            ScramMechanism::Sha1,
        ] {
            if has(m) {
                mechs.push(m.name());
            }
        }
        if self.allow_plain && !self.users.is_empty() {
            mechs.push("PLAIN");
        }
        if self.allow_anonymous {
            mechs.push("ANONYMOUS");
        }
        self.mechs = mechs;
    }

    /// Add (or extend) a user with a stored verifier (builder-style).
    /// Panics if `username` is [`ANONYMOUS_USER`] (reserved) — config
    /// loading validates this first.
    pub fn with_verifier(
        mut self,
        username: impl Into<String>,
        mechanism: ScramMechanism,
        verifier: ScramVerifier,
    ) -> Self {
        let username = username.into();
        assert_ne!(
            username, ANONYMOUS_USER,
            "`anonymous` is a reserved user name"
        );
        let entry = self.users.entry(username).or_default();
        entry.retain(|(m, _)| *m != mechanism);
        entry.push((mechanism, verifier));
        self.recompute_mechs();
        self
    }

    /// Add a user from a password, derived into a SCRAM-SHA-256 verifier
    /// immediately (the password is not kept). Panics on a password
    /// SASLprep prohibits.
    pub fn with_password(self, username: impl Into<String>, password: &str) -> Self {
        let verifier =
            derive_scram_verifier(ScramMechanism::Sha256, password, StaticScram::ITERATIONS)
                .expect("password must survive SASLprep");
        self.with_verifier(username, ScramMechanism::Sha256, verifier)
    }

    /// Mark `username` as a management-API administrator.
    pub fn with_admin(mut self, username: impl Into<String>) -> Self {
        self.admins.insert(username.into());
        self
    }

    /// Add a permission grant.
    pub fn with_permission(mut self, permission: Permission) -> Self {
        self.permissions.push(permission);
        self
    }

    /// Offer PLAIN (checked against the SCRAM verifier). Default `true`.
    pub fn allow_plain(mut self, allow: bool) -> Self {
        self.allow_plain = allow;
        self.recompute_mechs();
        self
    }

    /// Offer ANONYMOUS (and accept bare-AMQP connections). Default `false`.
    pub fn allow_anonymous(mut self, allow: bool) -> Self {
        self.allow_anonymous = allow;
        self.recompute_mechs();
        self
    }

    /// Number of configured users.
    pub fn user_count(&self) -> usize {
        self.users.len()
    }

    /// Whether `username` is an administrator.
    pub fn is_admin(&self, username: &str) -> bool {
        self.admins.contains(username)
    }

    /// Check a username/password pair against the stored verifier (the
    /// PLAIN path; also used for management-API Basic auth). Runs the
    /// PBKDF2 even for unknown users so timing doesn't reveal existence.
    pub fn check_password(&self, username: &str, password: &str) -> bool {
        let found = self.users.get(username).and_then(|vs| {
            vs.iter()
                .find(|(m, _)| *m == ScramMechanism::Sha256)
                .or_else(|| vs.first())
        });
        let decoy;
        let (mechanism, verifier) = match found {
            Some((m, v)) => (*m, v),
            None => {
                decoy = self.decoy();
                (decoy.0, &decoy.1)
            }
        };
        let Some(prepared) = ramqp_core::sasl::scram::saslprep(password) else {
            return false;
        };
        let salted = mechanism.pbkdf2(prepared.as_bytes(), &verifier.salt, verifier.iterations);
        let client_key = mechanism.hmac(&salted, b"Client Key");
        let stored = mechanism.h(&client_key);
        let ok = ct_bytes_eq(&stored, &verifier.stored_key);
        found.is_some() && ok
    }

    /// The verifier an unknown username is checked against: the same
    /// mechanism and iteration count most real users have, so the check
    /// costs the same and timing does not reveal whether a user exists.
    fn decoy(&self) -> (ScramMechanism, ScramVerifier) {
        let tag_of = |m: ScramMechanism| match m {
            ScramMechanism::Sha1 => 1u8,
            ScramMechanism::Sha256 => 2,
            ScramMechanism::Sha512 => 3,
        };
        let mut counts: HashMap<(u8, u32), (usize, ScramMechanism)> = HashMap::new();
        for vs in self.users.values() {
            let (m, v) = vs
                .iter()
                .find(|(m, _)| *m == ScramMechanism::Sha256)
                .or_else(|| vs.first())
                .expect("a user has at least one verifier");
            counts
                .entry((tag_of(*m), v.iterations))
                .or_insert((0, *m))
                .0 += 1;
        }
        let (mechanism, iterations) = counts
            .into_iter()
            .max_by_key(|((_, it), (n, _))| (*n, *it))
            .map(|((_, it), (_, m))| (m, it))
            .unwrap_or((ScramMechanism::Sha256, StaticScram::ITERATIONS));
        // Derived once per (mechanism, iterations) and cached.
        use std::sync::{Mutex, OnceLock};
        static DECOYS: OnceLock<Mutex<HashMap<(u8, u32), ScramVerifier>>> = OnceLock::new();
        let tag = tag_of(mechanism);
        let mut cache = DECOYS
            .get_or_init(|| Mutex::new(HashMap::new()))
            .lock()
            .expect("decoy lock");
        let v = cache
            .entry((tag, iterations))
            .or_insert_with(|| {
                derive_scram_verifier(mechanism, "decoy-password-never-matches", iterations)
                    .expect("decoy derives")
            })
            .clone();
        (mechanism, v)
    }
}

/// Constant-time byte comparison (lengths are public: fixed per mechanism).
fn ct_bytes_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

impl Authenticator for StaticUsers {
    fn mechanisms(&self) -> &[&'static str] {
        &self.mechs
    }

    fn verify(&self, credentials: Credentials<'_>) -> bool {
        match credentials {
            Credentials::Anonymous => self.allow_anonymous,
            Credentials::Plain { authcid, passwd } => {
                self.allow_plain && self.check_password(authcid, passwd)
            }
        }
    }

    fn scram_verifier(&self, mechanism: ScramMechanism, username: &str) -> Option<ScramVerifier> {
        self.users
            .get(username)?
            .iter()
            .find(|(m, _)| *m == mechanism)
            .map(|(_, v)| v.clone())
    }

    fn authorize(
        &self,
        identity: Option<&str>,
        vhost: &str,
        address: &str,
        operation: Operation,
    ) -> bool {
        let user = identity.unwrap_or(ANONYMOUS_USER);
        let mut grants = self
            .permissions
            .iter()
            .filter(|p| p.matches_principal(user, vhost));
        // The transaction coordinator's pseudo-address (queue names may not
        // start with `$`, so no queue can borrow this shortcut).
        if address == "$coordinator" && operation == Operation::Send {
            return grants.any(|p| p.send);
        }
        grants.any(|p| p.allows(operation) && glob_match(&p.address, address))
    }

    fn management_login(&self, username: &str, password: &str) -> bool {
        // Always run the PBKDF2 (timing does not reveal admin status).
        let ok = self.check_password(username, password);
        ok && self.is_admin(username)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn glob_matching() {
        assert!(glob_match("*", ""));
        assert!(glob_match("*", "/queues/a"));
        assert!(glob_match("/queues/*", "/queues/orders"));
        assert!(glob_match("/queues/orders.*", "/queues/orders.eu"));
        assert!(!glob_match("/queues/orders.*", "/queues/billing"));
        assert!(glob_match("/exchanges/events/*", "/exchanges/events/a.b.c"));
        assert!(glob_match("/q?eues/x", "/queues/x"));
        assert!(!glob_match("/queues/x", "/queues/xy"));
        assert!(glob_match("*/x*y", "a/b/xzzy"));
    }

    #[test]
    fn rfc5803_round_trip_and_validation() {
        let v = derive_scram_verifier(ScramMechanism::Sha256, "pencil", 4096).unwrap();
        let s = format_scram_verifier(ScramMechanism::Sha256, &v);
        assert!(s.starts_with("SCRAM-SHA-256$4096:"));
        let (m, back) = parse_scram_verifier(&s).unwrap();
        assert_eq!(m, ScramMechanism::Sha256);
        assert_eq!(back.stored_key, v.stored_key);
        assert_eq!(back.server_key, v.server_key);
        assert_eq!(back.salt, v.salt);
        // Too few iterations, wrong key length, junk.
        assert!(parse_scram_verifier(&s.replace("$4096:", "$1000:")).is_err());
        assert!(parse_scram_verifier(&s.replace("$4096:", "$1000001:")).is_err());
        assert!(parse_scram_verifier("SCRAM-SHA-1$4096:AAAAAAAAAAA=$AA==:AA==").is_err());
        assert!(parse_scram_verifier("nonsense").is_err());
        assert!(parse_scram_verifier("MD5$4096:a$b:c").is_err());
    }

    #[test]
    fn static_users_plain_checks_against_verifier() {
        let users = StaticUsers::new().with_password("alice", "s3cret");
        assert!(users.verify(Credentials::Plain {
            authcid: "alice",
            passwd: "s3cret"
        }));
        assert!(!users.verify(Credentials::Plain {
            authcid: "alice",
            passwd: "nope"
        }));
        assert!(!users.verify(Credentials::Plain {
            authcid: "mallory",
            passwd: "s3cret"
        }));
        assert!(!users.verify(Credentials::Anonymous));
        assert_eq!(users.mechanisms(), &["SCRAM-SHA-256", "PLAIN"]);
        let no_plain = StaticUsers::new()
            .with_password("alice", "s3cret")
            .allow_plain(false);
        assert!(!no_plain.verify(Credentials::Plain {
            authcid: "alice",
            passwd: "s3cret"
        }));
        assert_eq!(no_plain.mechanisms(), &["SCRAM-SHA-256"]);
        assert!(
            no_plain
                .scram_verifier(ScramMechanism::Sha256, "alice")
                .is_some()
        );
        assert!(
            no_plain
                .scram_verifier(ScramMechanism::Sha512, "alice")
                .is_none()
        );
    }

    #[test]
    fn static_users_permissions_deny_by_default() {
        let users = StaticUsers::new()
            .with_password("svc", "pw")
            .with_permission(Permission::new("svc", "", "/queues/jobs.*", true, false))
            .with_permission(Permission::new("svc", "", "/queues/replies", false, true))
            .with_permission(Permission::new(
                "*",
                "tenant",
                "/queues/public",
                false,
                true,
            ))
            .allow_anonymous(true);
        let a = |id: Option<&str>, vh: &str, addr: &str, op| users.authorize(id, vh, addr, op);
        assert!(a(Some("svc"), "", "/queues/jobs.email", Operation::Send));
        assert!(!a(
            Some("svc"),
            "",
            "/queues/jobs.email",
            Operation::Receive
        ));
        assert!(a(Some("svc"), "", "/queues/replies", Operation::Receive));
        assert!(!a(
            Some("svc"),
            "other",
            "/queues/jobs.email",
            Operation::Send
        ));
        assert!(!a(Some("svc"), "", "/queues/secret", Operation::Send));
        // `*` user covers anonymous too; nothing else is granted to it.
        assert!(a(None, "tenant", "/queues/public", Operation::Receive));
        assert!(!a(None, "", "/queues/jobs.email", Operation::Send));
        // Coordinator: any send grant in the vhost.
        assert!(a(Some("svc"), "", "$coordinator", Operation::Send));
        assert!(!a(None, "tenant", "$coordinator", Operation::Send));
        assert!(users.verify(Credentials::Anonymous));
        assert_eq!(users.mechanisms(), &["SCRAM-SHA-256", "PLAIN", "ANONYMOUS"]);
    }

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
