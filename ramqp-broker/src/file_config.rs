//! The daemon's configuration file (`ramqp-brokerd --config <path>` /
//! `RAMQP_CONFIG`): listeners, clustering, limits, users, permissions,
//! policies, and pre-declared topology, in TOML.
//!
//! ```toml
//! listen = "0.0.0.0:5672"
//! data_dir = "/var/lib/ramqp"
//! management_listen = "0.0.0.0:15692"
//! management_token_file = "/etc/ramqp/mgmt-token"   # bearer token for PUT/DELETE
//! management_auth_reads = false   # true: GETs need credentials too
//! allow_anonymous = false      # default
//! allow_plain = true           # PLAIN checked against the SCRAM verifier
//!
//! [limits]
//! max_connections = 16384
//! max_queue_depth = 1000000
//!
//! [cluster]
//! node_id = 1
//! listen = "0.0.0.0:7472"
//! seeds = ["1=broker-0.broker:7472", "2=broker-1.broker:7472", "3=broker-2.broker:7472"]
//! tls = true                      # mutual TLS on the fabric ([tls] cert + CA)
//!
//! [tls]                           # needs a `tls` or `fips` build
//! listen = "0.0.0.0:5671"         # amqps listener (the plain one stays)
//! cert_file = "/etc/ramqp/tls/tls.crt"   # reloaded when it changes
//! key_file = "/etc/ramqp/tls/tls.key"
//! client_ca_file = "/etc/ramqp/tls/ca.crt"   # optional: require client certs
//! management = true               # management endpoint over TLS too
//!
//! [[users]]
//! name = "orders"
//! verifier_file = "/etc/ramqp/users/orders.scram"   # RFC 5803 line(s)
//!
//! [[users]]
//! name = "ops"
//! password_file = "/etc/ramqp/users/ops"   # derived into a verifier at load
//! admin = true
//!
//! [[permissions]]
//! user = "orders"
//! vhost = ""                     # default vhost; "*" = every vhost
//! address = "/queues/orders.*"   # glob over the attach address
//! operations = ["send", "receive"]
//!
//! [[policies]]
//! prefix = "orders."
//! message_ttl_ms = 600000
//! max_length = 100000
//! overflow = "reject-publish"    # or "drop-head"
//! dead_letter = "/quorum/dead"
//!
//! [[queues]]
//! address = "/quorum/orders.created"
//!
//! [[exchanges]]
//! name = "events"
//! type = "topic"                 # fanout | direct | topic
//! alternate = "/queues/unroutable"   # optional
//!
//! [[bindings]]
//! exchange = "events"
//! queue = "/quorum/orders.created"
//! key = "order.#"
//! ```
//!
//! Secrets never appear inline: users reference a **verifier file** (RFC
//! 5803 `SCRAM-SHA-256$<iter>:<salt>$<stored>:<server>`, one per line —
//! produce one with `ramqp-brokerd hash-password`) or a **password file**
//! (derived into a SCRAM-SHA-256 verifier at load, then discarded). Both fit
//! a mounted Kubernetes Secret. Relative paths resolve against the config
//! file's directory.

use std::path::{Path, PathBuf};

use serde::Deserialize;

use crate::auth::{
    ANONYMOUS_USER, Permission, StaticScram, StaticUsers, derive_scram_verifier,
    parse_scram_verifier,
};
use crate::config::{BrokerConfig, ClusterMemberConfig, OverflowBehavior, QueuePolicy};
use ramqp_core::sasl::scram::ScramMechanism;

/// The parsed configuration file. Every field is optional; command-line
/// flags and environment variables override what is set here.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct FileConfig {
    /// AMQP listen address.
    pub listen: Option<String>,
    /// On-disk state directory (durable queues, Raft logs, definitions).
    pub data_dir: Option<PathBuf>,
    /// Management/metrics HTTP listen address.
    pub management_listen: Option<String>,
    /// File holding the management API bearer token.
    pub management_token_file: Option<PathBuf>,
    /// Require credentials for management reads too (default false).
    pub management_auth_reads: Option<bool>,
    /// Offer ANONYMOUS / accept unauthenticated connections. Required to
    /// start on a non-loopback listener with no users configured.
    pub allow_anonymous: Option<bool>,
    /// Offer PLAIN (checked against the stored SCRAM verifier). Default on.
    pub allow_plain: Option<bool>,
    /// Cluster membership.
    pub cluster: Option<ClusterSection>,
    /// Server TLS (needs a `tls` or `fips` build).
    pub tls: Option<TlsSection>,
    /// Broker resource limits.
    pub limits: Option<LimitsSection>,
    /// Users.
    #[serde(default)]
    pub users: Vec<UserEntry>,
    /// Access grants.
    #[serde(default)]
    pub permissions: Vec<PermissionEntry>,
    /// Queue policies, first match wins (file order).
    #[serde(default)]
    pub policies: Vec<PolicyEntry>,
    /// Queues declared at startup (idempotent across restarts).
    #[serde(default)]
    pub queues: Vec<QueueEntry>,
    /// Exchanges declared at startup.
    #[serde(default)]
    pub exchanges: Vec<ExchangeEntry>,
    /// Exchange → queue bindings created at startup.
    #[serde(default)]
    pub bindings: Vec<BindingEntry>,
    /// The directory relative paths resolve against (the file's own).
    #[serde(skip)]
    base_dir: PathBuf,
}

/// `[cluster]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ClusterSection {
    /// This node's id (must appear in `seeds`).
    pub node_id: u64,
    /// Fabric listen address.
    pub listen: String,
    /// Founding members as `"<id>=<host:port>"`.
    pub seeds: Vec<String>,
    /// Replica count for new quorum queues (default 3).
    pub replicas: Option<u8>,
    /// Mutual TLS on the fabric with the `[tls]` certificate and its
    /// `client_ca_file` as the cluster CA.
    #[serde(default)]
    pub tls: bool,
}

/// `[tls]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct TlsSection {
    /// The `amqps` listener (e.g. `0.0.0.0:5671`).
    pub listen: Option<String>,
    /// PEM certificate chain.
    pub cert_file: PathBuf,
    /// PEM private key.
    pub key_file: PathBuf,
    /// PEM CA bundle: clients must present a certificate from it (mTLS).
    pub client_ca_file: Option<PathBuf>,
    /// Seconds between certificate-file change checks (default 10).
    pub reload_interval_secs: Option<u64>,
    /// Serve the management endpoint over TLS too.
    #[serde(default)]
    pub management: bool,
}

/// `[limits]` — overrides for the matching [`BrokerConfig`] fields.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
#[allow(missing_docs)]
pub struct LimitsSection {
    pub max_connections: Option<usize>,
    pub max_queues: Option<usize>,
    pub max_queues_per_vhost: Option<usize>,
    pub max_queue_depth: Option<usize>,
    pub max_queue_bytes: Option<usize>,
    pub max_message_size: Option<u64>,
    pub initial_credit: Option<u32>,
    pub resident_bytes_max: Option<usize>,
}

/// `[[users]]`: exactly one of `verifier`, `verifier_file`, `password_file`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct UserEntry {
    /// The username.
    pub name: String,
    /// Inline RFC 5803 verifier(s) — not secret (salted + iterated), but a
    /// file keeps rotation out of the config.
    pub verifier: Option<String>,
    /// File holding RFC 5803 verifier line(s), one per mechanism.
    pub verifier_file: Option<PathBuf>,
    /// File holding the plaintext password (derived at load, discarded).
    pub password_file: Option<PathBuf>,
    /// Management-API administrator.
    #[serde(default)]
    pub admin: bool,
}

/// `[[permissions]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PermissionEntry {
    /// Username, `anonymous`, or `*`.
    pub user: String,
    /// Vhost (empty = default) or `*`.
    #[serde(default)]
    pub vhost: String,
    /// Address glob (default `*`).
    #[serde(default = "star")]
    pub address: String,
    /// `send` and/or `receive`.
    pub operations: Vec<String>,
}

fn star() -> String {
    "*".to_owned()
}

/// `[[policies]]` — the [`QueuePolicy`] fields, keyed by vhost + name prefix.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct PolicyEntry {
    /// Vhost (empty = default).
    #[serde(default)]
    pub vhost: String,
    /// Queue-name prefix (bare name, no `/queues/`); empty = all.
    #[serde(default)]
    pub prefix: String,
    /// Message TTL in milliseconds.
    pub message_ttl_ms: Option<u64>,
    /// Max messages held.
    pub max_length: Option<usize>,
    /// Max body bytes held.
    pub max_length_bytes: Option<usize>,
    /// `reject-publish` (default) or `drop-head`.
    pub overflow: Option<String>,
    /// Dead-letter target address.
    pub dead_letter: Option<String>,
    /// Delivery attempts before dead-lettering.
    pub max_delivery_attempts: Option<u32>,
}

/// `[[queues]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct QueueEntry {
    /// Vhost (empty = default).
    #[serde(default)]
    pub vhost: String,
    /// The queue address (`/queues/x`, `/quorum/x`, `/durable/x`).
    pub address: String,
}

/// `[[exchanges]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct ExchangeEntry {
    /// Vhost (empty = default).
    #[serde(default)]
    pub vhost: String,
    /// Exchange name (addressed as `/exchanges/<name>[/<key>]`).
    pub name: String,
    /// `fanout`, `direct`, or `topic`.
    #[serde(rename = "type")]
    pub kind: String,
    /// Queue address for unroutable messages.
    pub alternate: Option<String>,
}

/// `[[bindings]]`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
#[non_exhaustive]
pub struct BindingEntry {
    /// Vhost (empty = default).
    #[serde(default)]
    pub vhost: String,
    /// The exchange name.
    pub exchange: String,
    /// The destination queue address.
    pub queue: String,
    /// Binding key (topic patterns allowed; ignored by fanout).
    #[serde(default)]
    pub key: String,
}

impl ExchangeEntry {
    /// The parsed kind (validated at load).
    pub fn kind(&self) -> crate::exchange::ExchangeKind {
        crate::exchange::ExchangeKind::parse(&self.kind).expect("validated at load")
    }
}

impl FileConfig {
    /// Read and parse a config file.
    pub fn load(path: &Path) -> Result<FileConfig, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("reading {}: {e}", path.display()))?;
        let base = path.parent().map(Path::to_path_buf).unwrap_or_default();
        Self::from_toml(&text, &base).map_err(|e| format!("{}: {e}", path.display()))
    }

    /// Parse TOML text; relative paths resolve against `base_dir`.
    pub fn from_toml(text: &str, base_dir: &Path) -> Result<FileConfig, String> {
        let mut cfg: FileConfig = toml::from_str(text).map_err(|e| e.to_string())?;
        cfg.base_dir = base_dir.to_path_buf();
        cfg.validate()?;
        Ok(cfg)
    }

    fn resolve(&self, p: &Path) -> PathBuf {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            self.base_dir.join(p)
        }
    }

    /// Structural checks that need no file I/O.
    fn validate(&self) -> Result<(), String> {
        let mut seen = std::collections::HashSet::new();
        for u in &self.users {
            if u.name.is_empty() {
                return Err("a [[users]] entry has an empty name".to_owned());
            }
            if u.name == ANONYMOUS_USER || u.name == "*" {
                return Err(format!("user name {:?} is reserved", u.name));
            }
            if !seen.insert(u.name.as_str()) {
                return Err(format!("user {:?} is defined twice", u.name));
            }
            let sources = [
                u.verifier.is_some(),
                u.verifier_file.is_some(),
                u.password_file.is_some(),
            ]
            .iter()
            .filter(|b| **b)
            .count();
            if sources != 1 {
                return Err(format!(
                    "user {:?}: set exactly one of `verifier`, `verifier_file`, `password_file`",
                    u.name
                ));
            }
        }
        for p in &self.permissions {
            if p.operations.is_empty() {
                return Err(format!(
                    "permission for {:?} on {:?} grants no operations",
                    p.user, p.address
                ));
            }
            for op in &p.operations {
                if op != "send" && op != "receive" {
                    return Err(format!(
                        "unknown operation {op:?} (expected \"send\" or \"receive\")"
                    ));
                }
            }
        }
        for p in &self.policies {
            if let Some(o) = &p.overflow
                && o != "reject-publish"
                && o != "drop-head"
            {
                return Err(format!(
                    "unknown overflow {o:?} (expected \"reject-publish\" or \"drop-head\")"
                ));
            }
        }
        for q in &self.queues {
            if !crate::registry::QueueRegistry::is_queue_address(&q.address) {
                return Err(format!(
                    "[[queues]] address {:?} must be /queues/<name>, /quorum/<name> or /durable/<name>",
                    q.address
                ));
            }
        }
        for e in &self.exchanges {
            let kind = crate::exchange::ExchangeKind::parse(&e.kind).ok_or_else(|| {
                format!(
                    "exchange {:?}: unknown type {:?} (expected fanout, direct or topic)",
                    e.name, e.kind
                )
            })?;
            crate::exchange::validate(&crate::exchange::ExchangeOp::Declare {
                vhost: e.vhost.clone(),
                name: e.name.clone(),
                kind,
                alternate: e.alternate.clone(),
            })
            .map_err(|err| format!("[[exchanges]] {:?}: {err}", e.name))?;
        }
        for b in &self.bindings {
            crate::exchange::validate(&crate::exchange::ExchangeOp::Bind {
                vhost: b.vhost.clone(),
                exchange: b.exchange.clone(),
                destination: b.queue.clone(),
                key: b.key.clone(),
            })
            .map_err(|err| format!("[[bindings]] {:?} -> {:?}: {err}", b.exchange, b.queue))?;
        }
        if let Some(c) = &self.cluster {
            for s in &c.seeds {
                parse_seed(s)
                    .ok_or_else(|| format!("bad seed {s:?} (expected <id>=<host:port>)"))?;
            }
        }
        Ok(())
    }

    /// Apply the file's broker settings onto `config`.
    pub fn apply(&self, config: &mut BrokerConfig) -> Result<(), String> {
        if let Some(d) = &self.data_dir {
            config.data_dir = Some(self.resolve(d));
        }
        if let Some(m) = &self.management_listen {
            config.management_listen = Some(m.clone());
        }
        if let Some(f) = &self.management_token_file {
            config.management_token = Some(read_token(&self.resolve(f))?);
        }
        if let Some(r) = self.management_auth_reads {
            config.management_auth_reads = r;
        }
        if let Some(l) = &self.limits {
            macro_rules! set {
                ($($f:ident),*) => { $(if let Some(v) = l.$f { config.$f = v; })* };
            }
            set!(
                max_connections,
                max_queues,
                max_queues_per_vhost,
                max_queue_depth,
                max_queue_bytes,
                initial_credit,
                resident_bytes_max
            );
            if let Some(v) = l.max_message_size {
                config.max_message_size = Some(v);
            }
        }
        if let Some(t) = &self.tls {
            let mut tls = crate::config::TlsServerConfig::new(
                self.resolve(&t.cert_file),
                self.resolve(&t.key_file),
            );
            tls.client_ca_file = t.client_ca_file.as_ref().map(|p| self.resolve(p));
            if let Some(secs) = t.reload_interval_secs {
                tls.reload_interval = std::time::Duration::from_secs(secs.max(1));
            }
            config.tls = Some(tls);
            config.tls_listen = t.listen.clone();
            config.management_tls = t.management;
        }
        if let Some(c) = &self.cluster {
            let seeds: Vec<(u64, String)> = c.seeds.iter().filter_map(|s| parse_seed(s)).collect();
            if !seeds.iter().any(|(id, _)| *id == c.node_id) {
                return Err(format!(
                    "cluster.node_id {} must appear in cluster.seeds",
                    c.node_id
                ));
            }
            let mut member = ClusterMemberConfig::new(c.node_id, c.listen.clone(), seeds);
            if let Some(r) = c.replicas {
                member.replicas = r;
            }
            if c.tls {
                let tls = config
                    .tls
                    .clone()
                    .ok_or("cluster.tls needs a [tls] section")?;
                if tls.client_ca_file.is_none() {
                    return Err(
                        "cluster.tls needs [tls].client_ca_file (the cluster CA) for mutual auth"
                            .to_owned(),
                    );
                }
                member.tls = Some(tls);
            }
            config.cluster = Some(member);
        }
        for p in &self.policies {
            config
                .policies
                .push((policy_key(&p.vhost, &p.prefix), p.to_policy()));
        }
        Ok(())
    }

    /// Build the authenticator, or `None` when no users are configured (the
    /// caller then decides between refusing to start and `AllowAll`).
    /// Reads verifier/password files.
    pub fn authenticator(&self) -> Result<Option<StaticUsers>, String> {
        if self.users.is_empty() {
            if !self.permissions.is_empty() && !self.allow_anonymous.unwrap_or(false) {
                return Err("[[permissions]] are configured but there are no [[users]] \
                     (and allow_anonymous is off)"
                    .to_owned());
            }
            if self.permissions.is_empty() {
                return Ok(None);
            }
        }
        let mut users = StaticUsers::new()
            .allow_plain(self.allow_plain.unwrap_or(true))
            .allow_anonymous(self.allow_anonymous.unwrap_or(false));
        for u in &self.users {
            let mut lines: Vec<String> = Vec::new();
            if let Some(v) = &u.verifier {
                lines.push(v.clone());
            }
            if let Some(f) = &u.verifier_file {
                let path = self.resolve(f);
                let text = std::fs::read_to_string(&path)
                    .map_err(|e| format!("user {:?}: reading {}: {e}", u.name, path.display()))?;
                lines.extend(
                    text.lines()
                        .map(str::trim)
                        .filter(|l| !l.is_empty() && !l.starts_with('#'))
                        .map(str::to_owned),
                );
            }
            for line in &lines {
                let (mech, verifier) =
                    parse_scram_verifier(line).map_err(|e| format!("user {:?}: {e}", u.name))?;
                users = users.with_verifier(u.name.clone(), mech, verifier);
            }
            if let Some(f) = &u.password_file {
                let path = self.resolve(f);
                let raw = std::fs::read_to_string(&path)
                    .map_err(|e| format!("user {:?}: reading {}: {e}", u.name, path.display()))?;
                // A mounted secret usually ends in a newline that is not part
                // of the password.
                let pw = raw.strip_suffix('\n').unwrap_or(&raw);
                let pw = pw.strip_suffix('\r').unwrap_or(pw);
                if pw.is_empty() {
                    return Err(format!("user {:?}: password file is empty", u.name));
                }
                let verifier =
                    derive_scram_verifier(ScramMechanism::Sha256, pw, StaticScram::ITERATIONS)
                        .ok_or_else(|| {
                            format!("user {:?}: password rejected by SASLprep", u.name)
                        })?;
                users = users.with_verifier(u.name.clone(), ScramMechanism::Sha256, verifier);
            } else if lines.is_empty() {
                return Err(format!("user {:?}: no verifier found", u.name));
            }
            if u.admin {
                users = users.with_admin(u.name.clone());
            }
        }
        for p in &self.permissions {
            users = users.with_permission(Permission::new(
                p.user.clone(),
                p.vhost.clone(),
                p.address.clone(),
                p.operations.iter().any(|o| o == "send"),
                p.operations.iter().any(|o| o == "receive"),
            ));
        }
        Ok(Some(users))
    }
}

impl PolicyEntry {
    fn to_policy(&self) -> QueuePolicy {
        QueuePolicy {
            message_ttl: self.message_ttl_ms.map(std::time::Duration::from_millis),
            max_length: self.max_length,
            max_length_bytes: self.max_length_bytes,
            overflow: match self.overflow.as_deref() {
                Some("drop-head") => OverflowBehavior::DropHead,
                _ => OverflowBehavior::RejectPublish,
            },
            dead_letter: self.dead_letter.clone(),
            max_delivery_attempts: self.max_delivery_attempts,
        }
    }
}

/// Policies match the namespace-qualified queue name (`<vhost>/<name>` for
/// a non-default vhost), so a per-vhost prefix is the vhost plus `/`.
fn policy_key(vhost: &str, prefix: &str) -> String {
    if vhost.is_empty() {
        prefix.to_owned()
    } else {
        format!("{vhost}/{prefix}")
    }
}

/// Read a secret token file (a trailing newline is not part of it).
pub fn read_token(path: &Path) -> Result<crate::config::SecretString, String> {
    let raw = std::fs::read_to_string(path)
        .map_err(|e| format!("reading token file {}: {e}", path.display()))?;
    let token = raw.trim_end_matches(['\n', '\r']);
    if token.len() < 16 {
        return Err(format!(
            "token in {} is shorter than 16 characters",
            path.display()
        ));
    }
    Ok(crate::config::SecretString::new(token))
}

/// Parse a `"<id>=<host:port>"` seed.
pub fn parse_seed(s: &str) -> Option<(u64, String)> {
    let (id, addr) = s.split_once('=')?;
    let addr = addr.trim();
    (!addr.is_empty()).then(|| Some((id.trim().parse().ok()?, addr.to_owned())))?
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{Authenticator, Credentials, Operation, format_scram_verifier};

    #[test]
    fn full_file_parses_and_applies() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("ops.pw"), "hunter2\n").unwrap();
        let v = derive_scram_verifier(ScramMechanism::Sha512, "pw", 4096).unwrap();
        std::fs::write(
            dir.path().join("orders.scram"),
            format!(
                "# rotated 2026-11\n{}\n",
                format_scram_verifier(ScramMechanism::Sha512, &v)
            ),
        )
        .unwrap();
        let text = r#"
            listen = "0.0.0.0:5672"
            management_listen = "127.0.0.1:15692"
            data_dir = "state"
            [limits]
            max_connections = 7
            max_message_size = 1024
            [cluster]
            node_id = 2
            listen = "0.0.0.0:7472"
            seeds = ["1=a:7472", "2=b:7472"]
            replicas = 2
            tls = true
            [tls]
            listen = "0.0.0.0:5671"
            cert_file = "tls.crt"
            key_file = "tls.key"
            client_ca_file = "/abs/ca.crt"
            reload_interval_secs = 3
            management = true
            [[users]]
            name = "orders"
            verifier_file = "orders.scram"
            [[users]]
            name = "ops"
            password_file = "ops.pw"
            admin = true
            [[permissions]]
            user = "orders"
            address = "/queues/orders.*"
            operations = ["send"]
            [[policies]]
            vhost = "tenant"
            prefix = "work."
            message_ttl_ms = 1500
            overflow = "drop-head"
            dead_letter = "/queues/dead"
            [[queues]]
            address = "/quorum/orders.created"
            [[exchanges]]
            name = "events"
            type = "topic"
            alternate = "/queues/unroutable"
            [[bindings]]
            exchange = "events"
            queue = "/quorum/orders.created"
            key = "order.#"
        "#;
        let cfg = FileConfig::from_toml(text, dir.path()).unwrap();
        let mut bc = BrokerConfig::default();
        cfg.apply(&mut bc).unwrap();
        assert_eq!(bc.max_connections, 7);
        assert_eq!(bc.max_message_size, Some(1024));
        assert_eq!(
            bc.data_dir.as_deref(),
            Some(dir.path().join("state").as_path())
        );
        assert_eq!(bc.management_listen.as_deref(), Some("127.0.0.1:15692"));
        let cluster = bc.cluster.as_ref().unwrap();
        assert_eq!((cluster.node_id, cluster.replicas), (2, 2));
        let tls = bc.tls.as_ref().unwrap();
        assert_eq!(tls.cert_file, dir.path().join("tls.crt"));
        assert_eq!(
            tls.client_ca_file.as_deref(),
            Some(Path::new("/abs/ca.crt"))
        );
        assert_eq!(tls.reload_interval, std::time::Duration::from_secs(3));
        assert_eq!(cluster.tls.as_ref(), Some(tls));
        assert_eq!(bc.tls_listen.as_deref(), Some("0.0.0.0:5671"));
        assert!(bc.management_tls);
        assert_eq!(bc.policies.len(), 1);
        assert_eq!(bc.policies[0].0, "tenant/work.");
        assert_eq!(bc.policies[0].1.overflow, OverflowBehavior::DropHead);
        assert_eq!(
            bc.policies[0].1.message_ttl,
            Some(std::time::Duration::from_millis(1500))
        );
        assert_eq!(cfg.queues[0].address, "/quorum/orders.created");
        assert_eq!(
            cfg.exchanges[0].kind(),
            crate::exchange::ExchangeKind::Topic
        );
        assert_eq!(cfg.bindings[0].key, "order.#");

        let auth = cfg.authenticator().unwrap().unwrap();
        assert!(auth.verify(Credentials::Plain {
            authcid: "ops",
            passwd: "hunter2"
        }));
        assert!(auth.verify(Credentials::Plain {
            authcid: "orders",
            passwd: "pw"
        }));
        assert!(auth.is_admin("ops") && !auth.is_admin("orders"));
        assert_eq!(
            auth.mechanisms(),
            &["SCRAM-SHA-512", "SCRAM-SHA-256", "PLAIN"]
        );
        assert!(auth.authorize(Some("orders"), "", "/queues/orders.eu", Operation::Send));
        assert!(!auth.authorize(Some("orders"), "", "/queues/orders.eu", Operation::Receive));
        assert!(!auth.authorize(Some("ops"), "", "/queues/orders.eu", Operation::Send));
    }

    #[test]
    fn invalid_files_are_refused() {
        let bad = |t: &str| FileConfig::from_toml(t, Path::new(".")).unwrap_err();
        assert!(bad("lisen = \"x\"").contains("unknown field"));
        assert!(bad("[[users]]\nname = \"anonymous\"\nverifier = \"x\"").contains("reserved"));
        assert!(bad("[[users]]\nname = \"a\"").contains("exactly one"));
        assert!(
            bad("[[permissions]]\nuser = \"a\"\noperations = [\"write\"]")
                .contains("unknown operation")
        );
        assert!(bad("[[policies]]\noverflow = \"drop\"").contains("unknown overflow"));
        assert!(bad("[[exchanges]]\nname = \"e\"\ntype = \"headers\"").contains("unknown type"));
        assert!(
            bad("[[bindings]]\nexchange = \"e\"\nqueue = \"/exchanges/x\"")
                .contains("queue address")
                || bad("[[bindings]]\nexchange = \"e\"\nqueue = \"/exchanges/x\"")
                    .contains("must be")
        );
        assert!(
            bad("[cluster]\nnode_id = 1\nlisten = \"x\"\nseeds = [\"oops\"]").contains("bad seed")
        );
        // Permissions without users and without anonymous: nobody could use them.
        let cfg = FileConfig::from_toml(
            "[[permissions]]\nuser = \"a\"\noperations = [\"send\"]",
            Path::new("."),
        )
        .unwrap();
        assert!(cfg.authenticator().is_err());
        // No users at all → no authenticator (caller decides).
        assert!(
            FileConfig::from_toml("", Path::new("."))
                .unwrap()
                .authenticator()
                .unwrap()
                .is_none()
        );
    }
}
