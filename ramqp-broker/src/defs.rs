//! Runtime definitions: state created through the management API that must
//! survive a restart and, clustered, be the same on every node — per-queue
//! policies, users, and permissions.
//!
//! Each definition is one `key → encoded value` entry. Standalone they live
//! in the store's definitions table (under [`STORE_PREFIX`]); clustered they
//! are replicated by the metadata group (`MetaCommand::Def`). Without a
//! store and without a cluster they are in memory only.
//!
//! Keys: `qpolicy:<qualified queue>`, `user:<name>`,
//! `perm:<vhost>\n<user>` (`\n` cannot occur in either component).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value as Json;

use crate::config::{OverflowBehavior, QueuePolicy};

/// Where runtime definitions live in the store's definitions table.
#[cfg(feature = "store-redb")]
pub(crate) const STORE_PREFIX: &str = "rt:";

/// A consistent view of every runtime definition.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct Defs {
    /// Bumped by every applied change.
    pub epoch: u64,
    /// Key → encoded value.
    pub map: BTreeMap<String, Vec<u8>>,
}

impl Defs {
    /// Decode the value under `key`, if present and readable.
    pub(crate) fn get<T: for<'de> Deserialize<'de>>(&self, key: &str) -> Option<T> {
        self.map
            .get(key)
            .and_then(|v| crate::serde_bin::from_slice(v).ok())
    }

    /// The raw user and permission entries (what the authenticator is
    /// built from), for change detection.
    pub(crate) fn user_and_permission_entries(&self) -> Vec<(String, Vec<u8>)> {
        self.map
            .iter()
            .filter(|(k, _)| k.starts_with(USER) || k.starts_with(PERM))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }

    /// Every runtime user (name → definition).
    pub(crate) fn users(&self) -> impl Iterator<Item = (&str, UserDef)> + '_ {
        self.prefixed(USER)
            .filter_map(|(name, v)| crate::serde_bin::from_slice(v).ok().map(|u| (name, u)))
    }

    /// Every runtime permission set: `(vhost, user, grants)`.
    pub(crate) fn permissions(&self) -> impl Iterator<Item = (&str, &str, Vec<Grant>)> + '_ {
        self.prefixed(PERM).filter_map(|(id, v)| {
            let (vhost, user) = id.split_once('\n')?;
            crate::serde_bin::from_slice(v)
                .ok()
                .map(|g| (vhost, user, g))
        })
    }

    fn prefixed<'a>(&'a self, prefix: &'a str) -> impl Iterator<Item = (&'a str, &'a [u8])> + 'a {
        self.map
            .range(prefix.to_owned()..)
            .take_while(move |(k, _)| k.starts_with(prefix))
            .map(move |(k, v)| (&k[prefix.len()..], v.as_slice()))
    }
}

const QPOLICY: &str = "qpolicy:";
const USER: &str = "user:";
const PERM: &str = "perm:";

/// The key of a queue's policy.
pub(crate) fn queue_policy_key(qualified: &str) -> String {
    format!("{QPOLICY}{qualified}")
}

/// The key of a runtime user.
pub(crate) fn user_key(name: &str) -> String {
    format!("{USER}{name}")
}

/// The key of a user's grants within a vhost.
pub(crate) fn perm_key(vhost: &str, user: &str) -> String {
    format!("{PERM}{vhost}\n{user}")
}

/// Every permission key of `user`, across vhosts.
pub(crate) fn perm_keys_of<'a>(defs: &'a Defs, user: &'a str) -> impl Iterator<Item = String> + 'a {
    defs.permissions()
        .filter(move |(_, u, _)| *u == user)
        .map(|(v, u, _)| perm_key(v, u))
        .collect::<Vec<_>>()
        .into_iter()
}

/// A queue's own policy, set when it is declared (management API or
/// `[[queues]]`). Overrides any prefix policy for that queue.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PolicySpec {
    /// Message TTL in milliseconds.
    pub message_ttl_ms: Option<u64>,
    /// Max messages held.
    pub max_length: Option<u64>,
    /// Max body bytes held.
    pub max_length_bytes: Option<u64>,
    /// `reject-publish` (default) or `drop-head`.
    pub overflow: Option<String>,
    /// Dead-letter target address.
    pub dead_letter: Option<String>,
    /// Delivery attempts before dead-lettering.
    pub max_delivery_attempts: Option<u32>,
}

impl PolicySpec {
    /// The JSON fields a queue declaration may carry.
    pub(crate) const FIELDS: [&'static str; 6] = [
        "message_ttl_ms",
        "max_length",
        "max_length_bytes",
        "overflow",
        "dead_letter",
        "max_delivery_attempts",
    ];

    /// Whether no field is set (equivalent to "no queue policy").
    pub fn is_empty(&self) -> bool {
        *self == PolicySpec::default()
    }

    /// Check the values (overflow keyword, dead-letter address form).
    pub fn validate(&self) -> Result<(), String> {
        if let Some(o) = &self.overflow
            && o != "reject-publish"
            && o != "drop-head"
        {
            return Err(format!(
                "unknown overflow {o:?} (expected \"reject-publish\" or \"drop-head\")"
            ));
        }
        if let Some(d) = &self.dead_letter
            && !crate::registry::QueueRegistry::is_queue_address(d)
        {
            return Err(format!(
                "dead_letter {d:?} is not a queue address (/queues/, /quorum/, /durable/)"
            ));
        }
        if self.max_delivery_attempts == Some(0) {
            return Err("max_delivery_attempts must be at least 1".to_owned());
        }
        Ok(())
    }

    /// As the policy type the queues run on.
    pub fn to_policy(&self) -> QueuePolicy {
        let size = |v: Option<u64>| v.map(|v| usize::try_from(v).unwrap_or(usize::MAX));
        QueuePolicy {
            message_ttl: self.message_ttl_ms.map(std::time::Duration::from_millis),
            max_length: size(self.max_length),
            max_length_bytes: size(self.max_length_bytes),
            overflow: match self.overflow.as_deref() {
                Some("drop-head") => OverflowBehavior::DropHead,
                _ => OverflowBehavior::RejectPublish,
            },
            dead_letter: self.dead_letter.clone(),
            max_delivery_attempts: self.max_delivery_attempts,
        }
    }

    /// From the policy type (a configuration `[[queues]]` policy, for
    /// display).
    pub fn from_policy(p: &QueuePolicy) -> Self {
        PolicySpec {
            message_ttl_ms: p
                .message_ttl
                .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX)),
            max_length: p.max_length.map(|v| v as u64),
            max_length_bytes: p.max_length_bytes.map(|v| v as u64),
            overflow: Some(
                match p.overflow {
                    OverflowBehavior::DropHead => "drop-head",
                    _ => "reject-publish",
                }
                .to_owned(),
            ),
            dead_letter: p.dead_letter.clone(),
            max_delivery_attempts: p.max_delivery_attempts,
        }
    }

    /// Read the policy fields of a queue-declaration JSON body (other fields
    /// are ignored here). `Ok(None)` when none is present.
    pub(crate) fn from_json(body: &Json) -> Result<Option<Self>, String> {
        if !Self::FIELDS
            .iter()
            .any(|f| body.get(f).is_some_and(|v| !v.is_null()))
        {
            return Ok(None);
        }
        let uint = |f: &str| -> Result<Option<u64>, String> {
            match body.get(f) {
                None | Some(Json::Null) => Ok(None),
                Some(v) => v
                    .as_u64()
                    .map(Some)
                    .ok_or_else(|| format!("{f:?} must be a non-negative integer")),
            }
        };
        let string = |f: &str| -> Result<Option<String>, String> {
            match body.get(f) {
                None | Some(Json::Null) => Ok(None),
                Some(Json::String(s)) => Ok(Some(s.clone())),
                Some(_) => Err(format!("{f:?} must be a string")),
            }
        };
        let spec = PolicySpec {
            message_ttl_ms: uint("message_ttl_ms")?,
            max_length: uint("max_length")?,
            max_length_bytes: uint("max_length_bytes")?,
            overflow: string("overflow")?,
            dead_letter: string("dead_letter")?,
            max_delivery_attempts: uint("max_delivery_attempts")?
                .map(|v| u32::try_from(v).map_err(|_| "max_delivery_attempts is too large"))
                .transpose()?,
        };
        spec.validate()?;
        Ok(Some(spec))
    }

    /// As JSON (for `GET /queues/...`).
    pub(crate) fn to_json(&self) -> Json {
        serde_json::json!({
            "message_ttl_ms": self.message_ttl_ms,
            "max_length": self.max_length,
            "max_length_bytes": self.max_length_bytes,
            "overflow": self.overflow,
            "dead_letter": self.dead_letter,
            "max_delivery_attempts": self.max_delivery_attempts,
        })
    }
}

/// A user created through the management API.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct UserDef {
    /// RFC 5803 SCRAM verifiers (`SCRAM-SHA-256$<iter>:<salt>$<stored>:<server>`).
    /// The password itself is never stored.
    pub verifiers: Vec<String>,
    /// May administer the broker through the management API.
    pub admin: bool,
}

/// One grant: an address glob and the operations allowed on it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Grant {
    /// Address glob, as in `[[permissions]]` (`*` any run, `?` one char).
    pub address: String,
    /// May publish.
    pub send: bool,
    /// May consume.
    pub receive: bool,
}

impl Grant {
    /// Parse `[{"address": "...", "operations": ["send", "receive"]}, ...]`
    /// (or one such object).
    pub(crate) fn list_from_json(body: &Json) -> Result<Vec<Grant>, String> {
        let items: Vec<&Json> = match body {
            Json::Array(a) => a.iter().collect(),
            Json::Object(o) if o.contains_key("grants") => match &o["grants"] {
                Json::Array(a) => a.iter().collect(),
                _ => return Err("\"grants\" must be an array".to_owned()),
            },
            Json::Object(_) => vec![body],
            _ => return Err("expected a grant object or an array of them".to_owned()),
        };
        if items.is_empty() {
            return Err("no grants given (DELETE the permissions to remove them)".to_owned());
        }
        items
            .into_iter()
            .map(|g| {
                let address = g
                    .get("address")
                    .and_then(Json::as_str)
                    .filter(|a| !a.is_empty())
                    .ok_or("each grant needs a non-empty \"address\"")?
                    .to_owned();
                let ops = g
                    .get("operations")
                    .and_then(Json::as_array)
                    .ok_or("each grant needs \"operations\": [\"send\" and/or \"receive\"]")?;
                let mut grant = Grant {
                    address,
                    send: false,
                    receive: false,
                };
                for op in ops {
                    match op.as_str() {
                        Some("send") => grant.send = true,
                        Some("receive") => grant.receive = true,
                        other => {
                            return Err(format!(
                                "unknown operation {other:?} (expected \"send\" or \"receive\")"
                            ));
                        }
                    }
                }
                Ok(grant)
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn policy_spec_parses_validates_and_converts() {
        let body = serde_json::json!({
            "kind": "quorum",
            "dead_letter": "/quorum/jobs.dlq",
            "max_delivery_attempts": 3,
            "overflow": "drop-head",
            "max_length": 10
        });
        let spec = PolicySpec::from_json(&body).unwrap().unwrap();
        let p = spec.to_policy();
        assert_eq!(p.dead_letter.as_deref(), Some("/quorum/jobs.dlq"));
        assert_eq!(p.max_delivery_attempts, Some(3));
        assert_eq!(p.overflow, OverflowBehavior::DropHead);
        assert_eq!(p.max_length, Some(10));
        assert_eq!(
            PolicySpec::from_json(&serde_json::json!({"kind": "durable"})),
            Ok(None)
        );
        assert!(PolicySpec::from_json(&serde_json::json!({"overflow": "explode"})).is_err());
        assert!(PolicySpec::from_json(&serde_json::json!({"dead_letter": "nowhere"})).is_err());
        assert!(PolicySpec::from_json(&serde_json::json!({"max_length": -1})).is_err());
    }

    #[test]
    fn grants_parse_from_either_shape() {
        let one = serde_json::json!({"address": "/queues/jobs", "operations": ["receive"]});
        assert_eq!(
            Grant::list_from_json(&one).unwrap(),
            vec![Grant {
                address: "/queues/jobs".into(),
                send: false,
                receive: true
            }]
        );
        let many = serde_json::json!([
            {"address": "/queues/a", "operations": ["send", "receive"]},
            {"address": "/exchanges/*", "operations": ["send"]}
        ]);
        assert_eq!(Grant::list_from_json(&many).unwrap().len(), 2);
        assert!(Grant::list_from_json(&serde_json::json!([])).is_err());
        assert!(
            Grant::list_from_json(&serde_json::json!({"address": "x", "operations": ["admin"]}))
                .is_err()
        );
    }

    #[test]
    fn defs_index_users_and_permissions_by_key() {
        let mut defs = Defs::default();
        let user = UserDef {
            verifiers: vec!["v".into()],
            admin: false,
        };
        defs.map
            .insert(user_key("reader"), crate::serde_bin::to_vec(&user).unwrap());
        let grants = vec![Grant {
            address: "/queues/jobs".into(),
            send: false,
            receive: true,
        }];
        defs.map.insert(
            perm_key("tenant", "reader"),
            crate::serde_bin::to_vec(&grants).unwrap(),
        );
        defs.map.insert(
            queue_policy_key("jobs"),
            crate::serde_bin::to_vec(&PolicySpec::default()).unwrap(),
        );
        assert_eq!(defs.users().map(|(n, _)| n).collect::<Vec<_>>(), ["reader"]);
        let perms: Vec<_> = defs.permissions().collect();
        assert_eq!(perms, vec![("tenant", "reader", grants)]);
        assert_eq!(
            perm_keys_of(&defs, "reader").collect::<Vec<_>>(),
            [perm_key("tenant", "reader")]
        );
        assert!(defs.get::<PolicySpec>(&queue_policy_key("jobs")).is_some());
    }
}
