//! Exchanges: publish once, deliver to every bound queue (issue #37 §1).
//!
//! Addressing follows RabbitMQ 4.x's AMQP 1.0 "v2" format, so clients move
//! without changes: a producer targets `/exchanges/<name>/<routing-key>`, or
//! `/exchanges/<name>` with the routing key taken from each message's
//! `properties.subject`. Segments are percent-decoded (`%2F` for a `/`
//! inside a key). Consumers never attach to exchanges — they consume from
//! the queues bound to them.
//!
//! Kinds: **fanout** (every bound queue), **direct** (binding key equals
//! routing key), **topic** (dot-separated words; `*` matches exactly one
//! word, `#` zero or more). A message matching several bindings of the same
//! queue is delivered to it once. A message matching nothing is accepted and
//! dropped — or routed to the exchange's `alternate` queue when set.
//!
//! Every vhost implicitly has `amq.direct`, `amq.fanout` and `amq.topic`.
//!
//! Definitions are small and change rarely; the router reads them through an
//! `Arc` snapshot plus a version counter, so a producer's hot path is one
//! atomic load while its cached route is current.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

/// An exchange's routing behavior.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExchangeKind {
    /// Every bound queue, regardless of key.
    Fanout,
    /// Queues whose binding key equals the routing key.
    Direct,
    /// AMQP topic matching (`*` = one word, `#` = zero or more).
    Topic,
}

impl ExchangeKind {
    /// Parse `fanout` / `direct` / `topic`.
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "fanout" => Some(ExchangeKind::Fanout),
            "direct" => Some(ExchangeKind::Direct),
            "topic" => Some(ExchangeKind::Topic),
            _ => None,
        }
    }

    /// The lowercase name.
    pub fn as_str(self) -> &'static str {
        match self {
            ExchangeKind::Fanout => "fanout",
            ExchangeKind::Direct => "direct",
            ExchangeKind::Topic => "topic",
        }
    }
}

/// One exchange → queue binding.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct BindingDef {
    /// The destination queue address (`/queues/x`, `/quorum/x`, `/durable/x`),
    /// in the exchange's vhost.
    pub destination: String,
    /// The binding key (ignored by fanout).
    pub key: String,
}

/// A declared exchange.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ExchangeDef {
    pub kind: ExchangeKind,
    /// Where unroutable messages go (a queue address), if anywhere.
    pub alternate: Option<String>,
    pub bindings: Vec<BindingDef>,
}

/// A topology change. Applied deterministically (the same op yields the
/// same result on every node), so it can ride the metadata Raft log.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) enum ExchangeOp {
    Declare {
        vhost: String,
        name: String,
        kind: ExchangeKind,
        alternate: Option<String>,
    },
    Delete {
        vhost: String,
        name: String,
    },
    Bind {
        vhost: String,
        exchange: String,
        destination: String,
        key: String,
    },
    Unbind {
        vhost: String,
        exchange: String,
        destination: String,
        key: String,
    },
    /// Remove every binding (in `vhost`) to `destination` — the queue was
    /// deleted. Appended last: existing variants keep their wire tags.
    UnbindDestination {
        vhost: String,
        destination: String,
    },
}

/// Why a topology change was refused.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ExchangeError {
    /// Declared already with different arguments (HTTP 409).
    Conflict(String),
    /// The exchange (or binding) does not exist (HTTP 404).
    NotFound(String),
    /// A malformed name, key, or destination (HTTP 400).
    Invalid(String),
    /// The change could not be committed (no metadata leader, storage
    /// failure; HTTP 503).
    Unavailable(String),
}

impl std::fmt::Display for ExchangeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ExchangeError::Conflict(m)
            | ExchangeError::NotFound(m)
            | ExchangeError::Invalid(m)
            | ExchangeError::Unavailable(m) => f.write_str(m),
        }
    }
}

impl std::error::Error for ExchangeError {}

/// Exchange definitions keyed by [`def_key`].
pub(crate) type Defs = BTreeMap<String, ExchangeDef>;

/// The definitions-map key: `<vhost>/<name>` (neither may contain `/`).
pub(crate) fn def_key(vhost: &str, name: &str) -> String {
    format!("{vhost}/{name}")
}

const BUILTINS: [(&str, ExchangeKind); 3] = [
    ("amq.direct", ExchangeKind::Direct),
    ("amq.fanout", ExchangeKind::Fanout),
    ("amq.topic", ExchangeKind::Topic),
];

fn builtin(name: &str) -> Option<ExchangeDef> {
    BUILTINS
        .iter()
        .find(|(n, _)| *n == name)
        .map(|(_, kind)| ExchangeDef {
            kind: *kind,
            alternate: None,
            bindings: Vec::new(),
        })
}

/// Look up an exchange, materializing the implicit `amq.*` ones.
pub(crate) fn lookup(defs: &Defs, vhost: &str, name: &str) -> Option<ExchangeDef> {
    defs.get(&def_key(vhost, name))
        .cloned()
        .or_else(|| builtin(name))
}

fn valid_segment(s: &str) -> bool {
    !s.is_empty() && s.len() <= 255 && !s.contains('/') && !s.chars().any(char::is_control)
}

fn valid_vhost(s: &str) -> bool {
    s.len() <= 255 && !s.contains('/') && !s.chars().any(char::is_control)
}

/// Longest routing / binding key (bytes), as in AMQP 0-9-1 and RabbitMQ.
pub(crate) const MAX_KEY_LEN: usize = 255;

fn valid_key(s: &str) -> bool {
    s.len() <= MAX_KEY_LEN && !s.chars().any(char::is_control)
}

fn valid_destination(d: &str) -> bool {
    ["/queues/", "/quorum/", "/durable/"].iter().any(|p| {
        d.strip_prefix(p)
            .is_some_and(|n| valid_segment(n) && !n.starts_with('$'))
    })
}

/// Structural validation, shared by every entry point.
pub(crate) fn validate(op: &ExchangeOp) -> Result<(), ExchangeError> {
    let bad = |m: String| Err(ExchangeError::Invalid(m));
    if let ExchangeOp::UnbindDestination { vhost, destination } = op {
        if !valid_vhost(vhost) {
            return bad(format!("invalid vhost {vhost:?}"));
        }
        if !valid_destination(destination) {
            return bad(format!("{destination:?} is not a queue address"));
        }
        return Ok(());
    }
    let (vhost, name) = match op {
        ExchangeOp::Declare { vhost, name, .. } | ExchangeOp::Delete { vhost, name } => {
            (vhost, name)
        }
        ExchangeOp::UnbindDestination { .. } => unreachable!("handled above"),
        ExchangeOp::Bind {
            vhost, exchange, ..
        }
        | ExchangeOp::Unbind {
            vhost, exchange, ..
        } => (vhost, exchange),
    };
    if !valid_vhost(vhost) {
        return bad(format!("invalid vhost {vhost:?}"));
    }
    if !valid_segment(name) {
        return bad(format!(
            "invalid exchange name {name:?} (1-255 chars, no `/` or control characters)"
        ));
    }
    match op {
        ExchangeOp::Declare {
            name, alternate, ..
        } => {
            if name.starts_with("amq.") && builtin(name).is_none() {
                return bad(format!("the `amq.` prefix is reserved ({name:?})"));
            }
            if let Some(a) = alternate
                && !valid_destination(a)
            {
                return bad(format!("alternate {a:?} must be a queue address"));
            }
        }
        ExchangeOp::Bind {
            destination, key, ..
        }
        | ExchangeOp::Unbind {
            destination, key, ..
        } => {
            if !valid_destination(destination) {
                return bad(format!(
                    "destination {destination:?} must be /queues/<name>, /quorum/<name> or /durable/<name>"
                ));
            }
            if !valid_key(key) {
                return bad(format!("invalid binding key {key:?}"));
            }
        }
        ExchangeOp::UnbindDestination { .. } => {}
        ExchangeOp::Delete { name, .. } => {
            if builtin(name).is_some() {
                return bad(format!("{name:?} is built in and cannot be deleted"));
            }
        }
    }
    Ok(())
}

/// Apply an op to the definitions. `Ok(true)` when it changed something,
/// `Ok(false)` for an idempotent no-op. Deterministic.
pub(crate) fn apply_op(defs: &mut Defs, op: &ExchangeOp) -> Result<bool, ExchangeError> {
    validate(op)?;
    match op {
        ExchangeOp::Declare {
            vhost,
            name,
            kind,
            alternate,
        } => {
            let key = def_key(vhost, name);
            let existing = defs.get(&key).cloned().or_else(|| builtin(name));
            match existing {
                Some(e) if e.kind != *kind || e.alternate != *alternate => {
                    Err(ExchangeError::Conflict(format!(
                        "exchange {name:?} exists as {} (alternate {:?})",
                        e.kind.as_str(),
                        e.alternate
                    )))
                }
                Some(_) => Ok(false),
                None => {
                    defs.insert(
                        key,
                        ExchangeDef {
                            kind: *kind,
                            alternate: alternate.clone(),
                            bindings: Vec::new(),
                        },
                    );
                    Ok(true)
                }
            }
        }
        ExchangeOp::Delete { vhost, name } => match defs.remove(&def_key(vhost, name)) {
            Some(_) => Ok(true),
            None => Err(ExchangeError::NotFound(format!("no exchange {name:?}"))),
        },
        ExchangeOp::Bind {
            vhost,
            exchange,
            destination,
            key,
        } => {
            let k = def_key(vhost, exchange);
            if !defs.contains_key(&k) {
                let Some(b) = builtin(exchange) else {
                    return Err(ExchangeError::NotFound(format!("no exchange {exchange:?}")));
                };
                defs.insert(k.clone(), b);
            }
            let def = defs.get_mut(&k).expect("present");
            let binding = BindingDef {
                destination: destination.clone(),
                key: key.clone(),
            };
            if def.bindings.contains(&binding) {
                return Ok(false);
            }
            def.bindings.push(binding);
            Ok(true)
        }
        ExchangeOp::UnbindDestination { vhost, destination } => {
            let prefix = format!("{vhost}/");
            let mut changed = false;
            for (k, def) in defs.iter_mut() {
                if k.starts_with(&prefix) && !k[prefix.len()..].contains('/') {
                    let before = def.bindings.len();
                    def.bindings.retain(|b| b.destination != *destination);
                    changed |= def.bindings.len() != before;
                }
            }
            Ok(changed)
        }
        ExchangeOp::Unbind {
            vhost,
            exchange,
            destination,
            key,
        } => {
            let def = defs
                .get_mut(&def_key(vhost, exchange))
                .ok_or_else(|| ExchangeError::NotFound(format!("no exchange {exchange:?}")))?;
            let before = def.bindings.len();
            def.bindings
                .retain(|b| !(b.destination == *destination && b.key == *key));
            if def.bindings.len() == before {
                return Err(ExchangeError::NotFound(format!(
                    "no binding {exchange:?} -> {destination:?} with key {key:?}"
                )));
            }
            Ok(true)
        }
    }
}

/// AMQP topic match: dot-separated words; `*` = exactly one word, `#` =
/// zero or more words. DP over (pattern word, key word) — no backtracking
/// blow-up however many `#`s a pattern has.
pub(crate) fn topic_match(pattern: &str, key: &str) -> bool {
    let p: Vec<&str> = pattern.split('.').collect();
    let k: Vec<&str> = if key.is_empty() {
        Vec::new()
    } else {
        key.split('.').collect()
    };
    // reach[j]: pattern prefix consumed so far matches key prefix k[..j].
    let mut reach = vec![false; k.len() + 1];
    reach[0] = true;
    for word in &p {
        let mut next = vec![false; k.len() + 1];
        if *word == "#" {
            // Zero or more: anything reachable stays reachable, and extends.
            let mut any = false;
            for j in 0..=k.len() {
                any |= reach[j];
                next[j] = any;
            }
        } else {
            for j in 0..k.len() {
                if reach[j] && (*word == "*" || *word == k[j]) {
                    next[j + 1] = true;
                }
            }
        }
        reach = next;
    }
    reach[k.len()]
}

impl ExchangeDef {
    /// The destinations a message with `routing_key` goes to (deduplicated,
    /// binding order). Empty = unroutable (the caller applies `alternate`).
    pub fn route<'a>(&'a self, routing_key: &str) -> Vec<&'a str> {
        let mut out: Vec<&str> = Vec::new();
        for b in &self.bindings {
            let hit = match self.kind {
                ExchangeKind::Fanout => true,
                ExchangeKind::Direct => b.key == routing_key,
                ExchangeKind::Topic => topic_match(&b.key, routing_key),
            };
            if hit && !out.contains(&b.destination.as_str()) {
                out.push(&b.destination);
            }
        }
        out
    }
}

/// Parse an exchange address: `/exchanges/<name>[/<routing-key>]`, segments
/// percent-decoded. `None` when `address` is not an exchange address (or is
/// malformed).
pub(crate) fn parse_exchange_address(address: &str) -> Option<(String, Option<String>)> {
    let rest = address.strip_prefix("/exchanges/")?;
    let (name, key) = match rest.split_once('/') {
        Some((n, k)) => (n, Some(k)),
        None => (rest, None),
    };
    let name = percent_decode(name)?;
    if !valid_segment(&name) {
        return None;
    }
    let key = match key {
        Some(k) => {
            let k = percent_decode(k)?;
            valid_key(&k).then_some(Some(k))?
        }
        None => None,
    };
    Some((name, key))
}

/// Strict percent-decoding (`%XX` → byte; the result must be UTF-8).
pub(crate) fn percent_decode(s: &str) -> Option<String> {
    if !s.contains('%') {
        return Some(s.to_owned());
    }
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        if b[i] == b'%' {
            let hex = std::str::from_utf8(b.get(i + 1..i + 3)?).ok()?;
            out.push(u8::from_str_radix(hex, 16).ok()?);
            i += 3;
        } else {
            out.push(b[i]);
            i += 1;
        }
    }
    String::from_utf8(out).ok()
}

/// The routing key of a keyless publish: the message's `properties.subject`
/// (RabbitMQ 4.x semantics). Decodes only the leading sections up to
/// `properties`; the body is never touched.
pub(crate) fn message_subject(message: &Bytes) -> Option<String> {
    use ramqp_core::codec::Decode;
    use ramqp_core::codec::described::{Descriptor, descriptors, peek_descriptor};
    use ramqp_core::types::messaging::{
        DeliveryAnnotations, Header, MessageAnnotations, Properties,
    };
    let mut buf = message.clone();
    loop {
        match peek_descriptor(&buf).ok()? {
            Descriptor::Code(descriptors::HEADER) => {
                Header::decode(&mut buf).ok()?;
            }
            Descriptor::Code(descriptors::DELIVERY_ANNOTATIONS) => {
                DeliveryAnnotations::decode(&mut buf).ok()?;
            }
            Descriptor::Code(descriptors::MESSAGE_ANNOTATIONS) => {
                MessageAnnotations::decode(&mut buf).ok()?;
            }
            Descriptor::Code(descriptors::PROPERTIES) => {
                return Properties::decode(&mut buf).ok()?.subject;
            }
            _ => return None,
        }
    }
}

/// The node's view of the exchange definitions: an immutable snapshot
/// swapped whole on change, plus a version a producer compares against its
/// cached route (one atomic load per message while nothing changes).
#[derive(Debug, Default)]
pub(crate) struct ExchangeRegistry {
    defs: std::sync::RwLock<Arc<Defs>>,
    version: AtomicU64,
    /// Clustered: the metadata-group epoch the snapshot reflects.
    epoch: AtomicU64,
}

impl ExchangeRegistry {
    /// The current version (bumped on every replacement).
    pub fn version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// The current definitions.
    pub fn snapshot(&self) -> Arc<Defs> {
        self.defs.read().expect("exchange defs lock").clone()
    }

    /// Replace the definitions (bumps the version).
    pub fn replace(&self, defs: Defs) {
        *self.defs.write().expect("exchange defs lock") = Arc::new(defs);
        self.version.fetch_add(1, Ordering::AcqRel);
    }

    /// Clustered: replace only when `epoch` is newer than what we hold.
    pub fn replace_if_newer(&self, epoch: u64, defs: impl FnOnce() -> Defs) {
        if epoch > self.epoch.load(Ordering::Acquire) {
            let defs = defs();
            let mut guard = self.defs.write().expect("exchange defs lock");
            // Re-check under the lock: a concurrent sync may have won.
            if epoch > self.epoch.load(Ordering::Acquire) {
                *guard = Arc::new(defs);
                self.epoch.store(epoch, Ordering::Release);
                self.version.fetch_add(1, Ordering::AcqRel);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_matching() {
        let t = topic_match;
        assert!(t("order.*", "order.created"));
        assert!(!t("order.*", "order.created.eu"));
        assert!(!t("order.*", "order"));
        assert!(t("order.#", "order"));
        assert!(t("order.#", "order.created.eu"));
        assert!(t("#", ""));
        assert!(t("#", "a.b.c"));
        assert!(t("*.created", "order.created"));
        assert!(t("#.eu", "order.created.eu"));
        assert!(t("a.#.z", "a.z"));
        assert!(t("a.#.z", "a.b.c.z"));
        assert!(!t("a.#.z", "a.b.c"));
        assert!(t("a.*.#", "a.b"));
        assert!(!t("a.*.#", "a"));
        assert!(t("reply.client-7", "reply.client-7"));
        assert!(!t("reply.client-7", "reply.client-8"));
        // Many hashes stay polynomial.
        let many = format!("{}.x", vec!["#"; 40].join("."));
        assert!(t(&many, "y.y.x"));
        assert!(!t(&many, "y.y.y"));
    }

    fn bind(defs: &mut Defs, ex: &str, dest: &str, key: &str) -> Result<bool, ExchangeError> {
        apply_op(
            defs,
            &ExchangeOp::Bind {
                vhost: String::new(),
                exchange: ex.into(),
                destination: dest.into(),
                key: key.into(),
            },
        )
    }

    #[test]
    fn declare_bind_route_unbind_delete() {
        let mut defs = Defs::new();
        let declare = |kind, alternate: Option<&str>| ExchangeOp::Declare {
            vhost: String::new(),
            name: "events".into(),
            kind,
            alternate: alternate.map(str::to_owned),
        };
        assert_eq!(
            apply_op(&mut defs, &declare(ExchangeKind::Topic, None)),
            Ok(true)
        );
        // Idempotent with the same args; 409 with different ones.
        assert_eq!(
            apply_op(&mut defs, &declare(ExchangeKind::Topic, None)),
            Ok(false)
        );
        assert!(matches!(
            apply_op(&mut defs, &declare(ExchangeKind::Fanout, None)),
            Err(ExchangeError::Conflict(_))
        ));
        assert_eq!(bind(&mut defs, "events", "/queues/a", "order.*"), Ok(true));
        assert_eq!(bind(&mut defs, "events", "/queues/a", "order.*"), Ok(false));
        assert_eq!(bind(&mut defs, "events", "/queues/a", "#"), Ok(true));
        assert_eq!(
            bind(&mut defs, "events", "/quorum/b", "order.created"),
            Ok(true)
        );
        let def = lookup(&defs, "", "events").unwrap();
        // /queues/a matches twice but is delivered once.
        assert_eq!(def.route("order.created"), vec!["/queues/a", "/quorum/b"]);
        assert_eq!(def.route("billing.x"), vec!["/queues/a"]);
        assert!(matches!(
            bind(&mut defs, "nope", "/queues/a", "k"),
            Err(ExchangeError::NotFound(_))
        ));
        assert!(matches!(
            bind(&mut defs, "events", "/exchanges/x", "k"),
            Err(ExchangeError::Invalid(_))
        ));
        let unbind = ExchangeOp::Unbind {
            vhost: String::new(),
            exchange: "events".into(),
            destination: "/queues/a".into(),
            key: "#".into(),
        };
        assert_eq!(apply_op(&mut defs, &unbind), Ok(true));
        assert!(matches!(
            apply_op(&mut defs, &unbind),
            Err(ExchangeError::NotFound(_))
        ));
        let del = ExchangeOp::Delete {
            vhost: String::new(),
            name: "events".into(),
        };
        assert_eq!(apply_op(&mut defs, &del), Ok(true));
        assert!(lookup(&defs, "", "events").is_none());
    }

    #[test]
    fn builtins_exist_per_vhost_and_are_protected() {
        let mut defs = Defs::new();
        assert_eq!(
            lookup(&defs, "tenant", "amq.topic").unwrap().kind,
            ExchangeKind::Topic
        );
        assert_eq!(bind(&mut defs, "amq.direct", "/queues/r", "rpc"), Ok(true));
        assert_eq!(
            lookup(&defs, "", "amq.direct").unwrap().route("rpc"),
            vec!["/queues/r"]
        );
        let reserved = ExchangeOp::Declare {
            vhost: String::new(),
            name: "amq.custom".into(),
            kind: ExchangeKind::Direct,
            alternate: None,
        };
        assert!(matches!(
            apply_op(&mut defs, &reserved),
            Err(ExchangeError::Invalid(_))
        ));
        let del = ExchangeOp::Delete {
            vhost: String::new(),
            name: "amq.topic".into(),
        };
        assert!(matches!(
            apply_op(&mut defs, &del),
            Err(ExchangeError::Invalid(_))
        ));
    }

    #[test]
    fn direct_and_fanout() {
        let mut defs = Defs::new();
        for (name, kind) in [("d", ExchangeKind::Direct), ("f", ExchangeKind::Fanout)] {
            apply_op(
                &mut defs,
                &ExchangeOp::Declare {
                    vhost: String::new(),
                    name: name.into(),
                    kind,
                    alternate: None,
                },
            )
            .unwrap();
            bind(&mut defs, name, "/queues/x", "k1").unwrap();
            bind(&mut defs, name, "/queues/y", "k2").unwrap();
        }
        assert_eq!(
            lookup(&defs, "", "d").unwrap().route("k1"),
            vec!["/queues/x"]
        );
        assert!(lookup(&defs, "", "d").unwrap().route("k3").is_empty());
        assert_eq!(
            lookup(&defs, "", "f").unwrap().route("anything"),
            vec!["/queues/x", "/queues/y"]
        );
    }

    #[test]
    fn exchange_addresses() {
        assert_eq!(
            parse_exchange_address("/exchanges/events/order.created"),
            Some(("events".into(), Some("order.created".into())))
        );
        assert_eq!(
            parse_exchange_address("/exchanges/events"),
            Some(("events".into(), None))
        );
        assert_eq!(
            parse_exchange_address("/exchanges/my%20ex/a%2Fb"),
            Some(("my ex".into(), Some("a/b".into())))
        );
        assert_eq!(
            parse_exchange_address("/exchanges/events/"),
            Some(("events".into(), Some(String::new())))
        );
        assert_eq!(parse_exchange_address("/exchanges/"), None);
        assert_eq!(parse_exchange_address("/exchanges/bad%zz"), None);
        assert_eq!(parse_exchange_address("/queues/x"), None);
    }

    #[test]
    fn subject_is_read_from_properties() {
        use bytes::BytesMut;
        use ramqp_core::codec::Encode;
        use ramqp_core::types::messaging::{Header, Properties};
        let mut buf = BytesMut::new();
        Header::default().encode(&mut buf);
        Properties {
            subject: Some("order.created".into()),
            ..Default::default()
        }
        .encode(&mut buf);
        assert_eq!(
            message_subject(&buf.freeze()),
            Some("order.created".to_owned())
        );
        assert_eq!(
            message_subject(&Bytes::from_static(b"\x00\x53\x77\x40")),
            None
        );
    }
}
