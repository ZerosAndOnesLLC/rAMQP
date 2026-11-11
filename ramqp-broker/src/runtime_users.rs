//! Users and permissions created through the management API, layered over
//! the configuration file's users.
//!
//! The configuration stays the source of truth for what it declares: its
//! users cannot be changed or deleted at runtime. Runtime users live in the
//! runtime definitions (`crate::defs`) — persisted, and replicated when
//! clustered — and this authenticator is rebuilt whenever they change.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use ramqp_core::sasl::scram::ScramMechanism;
use ramqp_core::sasl::server::ScramVerifier;

use crate::auth::{Authenticator, Credentials, Operation, Permission, StaticUsers};
use crate::defs::Defs;

/// The configuration's users plus the runtime ones.
#[derive(Debug)]
pub struct RuntimeUsers {
    base: StaticUsers,
    current: RwLock<Arc<StaticUsers>>,
    mechs: RwLock<&'static [&'static str]>,
    /// Runtime users present in the last applied definitions.
    known: Mutex<HashSet<String>>,
    /// The user and permission entries last applied (rebuild only when
    /// they change — a queue-policy write leaves users alone).
    applied: Mutex<Vec<(String, Vec<u8>)>>,
    /// Parsed verifiers by their stored text (parsing is not free).
    parsed: Mutex<HashMap<String, (ScramMechanism, ScramVerifier)>>,
    /// Bumped whenever a runtime user is removed: every connection then
    /// re-checks that its user still exists (a generation, not a list of
    /// names, so coalesced notifications lose nothing).
    revoked: tokio::sync::watch::Sender<u64>,
}

impl RuntimeUsers {
    /// Start from the configuration's users.
    pub fn new(base: StaticUsers) -> Self {
        let mechs = intern(base.mechanisms());
        RuntimeUsers {
            current: RwLock::new(Arc::new(base.clone())),
            base,
            mechs: RwLock::new(mechs),
            known: Mutex::new(HashSet::new()),
            applied: Mutex::new(Vec::new()),
            parsed: Mutex::new(HashMap::new()),
            revoked: tokio::sync::watch::channel(0).0,
        }
    }

    /// Whether `name` is declared in the configuration (read-only).
    pub fn is_config_user(&self, name: &str) -> bool {
        self.base.has_user(name)
    }

    /// The configuration's users: `(name, admin)`.
    pub fn config_users(&self) -> Vec<(String, bool)> {
        self.base.user_names()
    }

    /// Whether `name` exists (configuration or runtime).
    pub fn has_user(&self, name: &str) -> bool {
        self.current().has_user(name)
    }

    /// Changes whenever a runtime user is removed; re-check
    /// [`Self::has_user`] then.
    pub fn revocations(&self) -> tokio::sync::watch::Receiver<u64> {
        self.revoked.subscribe()
    }

    fn current(&self) -> Arc<StaticUsers> {
        self.current.read().expect("users lock").clone()
    }

    /// Rebuild from the definitions. Runtime users whose name the
    /// configuration already declares are ignored (the API refuses them;
    /// this guards definitions written before a config change).
    pub fn apply(&self, defs: &Defs) {
        let relevant = defs.user_and_permission_entries();
        {
            let mut applied = self.applied.lock().expect("applied lock");
            if *applied == relevant {
                return;
            }
            *applied = relevant;
        }
        let mut present = HashSet::new();
        let mut verifiers = Vec::new();
        let mut admins = Vec::new();
        {
            let mut parsed = self.parsed.lock().expect("parsed lock");
            for (name, def) in defs.users() {
                if self.base.has_user(name) {
                    tracing::warn!(
                        user = name,
                        "runtime user shadowed by a configured user; ignored"
                    );
                    continue;
                }
                let mut any = false;
                for text in &def.verifiers {
                    let parsed_one = match parsed.get(text) {
                        Some(p) => Some(p.clone()),
                        None => match crate::auth::parse_scram_verifier(text) {
                            Ok(p) => {
                                parsed.insert(text.clone(), p.clone());
                                Some(p)
                            }
                            Err(e) => {
                                tracing::warn!(user = name, error = %e, "unreadable stored verifier");
                                None
                            }
                        },
                    };
                    if let Some((mechanism, verifier)) = parsed_one {
                        verifiers.push((name.to_owned(), mechanism, verifier));
                        any = true;
                    }
                }
                if any {
                    if def.admin {
                        admins.push(name.to_owned());
                    }
                    present.insert(name.to_owned());
                }
            }
            // Forget parses of verifiers no longer stored.
            let live: HashSet<String> = defs.users().flat_map(|(_, d)| d.verifiers).collect();
            parsed.retain(|text, _| live.contains(text));
        }
        let mut users = self.base.clone().with_verifiers(verifiers);
        for a in admins {
            users = users.with_admin(a);
        }
        // Runtime grants apply to runtime users only: configuration users'
        // access is the file's.
        for (vhost, user, grants) in defs.permissions() {
            if !present.contains(user) {
                continue;
            }
            for g in grants {
                users = users.with_permission(Permission::new(
                    user.to_owned(),
                    vhost.to_owned(),
                    g.address.clone(),
                    g.send,
                    g.receive,
                ));
            }
        }
        *self.mechs.write().expect("mechs lock") = intern(users.mechanisms());
        *self.current.write().expect("users lock") = Arc::new(users);
        let removed: HashSet<String> = {
            let mut known = self.known.lock().expect("known lock");
            let removed = known.difference(&present).cloned().collect();
            *known = present;
            removed
        };
        if !removed.is_empty() {
            tracing::info!(users = ?removed, "runtime users removed; closing their connections");
            self.revoked.send_modify(|g| *g += 1);
        }
    }
}

/// Mechanism lists are borrowed for the authenticator's lifetime, but ours
/// change at runtime: intern each distinct list (a handful at most — the
/// subsets of five mechanism names).
fn intern(mechs: &[&'static str]) -> &'static [&'static str] {
    static LISTS: OnceLock<Mutex<HashMap<Vec<&'static str>, &'static [&'static str]>>> =
        OnceLock::new();
    let mut lists = LISTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .expect("mechanism lists lock");
    lists
        .entry(mechs.to_vec())
        .or_insert_with(|| Box::leak(mechs.to_vec().into_boxed_slice()))
}

impl Authenticator for RuntimeUsers {
    fn mechanisms(&self) -> &[&'static str] {
        *self.mechs.read().expect("mechs lock")
    }

    fn verify(&self, credentials: Credentials<'_>) -> bool {
        self.current().verify(credentials)
    }

    fn allow_unauthenticated(&self) -> bool {
        self.current().allow_unauthenticated()
    }

    fn scram_verifier(&self, mechanism: ScramMechanism, username: &str) -> Option<ScramVerifier> {
        self.current().scram_verifier(mechanism, username)
    }

    fn authorize(
        &self,
        identity: Option<&str>,
        vhost: &str,
        address: &str,
        operation: Operation,
    ) -> bool {
        self.current()
            .authorize(identity, vhost, address, operation)
    }

    fn management_login(&self, username: &str, password: &str) -> bool {
        self.current().management_login(username, password)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::defs::{Grant, UserDef, perm_key, user_key};

    fn defs_with(entries: Vec<(String, Vec<u8>)>) -> Defs {
        Defs {
            epoch: 1,
            map: entries.into_iter().collect(),
        }
    }

    fn user(password: &str, admin: bool) -> Vec<u8> {
        let verifiers = crate::auth::password_verifiers(password)
            .unwrap()
            .into_iter()
            .map(|(m, v)| crate::auth::format_scram_verifier(m, &v))
            .collect();
        crate::serde_bin::to_vec(&UserDef { verifiers, admin }).unwrap()
    }

    #[test]
    fn runtime_users_log_in_get_grants_and_are_revoked() {
        let base = StaticUsers::new()
            .with_password("ops", "ops-pw")
            .with_admin("ops");
        let users = RuntimeUsers::new(base);
        let revoked = users.revocations();
        let grants = vec![Grant {
            address: "/queues/jobs".into(),
            send: false,
            receive: true,
        }];
        users.apply(&defs_with(vec![
            (user_key("reader"), user("reader-pw", false)),
            (
                perm_key("", "reader"),
                crate::serde_bin::to_vec(&grants).unwrap(),
            ),
        ]));
        let plain = |u, p| Credentials::Plain {
            authcid: u,
            passwd: p,
        };
        assert!(users.verify(plain("reader", "reader-pw")));
        assert!(!users.verify(plain("reader", "nope")));
        assert!(
            users.verify(plain("ops", "ops-pw")),
            "config users still work"
        );
        assert!(users.authorize(Some("reader"), "", "/queues/jobs", Operation::Receive));
        assert!(!users.authorize(Some("reader"), "", "/queues/jobs", Operation::Send));
        assert!(!users.authorize(Some("reader"), "", "/queues/other", Operation::Receive));
        assert!(
            !users.management_login("reader", "reader-pw"),
            "not an admin"
        );
        assert!(users.mechanisms().contains(&"PLAIN"));
        assert!(!revoked.has_changed().unwrap());

        // Deleted: can no longer log in, and its connections are told.
        users.apply(&defs_with(vec![]));
        assert!(!users.verify(plain("reader", "reader-pw")));
        assert!(revoked.has_changed().unwrap());
        assert!(!users.has_user("reader"));
        assert!(users.verify(plain("ops", "ops-pw")));
    }

    #[test]
    fn config_users_cannot_be_shadowed() {
        let users = RuntimeUsers::new(StaticUsers::new().with_password("ops", "ops-pw"));
        users.apply(&defs_with(vec![(user_key("ops"), user("other", true))]));
        assert!(users.is_config_user("ops"));
        assert!(!users.verify(Credentials::Plain {
            authcid: "ops",
            passwd: "other"
        }));
        assert!(!users.management_login("ops", "other"));
    }
}
