//! `/users` and `/permissions`: runtime user and permission management
//! (admin-only; reads need credentials too). Users from the configuration
//! file are listed but read-only.

use std::sync::Arc;

use serde_json::{Value as Json, json};

use super::{Request, Response, body_json};
use crate::defs::{Grant, UserDef, perm_key, perm_keys_of, user_key};
use crate::registry::QueueRegistry;
use crate::runtime_users::RuntimeUsers;

fn runtime(registry: &QueueRegistry) -> Result<&Arc<RuntimeUsers>, Response> {
    registry.runtime_users().ok_or_else(|| {
        Response::error(
            501,
            "runtime users need the built-in user store (configure [[users]] in the \
             ramqp-brokerd config file, or Broker::with_runtime_users when embedding)",
        )
    })
}

/// A usable user name: non-empty, at most 255 bytes, no control characters,
/// and not the reserved anonymous principal.
fn valid_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 255
        && !name.chars().any(char::is_control)
        && name != crate::auth::ANONYMOUS_USER
}

fn encode<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, Response> {
    crate::serde_bin::to_vec(value).map_err(|e| Response::error(500, e.to_string()))
}

pub(super) fn list(registry: &QueueRegistry) -> Response {
    let runtime = match runtime(registry) {
        Ok(r) => r,
        Err(r) => return r,
    };
    let mut users: Vec<Json> = runtime
        .config_users()
        .into_iter()
        .map(|(name, admin)| json!({"name": name, "admin": admin, "source": "config"}))
        .collect();
    let defs = registry.defs();
    users.extend(
        defs.users()
            .filter(|(name, _)| !runtime.is_config_user(name))
            .map(|(name, u)| json!({"name": name, "admin": u.admin, "source": "api"})),
    );
    Response::json(200, &Json::Array(users))
}

pub(super) async fn put(registry: &Arc<QueueRegistry>, req: &Request, name: &str) -> Response {
    let runtime = match runtime(registry) {
        Ok(r) => r,
        Err(r) => return r,
    };
    if !valid_name(name) {
        return Response::error(400, "invalid user name");
    }
    if runtime.is_config_user(name) {
        return Response::error(
            409,
            format!("user {name:?} is defined in the configuration file"),
        );
    }
    let body = match body_json(req) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let admin = match body.get("admin") {
        None | Some(Json::Null) => false,
        Some(Json::Bool(b)) => *b,
        Some(_) => return Response::error(400, "\"admin\" must be a boolean"),
    };
    let verifiers = match (body.get("password"), body.get("verifier")) {
        (Some(Json::String(password)), None) => {
            if password.is_empty() {
                return Response::error(400, "\"password\" must not be empty");
            }
            // PBKDF2 for two mechanisms: off the async threads. The
            // password is turned into verifiers here and never stored.
            let password = password.clone();
            let derived =
                tokio::task::spawn_blocking(move || crate::auth::password_verifiers(&password))
                    .await
                    .ok()
                    .flatten();
            match derived {
                Some(vs) => vs
                    .into_iter()
                    .map(|(m, v)| crate::auth::format_scram_verifier(m, &v))
                    .collect::<Vec<_>>(),
                None => return Response::error(400, "password rejected by SASLprep"),
            }
        }
        (None, Some(v)) => {
            let texts: Vec<&str> = match v {
                Json::String(s) => vec![s.as_str()],
                Json::Array(items) => match items.iter().map(Json::as_str).collect::<Option<_>>() {
                    Some(t) => t,
                    None => return Response::error(400, "\"verifier\" entries must be strings"),
                },
                _ => return Response::error(400, "\"verifier\" must be a string or an array"),
            };
            let mut out = Vec::new();
            for t in texts {
                match crate::auth::parse_scram_verifier(t) {
                    Ok((m, v)) => out.push(crate::auth::format_scram_verifier(m, &v)),
                    Err(e) => return Response::error(400, format!("bad verifier: {e}")),
                }
            }
            if out.is_empty() {
                return Response::error(400, "\"verifier\" must not be empty");
            }
            out
        }
        _ => {
            return Response::error(
                400,
                "give exactly one of \"password\" (string) or \"verifier\" (RFC 5803 string or array)",
            );
        }
    };
    let key = user_key(name);
    let defs = registry.defs();
    let existed = defs.map.contains_key(&key);
    // A new user starts with no grants: drop any left over from an earlier
    // user of this name (a delete racing a permissions write can leave one).
    if !existed {
        for perm in perm_keys_of(&defs, name) {
            if let Err(e) = registry.def_write(&perm, None).await {
                return Response::error(503, format!("could not clear stale permissions: {e}"));
            }
        }
    }
    let value = match encode(&UserDef { verifiers, admin }) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match registry.def_write(&key, Some(value)).await {
        Ok(_) if existed => Response::empty(204),
        Ok(_) => Response::empty(201),
        Err(e) => Response::error(503, format!("could not store the user: {e}")),
    }
}

pub(super) async fn delete(registry: &Arc<QueueRegistry>, name: &str) -> Response {
    let runtime = match runtime(registry) {
        Ok(r) => r,
        Err(r) => return r,
    };
    if runtime.is_config_user(name) {
        return Response::error(
            409,
            format!("user {name:?} is defined in the configuration file"),
        );
    }
    let key = user_key(name);
    let defs = registry.defs();
    if !defs.map.contains_key(&key) {
        return Response::error(404, "no such user");
    }
    // Its permissions go with it (RabbitMQ semantics); the user itself last,
    // so a partial failure leaves a user that can be deleted again.
    for perm in perm_keys_of(&defs, name) {
        if let Err(e) = registry.def_write(&perm, None).await {
            return Response::error(503, format!("could not remove the user's permissions: {e}"));
        }
    }
    if let Err(e) = registry.def_write(&key, None).await {
        return Response::error(503, format!("could not remove the user: {e}"));
    }
    // Grants written while the delete ran (orphans otherwise).
    for perm in perm_keys_of(&registry.defs(), name) {
        let _ = registry.def_write(&perm, None).await;
    }
    Response::empty(204)
}

pub(super) fn get_permissions(registry: &QueueRegistry, vhost: &str, user: &str) -> Response {
    if let Err(r) = runtime(registry) {
        return r;
    }
    match registry.defs().get::<Vec<Grant>>(&perm_key(vhost, user)) {
        Some(grants) => Response::json(
            200,
            &Json::Array(
                grants
                    .into_iter()
                    .map(|g| {
                        let mut ops = Vec::new();
                        if g.send {
                            ops.push("send");
                        }
                        if g.receive {
                            ops.push("receive");
                        }
                        json!({"address": g.address, "operations": ops})
                    })
                    .collect(),
            ),
        ),
        None => Response::error(
            404,
            "no permissions set through the API for this user and vhost",
        ),
    }
}

pub(super) async fn put_permissions(
    registry: &Arc<QueueRegistry>,
    req: &Request,
    vhost: &str,
    user: &str,
) -> Response {
    let runtime = match runtime(registry) {
        Ok(r) => r,
        Err(r) => return r,
    };
    if !QueueRegistry::valid_vhost(vhost) || !valid_name(user) {
        return Response::error(400, "invalid vhost or user name");
    }
    if runtime.is_config_user(user) {
        return Response::error(
            409,
            format!(
                "user {user:?} is defined in the configuration file (grant in [[permissions]])"
            ),
        );
    }
    if !registry.defs().map.contains_key(&user_key(user)) {
        return Response::error(404, format!("no runtime user {user:?}"));
    }
    // An array of grants (or one grant object).
    let body: Json = match serde_json::from_slice(&req.body) {
        Ok(b) => b,
        Err(e) => return Response::error(400, format!("invalid JSON body: {e}")),
    };
    let grants = match Grant::list_from_json(&body) {
        Ok(g) => g,
        Err(e) => return Response::error(400, e),
    };
    let key = perm_key(vhost, user);
    let existed = registry.defs().map.contains_key(&key);
    let value = match encode(&grants) {
        Ok(v) => v,
        Err(r) => return r,
    };
    match registry.def_write(&key, Some(value)).await {
        Ok(_) if existed => Response::empty(204),
        Ok(_) => Response::empty(201),
        Err(e) => Response::error(503, format!("could not store the permissions: {e}")),
    }
}

pub(super) async fn delete_permissions(
    registry: &Arc<QueueRegistry>,
    vhost: &str,
    user: &str,
) -> Response {
    if let Err(r) = runtime(registry) {
        return r;
    }
    let key = perm_key(vhost, user);
    if !registry.defs().map.contains_key(&key) {
        return Response::error(
            404,
            "no permissions set through the API for this user and vhost",
        );
    }
    match registry.def_write(&key, None).await {
        Ok(_) => Response::empty(204),
        Err(e) => Response::error(503, format!("could not remove the permissions: {e}")),
    }
}
