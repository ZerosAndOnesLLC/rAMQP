//! The management endpoint: metrics, inspection, and the admin API — a
//! deliberately tiny HTTP/1.1 server (one request per connection, bounded
//! head and body, no web framework, no new dependencies).
//!
//! Reads (open unless `management_auth_reads`):
//! - `GET /metrics` — Prometheus text: process RSS, connections, per-queue
//!   ready/unacked/consumer gauges.
//! - `GET /queues` — JSON: every declared queue with its kind and stats.
//! - `GET /queues/<vhost>/<name>[?kind=quorum|durable|transient]` — one
//!   queue's `ready`, `unacked`, `consumers`, `messages` (a single JSON
//!   object: what KEDA's `metrics-api` scaler polls).
//! - `GET /exchanges`, `GET /exchanges/<vhost>/<name>` — definitions with
//!   their bindings.
//!
//! Writes (always authenticated; idempotent — `201` created, `204` already
//! so, `409` exists with different arguments):
//! - `PUT /queues/<vhost>/<name>` body `{"kind": "quorum"}` (default
//!   `transient`), optionally with the queue's own policy fields
//!   (`dead_letter`, `max_delivery_attempts`, ...);
//!   `DELETE /queues/<vhost>/<name>[?kind=…]`.
//! - `PUT /exchanges/<vhost>/<name>` body `{"type": "topic", "alternate":
//!   "/queues/x"}`; `DELETE /exchanges/<vhost>/<name>`.
//! - `PUT` / `DELETE /bindings/<vhost>/<exchange>` body `{"queue":
//!   "/queues/x", "key": "order.*"}`.
//! - `PUT /users/<name>` body `{"password": "..."}` or `{"verifier":
//!   "SCRAM-SHA-256$..."}` (+ optional `"admin"`); `DELETE /users/<name>`.
//! - `PUT` / `DELETE /permissions/<vhost>/<user>` body `[{"address":
//!   "/queues/x", "operations": ["receive"]}]`.
//!
//! `GET /users` and `GET /permissions/<vhost>/<user>` always need
//! credentials (see [`users`]).
//!
//! Path segments are percent-decoded; the default vhost is `%2F` (as in
//! RabbitMQ's API). Credentials: `Authorization: Bearer <token>`
//! ([`crate::config::BrokerConfig::management_token`]) or `Basic` for a user
//! the authenticator marks as admin.
//!
//! Stats are collected by *asking* the queue actors (a `Stats` message
//! each) — nothing rides the message hot path (§3.2).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use serde_json::{Value as Json, json};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

mod users;

use crate::auth::Authenticator;
use crate::config::SecretString;
use crate::exchange::{ExchangeError, ExchangeKind, ExchangeOp};
use crate::queue::{QueueHandle, QueueMsg, QueueStats};
use crate::registry::{QueueDeleteError, QueueKind, QueueRegistry};

/// Broker-wide counters the endpoint exposes (updated with relaxed atomics —
/// negligible next to a network frame).
#[derive(Debug, Default)]
pub(crate) struct BrokerCounters {
    /// Currently established AMQP connections.
    pub connections: AtomicUsize,
    /// Connections accepted over the broker's lifetime.
    pub connections_total: AtomicUsize,
}

/// Whole-request deadline (read + respond): a socket that trickles bytes
/// forever must not park a task indefinitely (slow-loris guard).
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Concurrent management connections; excess sockets are dropped.
const MAX_CONCURRENT: usize = 64;
/// How long a client has to send the request head.
const HEAD_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(3);
/// TLS handshake bound on the management port.
#[cfg(any(feature = "tls", feature = "fips"))]
pub(crate) const TLS_HANDSHAKE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);
/// Request head bound.
const MAX_HEAD: usize = 8 * 1024;
/// Request body bound (definitions are tiny).
const MAX_BODY: usize = 64 * 1024;

/// Who may do what on the endpoint.
pub(crate) struct MgmtAuth {
    pub token: Option<SecretString>,
    pub auth_reads: bool,
    pub authenticator: Arc<dyn Authenticator>,
}

/// Wraps an accepted socket into the stream a request is served over
/// (identity for plain TCP; a TLS handshake when configured).
pub(crate) type Acceptor = Arc<
    dyn Fn(
            tokio::net::TcpStream,
        ) -> std::pin::Pin<Box<dyn Future<Output = Option<Box<dyn MgmtStream>>> + Send>>
        + Send
        + Sync,
>;

/// A byte stream the endpoint can serve.
pub(crate) trait MgmtStream: AsyncRead + AsyncWrite + Unpin + Send {}
impl<T: AsyncRead + AsyncWrite + Unpin + Send> MgmtStream for T {}

/// The plain-TCP acceptor.
pub(crate) fn plain_acceptor() -> Acceptor {
    Arc::new(|s: tokio::net::TcpStream| {
        Box::pin(async move { Some(Box::new(s) as Box<dyn MgmtStream>) })
    })
}

/// Serve the management endpoint until the registry (broker) goes away.
pub(crate) fn spawn_mgmt(
    listener: TcpListener,
    acceptor: Acceptor,
    registry: std::sync::Weak<QueueRegistry>,
    counters: Arc<BrokerCounters>,
    auth: Arc<MgmtAuth>,
) {
    let permits = Arc::new(tokio::sync::Semaphore::new(MAX_CONCURRENT));
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                continue;
            };
            let Some(registry) = registry.upgrade() else {
                return; // broker gone
            };
            let Ok(permit) = permits.clone().try_acquire_owned() else {
                continue; // at the cap: drop the socket
            };
            let _ = stream.set_nodelay(true);
            let (counters, auth, acceptor) = (counters.clone(), auth.clone(), acceptor.clone());
            tokio::spawn(async move {
                let _permit = permit;
                let _ = tokio::time::timeout(REQUEST_TIMEOUT, async move {
                    if let Some(stream) = acceptor(stream).await {
                        handle_request(stream, registry, counters, auth).await;
                    }
                })
                .await;
            });
        }
    });
}

/// A parsed request.
struct Request {
    method: String,
    path: String,
    query: String,
    authorization: Option<String>,
    body: Vec<u8>,
}

/// A response: status, content type, body.
struct Response {
    status: u16,
    content_type: &'static str,
    body: String,
}

impl Response {
    fn json(status: u16, value: &Json) -> Self {
        let mut body = value.to_string();
        body.push('\n');
        Response {
            status,
            content_type: "application/json",
            body,
        }
    }

    fn error(status: u16, message: impl std::fmt::Display) -> Self {
        Self::json(status, &json!({ "error": message.to_string() }))
    }

    fn empty(status: u16) -> Self {
        Response {
            status,
            content_type: "text/plain",
            body: String::new(),
        }
    }
}

async fn read_request(stream: &mut Box<dyn MgmtStream>) -> Result<Request, Response> {
    let mut buf = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];
    let head_end = loop {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| Response::empty(400))?;
        if n == 0 {
            return Err(Response::empty(400));
        }
        buf.extend_from_slice(&chunk[..n]);
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            break end;
        }
        if buf.len() > MAX_HEAD {
            return Err(Response::error(431, "request head too large"));
        }
    };
    let head = String::from_utf8_lossy(&buf[..head_end]).into_owned();
    let mut lines = head.split("\r\n");
    let mut request_line = lines.next().unwrap_or_default().split_whitespace();
    let (Some(method), Some(target)) = (request_line.next(), request_line.next()) else {
        return Err(Response::error(400, "malformed request line"));
    };
    let (path, query) = target.split_once('?').unwrap_or((target, ""));
    let mut content_length = 0usize;
    let mut authorization = None;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let value = value.trim();
        match name.trim().to_ascii_lowercase().as_str() {
            "content-length" => {
                content_length = value
                    .parse()
                    .map_err(|_| Response::error(400, "bad content-length"))?;
            }
            "transfer-encoding" => {
                return Err(Response::error(411, "send a content-length body"));
            }
            "authorization" => authorization = Some(value.to_owned()),
            _ => {}
        }
    }
    if content_length > MAX_BODY {
        return Err(Response::error(413, "request body too large"));
    }
    let mut body = buf[head_end + 4..].to_vec();
    while body.len() < content_length {
        let n = stream
            .read(&mut chunk)
            .await
            .map_err(|_| Response::empty(400))?;
        if n == 0 {
            return Err(Response::error(400, "truncated body"));
        }
        body.extend_from_slice(&chunk[..n]);
    }
    body.truncate(content_length);
    Ok(Request {
        method: method.to_owned(),
        path: path.to_owned(),
        query: query.to_owned(),
        authorization,
        body,
    })
}

async fn handle_request(
    mut stream: Box<dyn MgmtStream>,
    registry: Arc<QueueRegistry>,
    counters: Arc<BrokerCounters>,
    auth: Arc<MgmtAuth>,
) {
    // The head must arrive promptly: idle sockets must not hold one of the
    // few connection slots for the whole request deadline.
    let response = match tokio::time::timeout(HEAD_TIMEOUT, read_request(&mut stream)).await {
        Ok(Ok(req)) => route(req, &registry, &counters, &auth).await,
        Ok(Err(resp)) => resp,
        Err(_) => Response::error(408, "request not received in time"),
    };
    let _ = respond(&mut stream, response).await;
    // Swallow whatever the client still sends (an unread body behind an
    // early 401/413) until it closes: closing with unread data would reset
    // the connection and could destroy the response in flight.
    let mut sink = [0u8; 4096];
    let mut left = MAX_BODY;
    let _ = tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while left > 0 {
            match stream.read(&mut sink).await {
                Ok(0) | Err(_) => break,
                Ok(n) => left = left.saturating_sub(n),
            }
        }
    })
    .await;
}

/// Constant-time byte comparison.
fn ct_eq(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0u8, |d, (x, y)| d | (x ^ y)) == 0
}

/// Whether the request carries valid management credentials.
fn authenticated(req: &Request, auth: &MgmtAuth) -> bool {
    let Some(header) = req.authorization.as_deref() else {
        return false;
    };
    let (scheme, value) = header.split_once(' ').unwrap_or((header, ""));
    match scheme.to_ascii_lowercase().as_str() {
        "bearer" => auth
            .token
            .as_ref()
            .is_some_and(|t| ct_eq(t.expose().as_bytes(), value.trim().as_bytes())),
        "basic" => {
            use base64::Engine;
            let Ok(decoded) = base64::engine::general_purpose::STANDARD.decode(value.trim()) else {
                return false;
            };
            let Ok(text) = String::from_utf8(decoded) else {
                return false;
            };
            let Some((user, password)) = text.split_once(':') else {
                return false;
            };
            // Checked off the async workers in `route` (PBKDF2).
            let _ = (user, password);
            false
        }
        _ => false,
    }
}

/// [`authenticated`], plus Basic credentials checked on the blocking pool
/// (the authenticator runs PBKDF2).
async fn authenticated_async(req: &Request, auth: &MgmtAuth) -> bool {
    if authenticated(req, auth) {
        return true;
    }
    let Some(header) = req.authorization.as_deref() else {
        return false;
    };
    let Some((scheme, value)) = header.split_once(' ') else {
        return false;
    };
    if !scheme.eq_ignore_ascii_case("basic") {
        return false;
    }
    use base64::Engine;
    let Some((user, password)) = base64::engine::general_purpose::STANDARD
        .decode(value.trim())
        .ok()
        .and_then(|d| String::from_utf8(d).ok())
        .and_then(|t| t.split_once(':').map(|(u, p)| (u.to_owned(), p.to_owned())))
    else {
        return false;
    };
    let authenticator = auth.authenticator.clone();
    crate::auth::run_password_check(move || {
        authenticator
            .management_login(&user, &password)
            .then_some(())
    })
    .await
    .is_some()
}

/// Percent-decoded path segments; the RabbitMQ-style default vhost `%2F`
/// becomes `""` (callers treat the first segment after the collection as
/// the vhost).
fn segments(path: &str) -> Option<Vec<String>> {
    path.trim_start_matches('/')
        .split('/')
        .filter(|s| !s.is_empty())
        .map(crate::exchange::percent_decode)
        .collect()
}

fn vhost_of(segment: &str) -> &str {
    if segment == "/" { "" } else { segment }
}

fn query_param<'a>(query: &'a str, key: &str) -> Option<&'a str> {
    query
        .split('&')
        .filter_map(|kv| kv.split_once('='))
        .find(|(k, _)| *k == key)
        .map(|(_, v)| v)
}

fn parse_kind(s: &str) -> Option<QueueKind> {
    match s {
        "transient" | "classic" => Some(QueueKind::Transient),
        "quorum" => Some(QueueKind::Quorum),
        "durable" => Some(QueueKind::Durable),
        _ => None,
    }
}

fn kind_label(kind: QueueKind) -> &'static str {
    match kind {
        QueueKind::Transient => "transient",
        QueueKind::Quorum => "quorum",
        QueueKind::Durable => "durable",
    }
}

fn kind_prefix(kind: QueueKind) -> &'static str {
    match kind {
        QueueKind::Transient => "/queues/",
        QueueKind::Quorum => "/quorum/",
        QueueKind::Durable => "/durable/",
    }
}

fn body_json(req: &Request) -> Result<Json, Response> {
    if req.body.iter().all(u8::is_ascii_whitespace) {
        return Ok(json!({}));
    }
    serde_json::from_slice::<Json>(&req.body)
        .map_err(|e| Response::error(400, format!("invalid JSON body: {e}")))
        .and_then(|v| {
            if v.is_object() {
                Ok(v)
            } else {
                Err(Response::error(400, "the body must be a JSON object"))
            }
        })
}

fn exchange_error(e: ExchangeError) -> Response {
    match e {
        ExchangeError::Conflict(m) => Response::error(409, m),
        ExchangeError::NotFound(m) => Response::error(404, m),
        ExchangeError::Invalid(m) => Response::error(400, m),
        ExchangeError::Unavailable(m) => Response::error(503, m),
    }
}

async fn route(
    req: Request,
    registry: &Arc<QueueRegistry>,
    counters: &BrokerCounters,
    auth: &MgmtAuth,
) -> Response {
    let write = match req.method.as_str() {
        "GET" => false,
        "PUT" | "DELETE" => true,
        _ => return Response::error(405, "method not allowed"),
    };
    // Who exists and who may do what is never public, whatever auth_reads
    // says.
    let private = req.path.starts_with("/users") || req.path.starts_with("/permissions");
    if (write || auth.auth_reads || private) && !authenticated_async(&req, auth).await {
        // Slow every failed attempt down (with the connection cap this
        // bounds guessing to a few hundred tries per second).
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;
        let mut r = Response::error(
            401,
            "credentials required: Authorization: Bearer <management token>, \
             or Basic for an admin user",
        );
        r.content_type = "application/json";
        return r;
    }
    let Some(seg) = segments(&req.path) else {
        return Response::error(400, "malformed percent-encoding in path");
    };
    let seg: Vec<&str> = seg.iter().map(String::as_str).collect();
    match (req.method.as_str(), seg.as_slice()) {
        ("GET", ["metrics"]) => Response {
            status: 200,
            content_type: "text/plain; version=0.0.4; charset=utf-8",
            body: render_metrics(registry, counters).await,
        },
        ("GET", ["queues"]) => Response {
            status: 200,
            content_type: "application/json",
            body: render_queues(registry).await,
        },
        ("GET", ["queues", vhost, name]) => get_queue(registry, &req, vhost_of(vhost), name).await,
        ("PUT", ["queues", vhost, name]) => put_queue(registry, &req, vhost_of(vhost), name).await,
        ("DELETE", ["queues", vhost, name]) => {
            delete_queue(registry, &req, vhost_of(vhost), name).await
        }
        ("GET", ["users"]) => users::list(registry),
        ("PUT", ["users", name]) => users::put(registry, &req, name).await,
        ("DELETE", ["users", name]) => users::delete(registry, name).await,
        ("GET", ["permissions", vhost, user]) => {
            users::get_permissions(registry, vhost_of(vhost), user)
        }
        ("PUT", ["permissions", vhost, user]) => {
            users::put_permissions(registry, &req, vhost_of(vhost), user).await
        }
        ("DELETE", ["permissions", vhost, user]) => {
            users::delete_permissions(registry, vhost_of(vhost), user).await
        }
        ("GET", ["exchanges"]) => list_exchanges(registry, None).await,
        ("GET", ["exchanges", vhost, name]) => {
            list_exchanges(registry, Some((vhost_of(vhost), name))).await
        }
        ("PUT", ["exchanges", vhost, name]) => {
            let body = match body_json(&req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let kind = match body
                .get("type")
                .and_then(Json::as_str)
                .map(ExchangeKind::parse)
            {
                Some(Some(k)) => k,
                _ => return Response::error(400, "\"type\" must be fanout, direct, or topic"),
            };
            let alternate = match body.get("alternate") {
                None | Some(Json::Null) => None,
                Some(Json::String(a)) => Some(a.clone()),
                Some(_) => return Response::error(400, "\"alternate\" must be a string"),
            };
            change(
                registry,
                ExchangeOp::Declare {
                    vhost: vhost_of(vhost).to_owned(),
                    name: (*name).to_owned(),
                    kind,
                    alternate,
                },
            )
            .await
        }
        ("DELETE", ["exchanges", vhost, name]) => {
            change(
                registry,
                ExchangeOp::Delete {
                    vhost: vhost_of(vhost).to_owned(),
                    name: (*name).to_owned(),
                },
            )
            .await
        }
        (method @ ("PUT" | "DELETE"), ["bindings", vhost, exchange]) => {
            let body = match body_json(&req) {
                Ok(b) => b,
                Err(r) => return r,
            };
            let Some(queue) = body.get("queue").and_then(Json::as_str) else {
                return Response::error(400, "\"queue\" (a queue address) is required");
            };
            let key = match body.get("key") {
                None | Some(Json::Null) => String::new(),
                Some(Json::String(k)) => k.clone(),
                Some(_) => return Response::error(400, "\"key\" must be a string"),
            };
            let (vhost, exchange, destination) = (
                vhost_of(vhost).to_owned(),
                (*exchange).to_owned(),
                queue.to_owned(),
            );
            let op = if method == "PUT" {
                ExchangeOp::Bind {
                    vhost,
                    exchange,
                    destination,
                    key,
                }
            } else {
                ExchangeOp::Unbind {
                    vhost,
                    exchange,
                    destination,
                    key,
                }
            };
            change(registry, op).await
        }
        _ => Response::error(404, "not found"),
    }
}

/// Apply a topology change: `201` changed, `204` already so (or deleted).
async fn change(registry: &Arc<QueueRegistry>, op: ExchangeOp) -> Response {
    let deleting = matches!(op, ExchangeOp::Delete { .. } | ExchangeOp::Unbind { .. });
    // In its own task: the request deadline must not cancel a change midway
    // (persisted but not yet swapped into the routing view).
    let registry = registry.clone();
    let result = match tokio::spawn(async move { registry.exchange_op(op).await }).await {
        Ok(r) => r,
        Err(e) => return Response::error(500, e),
    };
    match result {
        Ok(_) if deleting => Response::empty(204),
        Ok(true) => Response::empty(201),
        Ok(false) => Response::empty(204),
        Err(e) => exchange_error(e),
    }
}

/// One queue's stats for a single-queue lookup: waits for mailbox room
/// (no `try_send`: a saturated queue is exactly when the number matters) and
/// follows a clustered queue to its leader. `None` = not knowable right now.
async fn queue_stats(handle: &QueueHandle) -> Option<QueueStats> {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        let (tx, rx) = oneshot::channel();
        handle
            .tx
            .send(QueueMsg::Stats {
                reply: tx,
                forward: true,
            })
            .await
            .ok()?;
        rx.await.ok()
    })
    .await
    .ok()
    .flatten()
}

fn requested_kind(req: &Request) -> Result<Option<QueueKind>, Response> {
    match query_param(&req.query, "kind") {
        None => Ok(None),
        Some(k) => parse_kind(k)
            .map(Some)
            .ok_or_else(|| Response::error(400, "kind must be transient, quorum, or durable")),
    }
}

async fn get_queue(registry: &QueueRegistry, req: &Request, vhost: &str, name: &str) -> Response {
    let kind = match requested_kind(req) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let Some(qualified) = QueueRegistry::qualify(vhost, name) else {
        return Response::error(400, "invalid vhost or queue name");
    };
    let Some((kind, handle)) = registry.find_queue(kind, &qualified).await else {
        return Response::error(404, "no such queue");
    };
    let Some(stats) = queue_stats(&handle).await else {
        // Never report zeros for an unknown backlog: an autoscaler would
        // scale consumers away from a full queue.
        return Response::error(503, "queue statistics unavailable right now; retry");
    };
    Response::json(
        200,
        &json!({
            "vhost": vhost,
            "name": name,
            "kind": kind_label(kind),
            "address": format!("{}{name}", kind_prefix(kind)),
            "ready": stats.ready,
            "unacked": stats.unacked,
            "messages": stats.ready + stats.unacked,
            "consumers": stats.consumers,
            "policy": registry.own_policy(&qualified).map(|p| p.to_json()),
        }),
    )
}

async fn put_queue(
    registry: &Arc<QueueRegistry>,
    req: &Request,
    vhost: &str,
    name: &str,
) -> Response {
    let body = match body_json(req) {
        Ok(b) => b,
        Err(r) => return r,
    };
    let kind = match body.get("kind").or_else(|| body.get("type")) {
        None | Some(Json::Null) => QueueKind::Transient,
        Some(Json::String(k)) => match parse_kind(k) {
            Some(k) => k,
            None => return Response::error(400, "kind must be transient, quorum, or durable"),
        },
        Some(_) => return Response::error(400, "\"kind\" must be a string"),
    };
    let Some(qualified) = QueueRegistry::qualify(vhost, name) else {
        return Response::error(400, "invalid vhost or queue name");
    };
    // The queue's own policy (dead-letter target, retry budget, limits):
    // part of the declaration, like `kind`.
    let policy = match crate::defs::PolicySpec::from_json(&body) {
        Ok(p) => p,
        Err(e) => return Response::error(400, e),
    };
    if policy.is_some() && registry.has_config_policy(&qualified) {
        return Response::error(
            409,
            format!("queue {name:?} has its policy set in the configuration file"),
        );
    }
    match registry.find_queue(None, &qualified).await {
        Some((existing, _)) if existing == kind => {
            let current = registry.own_policy(&qualified);
            return if registry.has_config_policy(&qualified) || current == policy {
                Response::empty(204)
            } else {
                Response::error(
                    409,
                    format!(
                        "queue {name:?} exists with a different policy (delete it to change it)"
                    ),
                )
            };
        }
        Some((existing, _)) => {
            return Response::error(
                409,
                format!("queue {name:?} exists as {}", kind_label(existing)),
            );
        }
        None => {}
    }
    let address = format!("{}{name}", kind_prefix(kind));
    // The policy is written first: the queue applies it when it starts.
    let key = crate::defs::queue_policy_key(&qualified);
    let value = match &policy {
        Some(p) => match crate::serde_bin::to_vec(p) {
            Ok(v) => Some(v),
            Err(e) => return Response::error(500, e.to_string()),
        },
        None => None,
    };
    if let Err(e) = registry.def_write(&key, value).await {
        return Response::error(503, format!("could not store the queue policy: {e}"));
    }
    // Explicit kind: `/queues/<name>` here means a transient queue.
    let (reg, vh, bare) = (registry.clone(), vhost.to_owned(), name.to_owned());
    let declared = tokio::spawn(async move { reg.declare_in(&vh, kind, &bare).await.is_some() })
        .await
        .unwrap_or(false);
    if !declared {
        if policy.is_some() {
            let _ = registry.def_write(&key, None).await;
        }
        return Response::error(
            503,
            format!("could not declare {address} (limits, store, or cluster unavailable)"),
        );
    }
    Response::empty(201)
}

async fn delete_queue(
    registry: &Arc<QueueRegistry>,
    req: &Request,
    vhost: &str,
    name: &str,
) -> Response {
    let kind = match requested_kind(req) {
        Ok(k) => k,
        Err(r) => return r,
    };
    let Some(qualified) = QueueRegistry::qualify(vhost, name) else {
        return Response::error(400, "invalid vhost or queue name");
    };
    let kind = match kind {
        Some(k) => k,
        None => match registry.kinds_of(&qualified).as_slice() {
            [k] => *k,
            [] => match registry.find_queue(None, &qualified).await {
                Some((k, _)) => k,
                None => return Response::error(404, "no such queue"),
            },
            _ => {
                return Response::error(
                    409,
                    "several queues share this name; pass ?kind=transient|quorum|durable",
                );
            }
        },
    };
    let reg = registry.clone();
    let outcome = tokio::spawn(async move { reg.delete_queue(kind, &qualified).await })
        .await
        .unwrap_or(Err(QueueDeleteError::Failed));
    match outcome {
        Ok(()) => Response::empty(204),
        Err(QueueDeleteError::NotFound) => Response::error(404, "no such queue"),
        Err(QueueDeleteError::Unavailable(why)) => Response::error(503, why),
        Err(QueueDeleteError::Failed) => Response::error(500, "queue deletion failed"),
    }
}

async fn list_exchanges(registry: &QueueRegistry, one: Option<(&str, &str)>) -> Response {
    if !registry.ensure_exchanges_loaded().await {
        return Response::error(503, "exchange definitions not loaded yet");
    }
    let defs = registry.exchanges().snapshot();
    let render = |vhost: &str, name: &str, def: &crate::exchange::ExchangeDef| {
        json!({
            "vhost": vhost,
            "name": name,
            "type": def.kind.as_str(),
            "alternate": def.alternate,
            "bindings": def.bindings.iter().map(|b| json!({
                "queue": b.destination,
                "key": b.key,
            })).collect::<Vec<_>>(),
        })
    };
    match one {
        Some((vhost, name)) => match crate::exchange::lookup(&defs, vhost, name) {
            Some(def) => Response::json(200, &render(vhost, name, &def)),
            None => Response::error(404, "no such exchange"),
        },
        None => {
            let all: Vec<Json> = defs
                .iter()
                .map(|(key, def)| {
                    let (vhost, name) = key.split_once('/').unwrap_or(("", key));
                    render(vhost, name, def)
                })
                .collect();
            Response::json(200, &Json::Array(all))
        }
    }
}

async fn respond(stream: &mut Box<dyn MgmtStream>, response: Response) -> std::io::Result<()> {
    let reason = match response.status {
        200 => "OK",
        201 => "Created",
        204 => "No Content",
        400 => "Bad Request",
        401 => "Unauthorized",
        404 => "Not Found",
        408 => "Request Timeout",
        405 => "Method Not Allowed",
        409 => "Conflict",
        411 => "Length Required",
        413 => "Content Too Large",
        431 => "Request Header Fields Too Large",
        500 => "Internal Server Error",
        501 => "Not Implemented",
        503 => "Service Unavailable",
        _ => "Unknown",
    };
    let mut head = format!(
        "HTTP/1.1 {} {reason}\r\ncontent-type: {}\r\ncontent-length: {}\r\nconnection: close\r\n",
        response.status,
        response.content_type,
        response.body.len()
    );
    if response.status == 401 {
        head.push_str("www-authenticate: Bearer realm=\"ramqp\", Basic realm=\"ramqp\"\r\n");
    }
    head.push_str("\r\n");
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(response.body.as_bytes()).await?;
    stream.flush().await?;
    stream.shutdown().await
}

/// The node-local scrape: `(key like "t:name", stats)` for each queue this
/// node can report.
async fn collect(registry: &QueueRegistry) -> Vec<(String, QueueStats)> {
    let mut out = Vec::new();
    for (key, handle) in registry.queues() {
        let (tx, rx) = oneshot::channel();
        let sent = handle
            .tx
            .try_send(QueueMsg::Stats {
                reply: tx,
                forward: false,
            })
            .is_ok();
        // Omitted — not zeroed — when unknown here: a busy actor, or a
        // clustered queue another node leads (that node reports it).
        if sent
            && let Ok(Ok(stats)) =
                tokio::time::timeout(std::time::Duration::from_millis(200), rx).await
        {
            out.push((key, stats));
        }
    }
    out
}

fn kind_name(key: &str) -> (&'static str, &str) {
    match key.split_once(':') {
        Some(("t", name)) => ("transient", name),
        Some(("q", name)) => ("quorum", name),
        Some(("d", name)) => ("durable", name),
        _ => ("unknown", key),
    }
}

async fn render_metrics(registry: &QueueRegistry, counters: &BrokerCounters) -> String {
    let mut out = String::with_capacity(1024);
    out.push_str("# TYPE ramqp_connections gauge\n");
    out.push_str(&format!(
        "ramqp_connections {}\n",
        counters.connections.load(Ordering::Relaxed)
    ));
    out.push_str("# TYPE ramqp_connections_total counter\n");
    out.push_str(&format!(
        "ramqp_connections_total {}\n",
        counters.connections_total.load(Ordering::Relaxed)
    ));
    if let Some(rss) = rss_bytes() {
        out.push_str("# TYPE ramqp_process_resident_bytes gauge\n");
        out.push_str(&format!("ramqp_process_resident_bytes {rss}\n"));
    }
    let queues = collect(registry).await;
    out.push_str("# TYPE ramqp_queue_ready gauge\n");
    out.push_str("# TYPE ramqp_queue_unacked gauge\n");
    out.push_str("# TYPE ramqp_queue_consumers gauge\n");
    for (key, stats) in &queues {
        let (kind, name) = kind_name(key);
        let name = escape_label(name);
        out.push_str(&format!(
            "ramqp_queue_ready{{queue=\"{name}\",kind=\"{kind}\"}} {}\n",
            stats.ready
        ));
        out.push_str(&format!(
            "ramqp_queue_unacked{{queue=\"{name}\",kind=\"{kind}\"}} {}\n",
            stats.unacked
        ));
        out.push_str(&format!(
            "ramqp_queue_consumers{{queue=\"{name}\",kind=\"{kind}\"}} {}\n",
            stats.consumers
        ));
    }
    out
}

async fn render_queues(registry: &QueueRegistry) -> String {
    let queues = collect(registry).await;
    let mut out = String::from("[");
    for (i, (key, stats)) in queues.iter().enumerate() {
        let (kind, name) = kind_name(key);
        if i > 0 {
            out.push(',');
        }
        out.push_str(&format!(
            "{{\"name\":\"{}\",\"kind\":\"{kind}\",\"ready\":{},\"unacked\":{},\"consumers\":{}}}",
            escape_json(name),
            stats.ready,
            stats.unacked,
            stats.consumers
        ));
    }
    out.push_str("]\n");
    out
}

/// Prometheus label-value escaping (text exposition format): backslash,
/// double-quote, and newline get their escapes; any other control character
/// is dropped — a name with a raw `\n` would otherwise inject arbitrary
/// sample lines into the scrape (alert spoofing).
fn escape_label(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c if c.is_control() => {}
            c => out.push(c),
        }
    }
    out
}

/// JSON string escaping per RFC 8259: quotes, backslashes, and ALL control
/// characters (a raw newline in a name yields invalid JSON).
fn escape_json(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for c in s.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => out.push(c),
        }
    }
    out
}

fn rss_bytes() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let kib: u64 = status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()?;
    Some(kib * 1024)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// MED-18 (issue #19): a name carrying a newline must not inject
    /// Prometheus sample lines or break the JSON document.
    #[test]
    fn escapes_neutralize_control_characters() {
        let hostile = "q\"\\\ninjected_metric 1\r\x01";
        let label = escape_label(hostile);
        assert!(!label.contains('\n'), "no raw newline in a label: {label}");
        assert!(!label.contains('\r'));
        assert_eq!(label, "q\\\"\\\\\\ninjected_metric 1");

        let json = escape_json(hostile);
        assert!(!json.contains('\n') && !json.contains('\r'));
        assert_eq!(json, "q\\\"\\\\\\ninjected_metric 1\\r\\u0001");
    }

    #[test]
    fn path_segments_decode_and_default_vhost() {
        let s = segments("/queues/%2F/orders%2Eeu").unwrap();
        assert_eq!(s, vec!["queues", "/", "orders.eu"]);
        assert_eq!(vhost_of(&s[1]), "");
        assert!(segments("/queues/%zz/x").is_none());
        assert_eq!(query_param("a=1&kind=quorum", "kind"), Some("quorum"));
    }
}
