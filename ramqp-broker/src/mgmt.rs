//! The management/metrics endpoint (broker.md Phase 9): a deliberately tiny
//! HTTP/1.1 GET server — no web framework, no new dependencies.
//!
//! - `GET /metrics` — Prometheus text exposition: process RSS, connection
//!   count, per-queue depth/unacked/consumer gauges.
//! - `GET /queues` — JSON: every declared queue with its kind and stats.
//!
//! Everything is collected at scrape time by *asking* the queue actors
//! (a `Stats` message each) — nothing rides the message hot path (§3.2).
//! Bind it to loopback or a management network; there is no auth on this
//! surface yet.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::oneshot;

use crate::queue::{QueueMsg, QueueStats};
use crate::registry::QueueRegistry;

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
/// forever must not park a task indefinitely (slow-loris guard — the AMQP
/// path has the same bound via its handshake timeout).
const REQUEST_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
/// Concurrent management connections; excess sockets are dropped instead of
/// spawning unbounded tasks.
const MAX_CONCURRENT: usize = 64;

/// Serve the management endpoint until the registry (broker) goes away.
pub(crate) fn spawn_mgmt(
    listener: TcpListener,
    registry: std::sync::Weak<QueueRegistry>,
    counters: Arc<BrokerCounters>,
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
            let counters = counters.clone();
            tokio::spawn(async move {
                let _permit = permit;
                let _ = tokio::time::timeout(
                    REQUEST_TIMEOUT,
                    handle_request(stream, registry, counters),
                )
                .await;
            });
        }
    });
}

/// One GET request: parse the head, render, respond.
async fn handle_request(
    mut stream: tokio::net::TcpStream,
    registry: Arc<QueueRegistry>,
    counters: Arc<BrokerCounters>,
) {
    let _ = stream.set_nodelay(true);
    // Read the request head (bounded; GETs only).
    let mut buf = [0u8; 4096];
    let mut used = 0usize;
    let path = loop {
        let Ok(n) = stream.read(&mut buf[used..]).await else {
            return;
        };
        if n == 0 {
            return;
        }
        used += n;
        if let Some(head_end) = find_head_end(&buf[..used]) {
            let head = String::from_utf8_lossy(&buf[..head_end]);
            let mut parts = head.split_whitespace();
            match (parts.next(), parts.next()) {
                (Some("GET"), Some(path)) => break path.to_owned(),
                _ => {
                    let _ = respond(&mut stream, 405, "text/plain", "GET only\n").await;
                    return;
                }
            }
        }
        if used == buf.len() {
            return; // oversized head
        }
    };

    match path.as_str() {
        "/metrics" => {
            let body = render_metrics(&registry, &counters).await;
            let _ = respond(
                &mut stream,
                200,
                "text/plain; version=0.0.4; charset=utf-8",
                &body,
            )
            .await;
        }
        "/queues" => {
            let body = render_queues(&registry).await;
            let _ = respond(&mut stream, 200, "application/json", &body).await;
        }
        _ => {
            let _ = respond(&mut stream, 404, "text/plain", "not found\n").await;
        }
    }
}

fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n")
}

async fn respond(
    stream: &mut tokio::net::TcpStream,
    status: u16,
    content_type: &str,
    body: &str,
) -> std::io::Result<()> {
    let reason = match status {
        200 => "OK",
        404 => "Not Found",
        _ => "Method Not Allowed",
    };
    let head = format!(
        "HTTP/1.1 {status} {reason}\r\ncontent-type: {content_type}\r\ncontent-length: {}\r\nconnection: close\r\n\r\n",
        body.len()
    );
    stream.write_all(head.as_bytes()).await?;
    stream.write_all(body.as_bytes()).await?;
    stream.flush().await
}

/// One queue's scrape: `(key like "t:name", stats)`; a dead/busy actor
/// reports zeros rather than stalling the scrape.
async fn collect(registry: &QueueRegistry) -> Vec<(String, QueueStats)> {
    let mut out = Vec::new();
    for (key, handle) in registry.queues() {
        let (tx, rx) = oneshot::channel();
        let stats = if handle.tx.try_send(QueueMsg::Stats { reply: tx }).is_ok() {
            tokio::time::timeout(std::time::Duration::from_millis(200), rx)
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default()
        } else {
            QueueStats::default()
        };
        out.push((key, stats));
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
}
