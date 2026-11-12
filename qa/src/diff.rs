//! Differential conformance: the same protocol scenarios against several
//! brokers, and a report of where they behave differently.
//!
//! The first `--target` is the reference (normally `ramqp-brokerd`, compared
//! against RabbitMQ 4.x). Each scenario reduces what a broker did to a short
//! observation string; a target whose observation differs from the
//! reference's fails the run unless `--known` lists that difference with a
//! reason. Every scenario uses its own queue, `<prefix>qa.diff.<scenario>`
//! (`--list-queues` prints them, so a script can declare them first on
//! brokers that don't auto-create).

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use bytes::Bytes;
use ramqp::codec::{OrderedMap, Symbol, Value};
use ramqp::types::definitions::{AmqpError, Error};
use ramqp::types::messaging::{
    ApplicationProperties, Body, DeliveryState, Header, MessageId, Modified, Properties,
};
use ramqp::{Consumer, Delivery, Message, Session};
use serde_json::json;

use crate::common::{connect, write_json};

#[derive(clap::Args)]
pub struct Args {
    /// `name=url,prefix`, e.g. `ramqp=amqp://127.0.0.1:5700,/queues/`.
    /// Repeat; the first is the reference.
    #[arg(long = "target")]
    targets: Vec<String>,
    /// JSON: `{"<target>": {"<scenario>": "why it differs"}}`.
    #[arg(long)]
    known: Option<PathBuf>,
    /// Only print the scenario queue names (one per line) and exit.
    #[arg(long)]
    list_queues: bool,
    /// Per-scenario time limit (slow brokers reconnect slowly).
    #[arg(long, default_value_t = 120)]
    scenario_timeout_secs: u64,
    #[arg(long)]
    json: Option<PathBuf>,
}

/// Scenario names, in run order. Each one's queue is `qa.diff.<name>`.
const SCENARIOS: &[&str] = &[
    "roundtrip",
    "fifo_100",
    "release_redelivers",
    "modified_failed_counts",
    "reject_not_redelivered",
    "undeliverable_here",
    "sections_preserved",
    "large_1mib",
    "unsettled_close_redelivers",
    "presettled_send",
    "header_ttl_expires",
    "first_acquirer",
    "empty_queue",
    "missing_queue_attach",
];

/// A queue no script declares: the attach to it is the scenario.
const MISSING: &str = "qa.diff.never-declared";

pub async fn run(a: Args) -> bool {
    if a.list_queues {
        for s in SCENARIOS {
            if *s != "missing_queue_attach" {
                println!("qa.diff.{s}");
            }
        }
        return true;
    }
    let mut targets = Vec::new();
    for t in &a.targets {
        let Some((name, rest)) = t.split_once('=') else {
            eprintln!("diff: bad --target {t:?} (want name=url,prefix)");
            return false;
        };
        let (url, prefix) = rest.split_once(',').unwrap_or((rest, ""));
        targets.push((name.to_owned(), url.to_owned(), prefix.to_owned()));
    }
    if targets.is_empty() {
        eprintln!("diff: no --target");
        return false;
    }
    let known: BTreeMap<String, BTreeMap<String, String>> = match &a.known {
        Some(p) => match std::fs::read_to_string(p).map(|t| serde_json::from_str(&t)) {
            Ok(Ok(k)) => k,
            Ok(Err(e)) => {
                eprintln!("diff: {}: {e}", p.display());
                return false;
            }
            Err(e) => {
                eprintln!("diff: {}: {e}", p.display());
                return false;
            }
        },
        None => BTreeMap::new(),
    };

    // observations[scenario][target]
    let mut obs: BTreeMap<&str, BTreeMap<String, String>> = BTreeMap::new();
    for (name, url, prefix) in &targets {
        for s in SCENARIOS {
            let limit = Duration::from_secs(a.scenario_timeout_secs);
            let o = tokio::time::timeout(limit, scenario(s, url, prefix))
                .await
                .unwrap_or_else(|_| "timed out".into());
            obs.entry(s).or_default().insert(name.clone(), o);
        }
    }

    let reference = &targets[0].0;
    let mut ok = true;
    let mut report = Vec::new();
    for s in SCENARIOS {
        let row = &obs[s];
        let base = &row[reference];
        println!("{s}");
        for (name, _, _) in &targets {
            let o = &row[name];
            let verdict = if name == reference || o == base {
                "same".to_string()
            } else if let Some(why) = known.get(name).and_then(|k| k.get(*s)) {
                format!("known difference: {why}")
            } else {
                ok = false;
                "UNEXPECTED DIFFERENCE".to_string()
            };
            println!("  {name:<10} {o:<48} {verdict}");
            report.push(json!({"scenario": s, "target": name, "observed": o, "verdict": verdict}));
        }
    }
    write_json(
        a.json.as_deref(),
        &json!({"reference": reference, "results": report, "pass": ok}),
    );
    ok
}

async fn scenario(name: &str, url: &str, prefix: &str) -> String {
    let address = format!("{prefix}qa.diff.{name}");
    let res = match name {
        "missing_queue_attach" => missing_queue(url, &format!("{prefix}{MISSING}")).await,
        _ => {
            if let Err(e) = purge(url, &address).await {
                return format!("setup failed: {e}");
            }
            match name {
                "roundtrip" => roundtrip(url, &address).await,
                "fifo_100" => fifo(url, &address).await,
                "release_redelivers" => release(url, &address).await,
                "modified_failed_counts" => modified_failed(url, &address).await,
                "reject_not_redelivered" => reject(url, &address).await,
                "undeliverable_here" => undeliverable(url, &address).await,
                "sections_preserved" => sections(url, &address).await,
                "large_1mib" => large(url, &address).await,
                "unsettled_close_redelivers" => unsettled_close(url, &address).await,
                "presettled_send" => presettled(url, &address).await,
                "header_ttl_expires" => ttl(url, &address).await,
                "first_acquirer" => first_acquirer(url, &address).await,
                "empty_queue" => empty(url, &address).await,
                other => Err(format!("unknown scenario {other}")),
            }
        }
    };
    res.unwrap_or_else(|e| e)
}

/// An error reduced to what is comparable across brokers: the kind and the
/// AMQP condition, never the free-text description.
fn err<E: std::fmt::Display>(e: E) -> String {
    let s = e.to_string();
    let condition = s
        .split_whitespace()
        .find(|w| w.starts_with("amqp:"))
        .map(|w| w.trim_end_matches([':', ')', ',']).to_owned());
    match condition {
        Some(c) => format!("error {c}"),
        None => "error".into(),
    }
}

async fn session(url: &str) -> Result<(ramqp::Connection, Session), String> {
    // Retried: some brokers (Artemis) briefly refuse a burst of fresh
    // connections, which is not what a scenario is about.
    let mut last = String::new();
    for attempt in 0..8u64 {
        match connect(url, false).await {
            Ok(conn) => {
                let s = conn.begin_session().await.map_err(err)?;
                return Ok((conn, s));
            }
            Err(e) => {
                last = e;
                tokio::time::sleep(Duration::from_millis(500 * (attempt + 1))).await;
            }
        }
    }
    Err(format!("connect failed ({last})"))
}

async fn send(s: &Session, address: &str, msgs: Vec<Message>) -> Result<String, String> {
    let p = s.create_producer(address).await.map_err(err)?;
    let mut outcome = String::new();
    for m in msgs {
        outcome = match p.send(m).await.map_err(err)? {
            DeliveryState::Accepted(_) => "accepted".into(),
            DeliveryState::Released(_) => "released".into(),
            DeliveryState::Rejected(_) => "rejected".into(),
            DeliveryState::Modified(_) => "modified".into(),
            other => format!("{other:?}"),
        };
    }
    let _ = p.detach().await;
    Ok(outcome)
}

async fn recv(c: &mut Consumer, ms: u64) -> Option<Delivery> {
    tokio::time::timeout(Duration::from_millis(ms), c.recv())
        .await
        .ok()
        .and_then(Result::ok)
}

fn text_of(d: &Delivery) -> String {
    match d.message().map(|m| m.body) {
        Ok(Body::Value(Value::String(s))) => s,
        Ok(Body::Data(parts)) => format!(
            "<{} data bytes>",
            parts.iter().map(Bytes::len).sum::<usize>()
        ),
        _ => "<other body>".into(),
    }
}

fn delivery_count(d: &Delivery) -> u32 {
    d.message()
        .ok()
        .and_then(|m| m.header)
        .map_or(0, |h| h.delivery_count)
}

/// Take anything a previous run left behind.
async fn purge(url: &str, address: &str) -> Result<(), String> {
    let (conn, s) = session(url).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    while let Some(d) = recv(&mut c, 300).await {
        let _ = c.accept(&d).await;
    }
    let _ = conn.close().await;
    Ok(())
}

async fn roundtrip(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    let outcome = send(&s, address, vec![Message::text("hello")]).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let d = recv(&mut c, 5000).await.ok_or("nothing delivered")?;
    let body = if text_of(&d) == "hello" {
        "body ok"
    } else {
        "body changed"
    };
    c.accept(&d).await.map_err(err)?;
    let _ = conn.close().await;
    Ok(format!("outcome {outcome}, {body}"))
}

async fn fifo(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    send(
        &s,
        address,
        (0..100).map(|i| Message::text(i.to_string())).collect(),
    )
    .await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let mut got = Vec::new();
    while got.len() < 100 {
        let Some(d) = recv(&mut c, 5000).await else {
            break;
        };
        got.push(text_of(&d));
        c.accept(&d).await.map_err(err)?;
    }
    let _ = conn.close().await;
    let want: Vec<String> = (0..100).map(|i| i.to_string()).collect();
    Ok(match got.len() {
        100 if got == want => "100 in order".into(),
        100 => "100 out of order".into(),
        n => format!("only {n} delivered"),
    })
}

/// Deliver one message, settle it with `settle`, and say what came back.
async fn settle_then_look(
    url: &str,
    address: &str,
    settle: impl AsyncFnOnce(&Consumer, &Delivery) -> Result<(), String>,
) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    send(&s, address, vec![Message::text("m")]).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let d = recv(&mut c, 5000).await.ok_or("nothing delivered")?;
    settle(&c, &d).await?;
    let again = recv(&mut c, 1500).await;
    let out = match &again {
        Some(d2) => format!("redelivered, delivery-count {}", delivery_count(d2)),
        None => "not redelivered".into(),
    };
    if let Some(d2) = again {
        let _ = c.accept(&d2).await;
    }
    let _ = conn.close().await;
    Ok(out)
}

async fn release(url: &str, address: &str) -> Result<String, String> {
    settle_then_look(url, address, async |c, d| c.release(d).await.map_err(err)).await
}

async fn modified_failed(url: &str, address: &str) -> Result<String, String> {
    settle_then_look(url, address, async |c, d| {
        let m = Modified {
            delivery_failed: Some(true),
            ..Default::default()
        };
        c.modify(d, m).await.map_err(err)
    })
    .await
}

async fn reject(url: &str, address: &str) -> Result<String, String> {
    settle_then_look(url, address, async |c, d| {
        let e = Error::new(AmqpError::InternalError, Some("qa".into()));
        c.reject(d, Some(e)).await.map_err(err)
    })
    .await
}

async fn undeliverable(url: &str, address: &str) -> Result<String, String> {
    settle_then_look(url, address, async |c, d| {
        let m = Modified {
            undeliverable_here: Some(true),
            ..Default::default()
        };
        c.modify(d, m).await.map_err(err)
    })
    .await
}

async fn sections(url: &str, address: &str) -> Result<String, String> {
    let mut m = Message::text("sections");
    m.header = Some(Header {
        durable: true,
        priority: 7,
        ..Default::default()
    });
    m.properties = Some(Properties {
        message_id: Some(MessageId::Ulong(42)),
        correlation_id: Some(MessageId::String("corr-1".into())),
        subject: Some("qa-subject".into()),
        content_type: Some(Symbol("text/plain".into())),
        reply_to: Some("qa-reply".into()),
        ..Default::default()
    });
    m.application_properties = Some(ApplicationProperties(OrderedMap::from(vec![
        ("k".to_string(), Value::String("v".into())),
        ("n".to_string(), Value::Long(7)),
        ("b".to_string(), Value::Bool(true)),
    ])));
    let sent = m.clone();
    let (conn, s) = session(url).await?;
    send(&s, address, vec![m]).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let d = recv(&mut c, 5000).await.ok_or("nothing delivered")?;
    let got = d.message().map_err(|_| "undecodable".to_string())?;
    c.accept(&d).await.map_err(err)?;
    let _ = conn.close().await;
    let mut changed = Vec::new();
    let (sh, gh) = (
        sent.header.unwrap_or_default(),
        got.header.unwrap_or_default(),
    );
    if sh.durable != gh.durable {
        changed.push("durable");
    }
    if sh.priority != gh.priority {
        changed.push("priority");
    }
    let (sp, gp) = (
        sent.properties.unwrap_or_default(),
        got.properties.unwrap_or_default(),
    );
    if sp.message_id != gp.message_id {
        changed.push("message-id");
    }
    if sp.correlation_id != gp.correlation_id {
        changed.push("correlation-id");
    }
    if sp.subject != gp.subject {
        changed.push("subject");
    }
    if sp.content_type != gp.content_type {
        changed.push("content-type");
    }
    if sp.reply_to != gp.reply_to {
        changed.push("reply-to");
    }
    if sent.application_properties != got.application_properties {
        changed.push("application-properties");
    }
    if sent.body != got.body {
        changed.push("body");
    }
    Ok(if changed.is_empty() {
        "all preserved".into()
    } else {
        format!("changed: {}", changed.join(","))
    })
}

async fn large(url: &str, address: &str) -> Result<String, String> {
    let payload: Vec<u8> = (0..1024 * 1024).map(|i| (i % 251) as u8).collect();
    let (conn, s) = session(url).await?;
    send(
        &s,
        address,
        vec![Message::data(Bytes::from(payload.clone()))],
    )
    .await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let d = recv(&mut c, 10_000).await.ok_or("nothing delivered")?;
    let ok = match d.message().map(|m| m.body) {
        Ok(Body::Data(parts)) => parts.concat() == payload,
        _ => false,
    };
    c.accept(&d).await.map_err(err)?;
    let _ = conn.close().await;
    Ok(if ok {
        "1 MiB intact".into()
    } else {
        "1 MiB corrupted".into()
    })
}

async fn unsettled_close(url: &str, address: &str) -> Result<String, String> {
    {
        let (conn, s) = session(url).await?;
        send(
            &s,
            address,
            (0..3).map(|i| Message::text(i.to_string())).collect(),
        )
        .await?;
        let mut c = s.create_consumer(address).await.map_err(err)?;
        for _ in 0..3 {
            recv(&mut c, 5000).await.ok_or("nothing delivered")?;
        }
        // Close without settling anything.
        let _ = conn.close().await;
    }
    let (conn, s) = session(url).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let mut counts = Vec::new();
    while let Some(d) = recv(&mut c, 1500).await {
        counts.push(delivery_count(&d));
        c.accept(&d).await.map_err(err)?;
    }
    let _ = conn.close().await;
    Ok(format!(
        "{} redelivered, delivery-counts {counts:?}",
        counts.len()
    ))
}

async fn presettled(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    let p = s.create_producer(address).await.map_err(err)?;
    for i in 0..5 {
        p.send_settled(Message::text(i.to_string()))
            .await
            .map_err(err)?;
    }
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let mut n = 0;
    while let Some(d) = recv(&mut c, 1500).await {
        n += 1;
        c.accept(&d).await.map_err(err)?;
    }
    let _ = conn.close().await;
    Ok(format!("{n} of 5 delivered"))
}

async fn ttl(url: &str, address: &str) -> Result<String, String> {
    let mut m = Message::text("short-lived");
    m.header = Some(Header {
        ttl: Some(200),
        ..Default::default()
    });
    let (conn, s) = session(url).await?;
    send(&s, address, vec![m]).await?;
    tokio::time::sleep(Duration::from_millis(800)).await;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let out = match recv(&mut c, 1500).await {
        Some(d) => {
            let _ = c.accept(&d).await;
            "delivered after its ttl"
        }
        None => "expired",
    };
    let _ = conn.close().await;
    Ok(out.into())
}

async fn first_acquirer(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    send(&s, address, vec![Message::text("fa")]).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let d = recv(&mut c, 5000).await.ok_or("nothing delivered")?;
    let first = d
        .message()
        .ok()
        .and_then(|m| m.header)
        .map(|h| h.first_acquirer);
    c.release(&d).await.map_err(err)?;
    let d2 = recv(&mut c, 5000).await.ok_or("not redelivered")?;
    let second = d2
        .message()
        .ok()
        .and_then(|m| m.header)
        .map(|h| h.first_acquirer);
    c.accept(&d2).await.map_err(err)?;
    let _ = conn.close().await;
    let show = |v: Option<bool>| v.map_or("no header".into(), |b| b.to_string());
    Ok(format!(
        "first {}, after release {}",
        show(first),
        show(second)
    ))
}

async fn empty(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    let mut c = s.create_consumer(address).await.map_err(err)?;
    let out = match recv(&mut c, 1000).await {
        Some(_) => "unexpected delivery",
        None => "nothing delivered",
    };
    let _ = conn.close().await;
    Ok(out.into())
}

async fn missing_queue(url: &str, address: &str) -> Result<String, String> {
    let (conn, s) = session(url).await?;
    let out = match s.create_consumer(address).await {
        Ok(c) => {
            let _ = c.detach().await;
            "attached".to_string()
        }
        Err(e) => format!("refused, {}", err(e)),
    };
    let _ = conn.close().await;
    Ok(out)
}
