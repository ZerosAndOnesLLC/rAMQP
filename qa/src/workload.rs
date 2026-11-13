//! Concurrent produce/consume with consumer churn, then a delivery-guarantee
//! check.
//!
//! Every message carries `producer:seq`. The run records what the broker
//! acknowledged (`accepted` outcome), what each consumer received and what it
//! accepted; after the producers stop, a drain consumer takes what is left.
//! Then:
//! - **lost**: acknowledged but never accepted by anyone (fails unless
//!   `--allow-loss`, e.g. a transient queue across a broker crash);
//! - **redelivered after accept**: a message delivered again after a
//!   consumer accepted it (fails unless `--allow-dup-after-accept` — a crash
//!   or a reset connection can legitimately lose an accept in flight);
//! - **order**: on every consumer link, the messages it is the first to
//!   receive arrive in increasing `seq` per producer (queues deliver in
//!   publish order; redeliveries are exempt). Only checked when consumers
//!   don't leave (`--exit-every 0`): a departing consumer's unreceived
//!   messages go back to the queue while its successor may already be
//!   receiving newer ones, which legitimately reorders them.
//!
//! Consumers leave at random (`--exit-every`) by every route a real one
//! takes — detach, drop, ending the session, closing the connection — and
//! come back on a fresh connection, so the redelivery-after-accept class of
//! bug (issue #50) is exercised continuously.

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use ramqp::types::messaging::DeliveryState;
use ramqp::{Connection, Consumer, Message, Session};
use serde_json::json;

use crate::common::{Rng, body, connect, tag, write_json};

#[derive(clap::Args)]
pub struct Args {
    /// Broker URL, e.g. amqp://127.0.0.1:5672.
    #[arg(long)]
    url: String,
    /// Queue address, e.g. /queues/qa.work.
    #[arg(long)]
    address: String,
    #[arg(long, default_value_t = 2)]
    producers: u32,
    #[arg(long, default_value_t = 2)]
    consumers: u32,
    /// How long the producers publish.
    #[arg(long, default_value_t = 30)]
    secs: u64,
    /// Message body size in bytes.
    #[arg(long, default_value_t = 256)]
    size: usize,
    /// Each consumer leaves after a random 1..=N accepts (0 = never).
    #[arg(long, default_value_t = 0)]
    exit_every: u64,
    /// How a consumer leaves: any (random), detach, drop, end, close.
    #[arg(long, default_value = "any")]
    exit_mode: String,
    /// Percent of deliveries released (requeued) instead of accepted.
    #[arg(long, default_value_t = 0)]
    release_pct: u64,
    /// Per-producer send rate cap in msg/s (0 = as fast as acknowledged).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Use the client's transparent reconnect.
    #[arg(long)]
    reconnecting: bool,
    /// Acknowledged messages may be lost (transient queue, broker crash).
    #[arg(long)]
    allow_loss: bool,
    /// Accepted messages may come back (crashes, reset connections).
    #[arg(long)]
    allow_dup_after_accept: bool,
    /// Skip the order check (for brokers that don't keep publish order).
    #[arg(long)]
    no_order_check: bool,
    /// Drain through this URL instead of `--url` (e.g. around a flaky proxy).
    #[arg(long)]
    drain_url: Option<String>,
    /// The drain stops after this long without a delivery.
    #[arg(long, default_value_t = 3000)]
    drain_idle_ms: u64,
    /// How long to keep retrying to connect for the drain.
    #[arg(long, default_value_t = 120)]
    drain_connect_secs: u64,
    /// Write the summary here as JSON.
    #[arg(long)]
    json: Option<PathBuf>,
}

type Id = (u32, u64);

#[derive(Default)]
struct State {
    acked: HashSet<Id>,
    uncertain: HashSet<Id>,
    refused: u64,
    seen: HashSet<Id>,
    accepted: HashSet<Id>,
    deliveries: u64,
    redeliveries: u64,
    dup_after_accept: Vec<Id>,
    order_violations: Vec<String>,
    /// Per consumer link: the last first-time `seq` seen from each producer.
    last_first: HashMap<(u64, u32), u64>,
    unparsable: u64,
}

struct Shared {
    state: Mutex<State>,
    stop_consumers: AtomicBool,
    link_ids: AtomicU64,
    exits: AtomicU64,
    errors: AtomicU64,
}

impl Shared {
    /// Record one delivery on consumer link `link`; returns its id.
    fn record(&self, link: u64, id: Id) {
        let mut st = self.state.lock().expect("state");
        st.deliveries += 1;
        if st.accepted.contains(&id) {
            st.dup_after_accept.push(id);
        }
        if !st.seen.insert(id) {
            st.redeliveries += 1;
            return;
        }
        let key = (link, id.0);
        if let Some(prev) = st.last_first.get(&key)
            && *prev > id.1
        {
            let v = format!("link {link}: producer {} seq {} after {prev}", id.0, id.1);
            st.order_violations.push(v);
        }
        st.last_first.insert(key, id.1);
    }
}

fn parse_id(t: &str) -> Option<Id> {
    let (p, s) = t.split_once(':')?;
    Some((p.parse().ok()?, s.parse().ok()?))
}

pub async fn run(a: Args) -> bool {
    let a = Arc::new(a);
    let shared = Arc::new(Shared {
        state: Mutex::new(State::default()),
        stop_consumers: AtomicBool::new(false),
        link_ids: AtomicU64::new(0),
        exits: AtomicU64::new(0),
        errors: AtomicU64::new(0),
    });
    let started = Instant::now();
    let deadline = started + Duration::from_secs(a.secs);

    let consumers: Vec<_> = (0..a.consumers)
        .map(|i| tokio::spawn(consumer(a.clone(), shared.clone(), i)))
        .collect();
    let producers: Vec<_> = (0..a.producers)
        .map(|i| tokio::spawn(producer(a.clone(), shared.clone(), i, deadline)))
        .collect();
    for p in producers {
        let _ = p.await;
    }
    // Let the consumers catch up a little, then stop them and drain the rest.
    tokio::time::sleep(Duration::from_millis(500)).await;
    shared.stop_consumers.store(true, Ordering::SeqCst);
    for c in consumers {
        let _ = c.await;
    }
    let drained = drain(&a, &shared).await;

    let st = shared.state.lock().expect("state");
    let lost: Vec<Id> = st.acked.difference(&st.accepted).copied().collect();
    let mut failures = Vec::new();
    if st.acked.is_empty() {
        failures.push("no message was acknowledged: the workload never ran".to_string());
    }
    if !drained {
        failures.push("could not connect to drain the queue".to_string());
    }
    if !lost.is_empty() && !a.allow_loss {
        failures.push(format!("{} acknowledged messages lost", lost.len()));
    }
    if !st.dup_after_accept.is_empty() && !a.allow_dup_after_accept {
        failures.push(format!(
            "{} accepted messages redelivered",
            st.dup_after_accept.len()
        ));
    }
    let order_checked = !a.no_order_check && a.exit_every == 0;
    if !st.order_violations.is_empty() && order_checked {
        failures.push(format!("{} order violations", st.order_violations.len()));
    }
    let sample =
        |v: &[Id]| -> Vec<String> { v.iter().take(10).map(|(p, s)| format!("{p}:{s}")).collect() };
    let summary = json!({
        "address": a.address,
        "secs": started.elapsed().as_secs_f64(),
        "acknowledged": st.acked.len(),
        "uncertain_sends": st.uncertain.len(),
        "refused_sends": st.refused,
        "deliveries": st.deliveries,
        "redeliveries": st.redeliveries,
        "accepted": st.accepted.len(),
        "consumer_exits": shared.exits.load(Ordering::SeqCst),
        "connection_errors": shared.errors.load(Ordering::SeqCst),
        "unparsable": st.unparsable,
        "lost": lost.len(),
        "lost_sample": sample(&lost),
        "redelivered_after_accept": st.dup_after_accept.len(),
        "redelivered_after_accept_sample": sample(&st.dup_after_accept),
        "order_checked": order_checked,
        "order_violations": st.order_violations.len(),
        "order_violation_sample": st.order_violations.iter().take(10).collect::<Vec<_>>(),
        "failures": failures,
        "pass": failures.is_empty(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    write_json(a.json.as_deref(), &summary);
    failures.is_empty()
}

async fn producer(a: Arc<Args>, shared: Arc<Shared>, pid: u32, deadline: Instant) {
    let mut seq: u64 = 0;
    let mut link: Option<(Connection, ramqp::Producer)> = None;
    let gap = (a.rate > 0).then(|| Duration::from_secs_f64(1.0 / a.rate as f64));
    let mut next = Instant::now();
    while Instant::now() < deadline {
        if link.is_none() {
            match open_producer(&a).await {
                Ok(l) => link = Some(l),
                Err(_) => {
                    shared.errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            }
        }
        if let Some(gap) = gap {
            next += gap;
            tokio::time::sleep_until(next.into()).await;
        }
        seq += 1;
        let id = (pid, seq);
        let msg = Message::text(body(&format!("{pid}:{seq}"), a.size));
        let (_, p) = link.as_ref().expect("opened above");
        let sent = tokio::time::timeout(Duration::from_secs(30), p.send(msg)).await;
        match sent {
            Ok(Ok(DeliveryState::Accepted(_))) => {
                shared.state.lock().expect("state").acked.insert(id);
            }
            Ok(Ok(DeliveryState::Released(_) | DeliveryState::Rejected(_))) => {
                shared.state.lock().expect("state").refused += 1;
            }
            // Modified, timed out, or failed: the broker may or may not
            // have it. Never counted as lost; fine if it turns up.
            Ok(Ok(_)) | Ok(Err(_)) | Err(_) => {
                shared.state.lock().expect("state").uncertain.insert(id);
                shared.errors.fetch_add(1, Ordering::Relaxed);
                if let Some((conn, _)) = link.take() {
                    let _ = tokio::time::timeout(Duration::from_secs(2), conn.close()).await;
                }
            }
        }
    }
    if let Some((conn, _)) = link {
        let _ = tokio::time::timeout(Duration::from_secs(5), conn.close()).await;
    }
}

async fn open_producer(a: &Args) -> Result<(Connection, ramqp::Producer), String> {
    let conn = connect(&a.url, a.reconnecting).await?;
    let session = conn.begin_session().await.map_err(|e| e.to_string())?;
    let p = session
        .create_producer(&a.address)
        .await
        .map_err(|e| e.to_string())?;
    Ok((conn, p))
}

struct ConsumerLink {
    conn: Connection,
    session: Session,
    consumer: Consumer,
    id: u64,
    accepts: u64,
    leave_after: u64,
}

async fn open_consumer(a: &Args, shared: &Shared, rng: &mut Rng) -> Result<ConsumerLink, String> {
    let conn = connect(&a.url, a.reconnecting).await?;
    let session = conn.begin_session().await.map_err(|e| e.to_string())?;
    let consumer = session
        .create_consumer(&a.address)
        .await
        .map_err(|e| e.to_string())?;
    Ok(ConsumerLink {
        conn,
        session,
        consumer,
        id: shared.link_ids.fetch_add(1, Ordering::SeqCst),
        accepts: 0,
        leave_after: if a.exit_every > 0 {
            1 + rng.below(a.exit_every)
        } else {
            u64::MAX
        },
    })
}

/// Leave the way a real consumer might: `mode`, or picked at random.
async fn leave(link: ConsumerLink, mode: &str, rng: &mut Rng) {
    let limit = Duration::from_secs(5);
    let ConsumerLink {
        conn,
        session,
        consumer,
        ..
    } = link;
    let pick = match mode {
        "detach" => 0,
        "drop" => 1,
        "end" => 2,
        "close" => 3,
        _ => rng.below(4),
    };
    match pick {
        0 => {
            let _ = tokio::time::timeout(limit, consumer.detach()).await;
            let _ = tokio::time::timeout(limit, conn.close()).await;
        }
        1 => {
            drop(consumer);
            let _ = tokio::time::timeout(limit, conn.close()).await;
        }
        2 => {
            let _ = tokio::time::timeout(limit, session.end()).await;
            drop(consumer);
            let _ = tokio::time::timeout(limit, conn.close()).await;
        }
        _ => {
            let _ = tokio::time::timeout(limit, conn.close()).await;
            drop(consumer);
        }
    }
}

async fn consumer(a: Arc<Args>, shared: Arc<Shared>, cid: u32) {
    let mut rng = Rng::seeded(cid as u64 + 1);
    let mut link: Option<ConsumerLink> = None;
    while !shared.stop_consumers.load(Ordering::SeqCst) {
        if link.is_none() {
            match open_consumer(&a, &shared, &mut rng).await {
                Ok(l) => link = Some(l),
                Err(_) => {
                    shared.errors.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(250)).await;
                    continue;
                }
            }
        }
        let l = link.as_mut().expect("opened above");
        let d = match tokio::time::timeout(Duration::from_millis(500), l.consumer.recv()).await {
            Err(_) => continue,
            Ok(Ok(d)) => d,
            Ok(Err(_)) => {
                shared.errors.fetch_add(1, Ordering::Relaxed);
                link = None;
                tokio::time::sleep(Duration::from_millis(200)).await;
                continue;
            }
        };
        let Some(id) = tag(&d).as_deref().and_then(parse_id) else {
            shared.state.lock().expect("state").unparsable += 1;
            let _ = l.consumer.accept(&d).await;
            continue;
        };
        shared.record(l.id, id);
        if a.release_pct > 0 && rng.below(100) < a.release_pct {
            if l.consumer.release(&d).await.is_err() {
                link = None;
            }
            continue;
        }
        if l.consumer.accept(&d).await.is_err() {
            link = None;
            continue;
        }
        shared.state.lock().expect("state").accepted.insert(id);
        l.accepts += 1;
        if l.accepts >= l.leave_after {
            shared.exits.fetch_add(1, Ordering::Relaxed);
            leave(link.take().expect("present"), &a.exit_mode, &mut rng).await;
        }
    }
    if let Some(l) = link {
        let _ = tokio::time::timeout(Duration::from_secs(5), l.conn.close()).await;
    }
}

/// Take everything left on the queue (accepting it), until idle.
async fn drain(a: &Args, shared: &Shared) -> bool {
    let url = a.drain_url.as_deref().unwrap_or(&a.url);
    let give_up = Instant::now() + Duration::from_secs(a.drain_connect_secs);
    let idle = Duration::from_millis(a.drain_idle_ms);
    loop {
        let opened = async {
            let conn = connect(url, false).await?;
            let s = conn.begin_session().await.map_err(|e| e.to_string())?;
            let c = s
                .create_consumer(&a.address)
                .await
                .map_err(|e| e.to_string())?;
            Ok::<_, String>((conn, c))
        }
        .await;
        let (conn, mut c) = match opened {
            Ok(x) => x,
            Err(_) if Instant::now() < give_up => {
                tokio::time::sleep(Duration::from_millis(500)).await;
                continue;
            }
            Err(_) => return false,
        };
        let link = shared.link_ids.fetch_add(1, Ordering::SeqCst);
        loop {
            match tokio::time::timeout(idle, c.recv()).await {
                Err(_) => {
                    let _ = tokio::time::timeout(Duration::from_secs(5), conn.close()).await;
                    return true;
                }
                Ok(Err(_)) => break, // connection trouble: reconnect and go on
                Ok(Ok(d)) => {
                    if let Some(id) = tag(&d).as_deref().and_then(parse_id) {
                        shared.record(link, id);
                        if c.accept(&d).await.is_ok() {
                            shared.state.lock().expect("state").accepted.insert(id);
                        }
                    } else {
                        let _ = c.accept(&d).await;
                    }
                }
            }
        }
        if Instant::now() >= give_up {
            return false;
        }
    }
}
