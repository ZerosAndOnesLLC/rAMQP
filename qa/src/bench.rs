//! Throughput and end-to-end latency for one scenario.
//!
//! Each message carries the time it was meant to be sent (microseconds since
//! the run started — one process, one clock). Consumers record
//! `received - meant` per message. With `--rate`, sends follow a fixed
//! schedule and latency counts from the scheduled time, so a stall shows up
//! as latency instead of silently slowing the load down (coordinated
//! omission). Without it, producers send as fast as `--window` in-flight
//! sends allow (closed loop: peak throughput).
//!
//! A warm-up run (same shape, `--warmup` messages) goes first and is not
//! measured.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use futures_util::stream::FuturesUnordered;
use futures_util::{FutureExt, StreamExt};
use ramqp::Message;
use ramqp::types::messaging::DeliveryState;
use serde_json::json;

use crate::common::{body, connect, tag, write_json};

#[derive(clap::Args, Clone)]
pub struct Args {
    /// Scenario name (the key in baselines).
    #[arg(long)]
    name: String,
    #[arg(long)]
    url: String,
    /// Queue address; must be empty and unused by anything else.
    #[arg(long)]
    address: String,
    #[arg(long, default_value_t = 256)]
    size: usize,
    /// Messages measured.
    #[arg(long, default_value_t = 100_000)]
    messages: u64,
    #[arg(long, default_value_t = 1)]
    producers: u64,
    #[arg(long, default_value_t = 1)]
    consumers: u64,
    /// In-flight (unacknowledged) sends per producer.
    #[arg(long, default_value_t = 64)]
    window: usize,
    /// Total send rate in msg/s (0 = closed loop, as fast as possible).
    #[arg(long, default_value_t = 0)]
    rate: u64,
    /// Messages in the unmeasured warm-up run.
    #[arg(long, default_value_t = 5_000)]
    warmup: u64,
    /// Give up after this long.
    #[arg(long, default_value_t = 300)]
    timeout_secs: u64,
    #[arg(long)]
    json: Option<PathBuf>,
}

pub async fn run(a: Args) -> bool {
    if a.warmup > 0 {
        let warm = Args {
            messages: a.warmup,
            ..a.clone()
        };
        if let Err(e) = once(&warm).await {
            eprintln!("bench {}: warm-up failed: {e}", a.name);
            return false;
        }
    }
    match once(&a).await {
        Ok(result) => {
            println!(
                "{}",
                serde_json::to_string_pretty(&result).unwrap_or_default()
            );
            write_json(a.json.as_deref(), &result);
            true
        }
        Err(e) => {
            eprintln!("bench {}: {e}", a.name);
            false
        }
    }
}

async fn once(a: &Args) -> Result<serde_json::Value, String> {
    let start = Instant::now();
    let received = Arc::new(AtomicU64::new(0));
    let send_errors = Arc::new(AtomicU64::new(0));
    let total = a.messages;

    // Consumers attach before anything is sent.
    let mut consumers = Vec::new();
    for _ in 0..a.consumers {
        let conn = connect(&a.url, false).await?;
        let s = conn.begin_session().await.map_err(|e| e.to_string())?;
        let mut c = s
            .create_consumer(&a.address)
            .await
            .map_err(|e| e.to_string())?;
        let received = received.clone();
        let share = (total / a.consumers.max(1)) as usize;
        consumers.push(tokio::spawn(async move {
            let mut lat = Vec::with_capacity(share);
            let mut last = start;
            while received.load(Ordering::SeqCst) < total {
                let d = match tokio::time::timeout(Duration::from_millis(200), c.recv()).await {
                    Err(_) => continue,
                    Ok(Ok(d)) => d,
                    Ok(Err(_)) => break,
                };
                let now = Instant::now();
                if let Some(meant) = tag(&d).and_then(|t| t.parse::<u64>().ok()) {
                    let at = now.duration_since(start).as_micros() as u64;
                    lat.push(at.saturating_sub(meant));
                }
                let _ = c.accept(&d).await;
                last = now;
                received.fetch_add(1, Ordering::SeqCst);
            }
            let _ = conn.close().await;
            (lat, last)
        }));
    }

    let per_producer = total / a.producers.max(1);
    let extra = total % a.producers.max(1);
    let rate_each = (a.rate > 0).then(|| a.rate as f64 / a.producers.max(1) as f64);
    let mut producers = Vec::new();
    for p in 0..a.producers {
        let count = per_producer + if p < extra { 1 } else { 0 };
        let conn = connect(&a.url, false).await?;
        let s = conn.begin_session().await.map_err(|e| e.to_string())?;
        let producer = s
            .create_producer(&a.address)
            .await
            .map_err(|e| e.to_string())?;
        let (size, window) = (a.size, a.window.max(1));
        let send_errors = send_errors.clone();
        producers.push(tokio::spawn(async move {
            let mut in_flight = FuturesUnordered::new();
            let tally = |r: Option<Result<DeliveryState, ramqp::error::SendError>>| {
                if let Some(r) = r
                    && !matches!(r, Ok(DeliveryState::Accepted(_)))
                {
                    send_errors.fetch_add(1, Ordering::Relaxed);
                }
            };
            for i in 0..count {
                let meant = match rate_each {
                    Some(r) => {
                        // Keep driving the in-flight sends while waiting for
                        // this one's slot: a send only progresses when polled.
                        let at = start + Duration::from_secs_f64(i as f64 / r);
                        // Timers fire on a 1 ms tick: sleep to within it,
                        // then yield until the exact time, so the harness
                        // doesn't add up to 1 ms of its own lateness.
                        let near = at.checked_sub(Duration::from_millis(1)).unwrap_or(at);
                        loop {
                            tokio::select! {
                                _ = tokio::time::sleep_until(near.into()) => break,
                                r = in_flight.next(), if !in_flight.is_empty() => tally(r),
                            }
                        }
                        while Instant::now() < at {
                            while let Some(r) = in_flight.next().now_or_never() {
                                tally(r);
                                if in_flight.is_empty() {
                                    break;
                                }
                            }
                            tokio::task::yield_now().await;
                        }
                        at
                    }
                    None => Instant::now(),
                };
                let us = meant.duration_since(start).as_micros() as u64;
                let msg = Message::text(body(&us.to_string(), size));
                in_flight.push(producer.send(msg));
                // Start it now (and collect whatever already finished).
                while let Some(r) = in_flight.next().now_or_never() {
                    tally(r);
                    if in_flight.is_empty() {
                        break;
                    }
                }
                while in_flight.len() >= window {
                    tally(in_flight.next().await);
                }
            }
            while let Some(r) = in_flight.next().await {
                if !matches!(r, Ok(DeliveryState::Accepted(_))) {
                    send_errors.fetch_add(1, Ordering::Relaxed);
                }
            }
            drop(in_flight);
            let _ = conn.close().await;
        }));
    }

    let finished = async {
        for p in producers {
            let _ = p.await;
        }
        let mut all = Vec::new();
        let mut last = start;
        for c in consumers {
            if let Ok((lat, l)) = c.await {
                all.extend(lat);
                last = last.max(l);
            }
        }
        (all, last)
    };
    let (mut lat, last) = tokio::time::timeout(Duration::from_secs(a.timeout_secs), finished)
        .await
        .map_err(|_| {
            format!(
                "timed out with {} of {total} received",
                received.load(Ordering::SeqCst)
            )
        })?;
    let errors = send_errors.load(Ordering::SeqCst);
    if errors > 0 {
        return Err(format!("{errors} sends not accepted"));
    }
    if (lat.len() as u64) < total {
        return Err(format!("only {} of {total} received", lat.len()));
    }
    lat.sort_unstable();
    let pct = |p: f64| lat[((lat.len() as f64 - 1.0) * p).round() as usize];
    let secs = last.duration_since(start).as_secs_f64().max(1e-9);
    Ok(json!({
        "name": a.name,
        "size": a.size,
        "messages": total,
        "producers": a.producers,
        "consumers": a.consumers,
        "window": a.window,
        "rate": a.rate,
        "secs": secs,
        "msgs_per_sec": total as f64 / secs,
        "mib_per_sec": (total as f64 * a.size as f64) / secs / (1024.0 * 1024.0),
        "latency_us": {
            "p50": pct(0.50),
            "p90": pct(0.90),
            "p99": pct(0.99),
            "p999": pct(0.999),
            "max": lat[lat.len() - 1],
        },
    }))
}
