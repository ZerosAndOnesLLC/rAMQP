//! Endurance / soak harness: sustained concurrent produce+consume against a
//! real broker, sampling throughput and RSS to surface leaks or stalls.
//!
//! ```sh
//! RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
//! RAMQP_BROKER_ADDRESS=/queues/ramqp_soak \
//! RAMQP_SOAK_SECS=60 RAMQP_SIZE=256 \
//!     cargo run --release --example soak
//! ```
//!
//! The producer uses the awaited `send` (credit→disposition back-pressure), so
//! memory stays bounded by flow control rather than local buffering. A growing
//! RSS across samples (beyond warm-up) indicates a leak.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use ramqp::{Connection, Message};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

/// Resident set size in KiB (Linux `/proc/self/status` VmRSS), or 0 elsewhere.
fn rss_kib() -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env("RAMQP_BROKER_URL", "amqp://guest:guest@localhost:5672");
    let address = env("RAMQP_BROKER_ADDRESS", "/queues/ramqp_soak");
    let secs: u64 = env("RAMQP_SOAK_SECS", "60").parse()?;
    let size: usize = env("RAMQP_SIZE", "256").parse()?;

    let conn = Connection::open(&url).await?;
    let session = conn.begin_session().await?;
    let producer = session.create_producer(&address).await?;
    let mut consumer = session.create_consumer(&address).await?;

    let produced = Arc::new(AtomicU64::new(0));
    let consumed = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let deadline = Instant::now() + Duration::from_secs(secs);

    // Producer: awaited sends until the deadline.
    let prod = {
        let produced = produced.clone();
        let stop = stop.clone();
        let body = Bytes::from(vec![0x61u8; size]);
        tokio::spawn(async move {
            while Instant::now() < deadline {
                if producer.send(Message::data(body.clone())).await.is_err() {
                    break;
                }
                produced.fetch_add(1, Ordering::Relaxed);
            }
            stop.store(true, Ordering::Relaxed);
            producer.detach().await.ok();
        })
    };

    // Consumer: drain until the producer stops and we've caught up.
    let cons = {
        let produced = produced.clone();
        let consumed = consumed.clone();
        let stop = stop.clone();
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(Duration::from_secs(2), consumer.recv()).await {
                    Ok(Ok(d)) => {
                        consumer.accept(&d).await.ok();
                        consumed.fetch_add(1, Ordering::Relaxed);
                    }
                    // Idle: done once the producer stopped and we've caught up.
                    _ => {
                        if stop.load(Ordering::Relaxed)
                            && consumed.load(Ordering::Relaxed) >= produced.load(Ordering::Relaxed)
                        {
                            break;
                        }
                    }
                }
            }
            consumer
        })
    };

    // Sampler: throughput + RSS every 5s.
    let rss0 = rss_kib();
    let start = Instant::now();
    let mut last_p = 0u64;
    let mut last_c = 0u64;
    let mut rss_max = rss0;
    println!("soak: {secs}s, {size}B msgs, start RSS {rss0} KiB");
    while !stop.load(Ordering::Relaxed)
        || consumed.load(Ordering::Relaxed) < produced.load(Ordering::Relaxed)
    {
        tokio::time::sleep(Duration::from_secs(5)).await;
        let p = produced.load(Ordering::Relaxed);
        let c = consumed.load(Ordering::Relaxed);
        let rss = rss_kib();
        rss_max = rss_max.max(rss);
        println!(
            "  t={:>4.0}s  produced={p:>9} (+{:>7}/5s)  consumed={c:>9} (+{:>7}/5s)  inflight={:>5}  RSS={rss} KiB",
            start.elapsed().as_secs_f64(),
            p - last_p,
            c - last_c,
            p.saturating_sub(c),
        );
        last_p = p;
        last_c = c;
        if start.elapsed() > Duration::from_secs(secs + 30) {
            eprintln!("soak: consumer failed to catch up; aborting");
            break;
        }
    }

    prod.await?;
    let consumer = cons.await?;
    consumer.detach().await.ok();
    session.end().await.ok();
    conn.close().await?;

    let p = produced.load(Ordering::Relaxed);
    let c = consumed.load(Ordering::Relaxed);
    let dur = start.elapsed().as_secs_f64();
    let rss1 = rss_kib();
    println!("\nsoak done in {dur:.1}s");
    println!(
        "  produced={p}  consumed={c}  ({:.0} msg/s sustained)",
        p as f64 / dur
    );
    println!(
        "  RSS: start {rss0} KiB, peak {rss_max} KiB, end {rss1} KiB (growth {} KiB)",
        rss1 as i64 - rss0 as i64
    );
    if c < p {
        eprintln!(
            "  WARNING: consumer did not fully catch up ({} missing)",
            p - c
        );
    }
    Ok(())
}
