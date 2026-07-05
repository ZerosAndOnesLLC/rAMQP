//! Broker latency + throughput + RSS harness (broker.md §3.4).
//!
//! Measures the ramqp client against any AMQP 1.0 broker:
//!
//! - **e2e latency** — closed-loop, one in-flight message: produce settled,
//!   consume, accept; nanosecond timestamps embedded in the payload.
//!   Reports p50/p90/p99/p99.9/max.
//! - **throughput** — blast N settled messages, drain them all; msgs/s.
//! - **RSS** — this process's VmRSS after the run. In in-process mode that
//!   *includes* the broker; against an external broker it is client-only.
//!
//! ```sh
//! # ramqp-broker, in-process (default):
//! cargo run -p ramqp-bench-compare --release --bin latency
//! # any external broker:
//! LAT_URL=amqp://guest:guest@localhost:5672 LAT_ADDRESS=/queues/bench-lat \
//!     cargo run -p ramqp-bench-compare --release --bin latency
//! ```
//!
//! Env knobs: `LAT_N` (throughput messages, default 50000), `LAT_LAT_N`
//! (latency samples, default 5000), `LAT_PAYLOAD` (bytes, default 256).

use std::sync::OnceLock;
use std::time::Instant;

use ramqp::{ConnectionBuilder, Message};

fn epoch() -> Instant {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    *EPOCH.get_or_init(Instant::now)
}

fn now_nanos() -> u64 {
    epoch().elapsed().as_nanos() as u64
}

fn payload(size: usize) -> Vec<u8> {
    let mut buf = vec![0u8; size.max(8)];
    buf[..8].copy_from_slice(&now_nanos().to_be_bytes());
    buf
}

fn sent_nanos(delivery: &ramqp::Delivery) -> u64 {
    let msg = delivery.message().expect("decodable");
    match &msg.body {
        ramqp::types::messaging::Body::Data(sections) => {
            let first = sections.first().expect("data section");
            u64::from_be_bytes(first[..8].try_into().expect("8-byte timestamp"))
        }
        other => panic!("expected data body, got {other:?}"),
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

fn percentile(sorted: &[u64], p: f64) -> u64 {
    if sorted.is_empty() {
        return 0;
    }
    let idx = ((sorted.len() as f64 - 1.0) * p).round() as usize;
    sorted[idx.min(sorted.len() - 1)]
}

fn rss_kib() -> Option<u64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    status
        .lines()
        .find(|l| l.starts_with("VmRSS:"))?
        .split_whitespace()
        .nth(1)?
        .parse()
        .ok()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Surface broker-side diagnostics when RUST_LOG is set (in-process mode).
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
    let lat_n = env_usize("LAT_LAT_N", 5_000);
    let thr_n = env_usize("LAT_N", 50_000);
    let size = env_usize("LAT_PAYLOAD", 256);
    let address = std::env::var("LAT_ADDRESS").unwrap_or_else(|_| "/queues/bench-lat".to_owned());

    // Target: external URL, or an in-process ramqp-broker.
    let (url, target) = match std::env::var("LAT_URL") {
        Ok(url) => (url.clone(), format!("external broker at {url}")),
        Err(_) => {
            let bound = ramqp_broker::Broker::new(ramqp_broker::BrokerConfig::default())
                .bind("127.0.0.1:0")
                .await?;
            let addr = bound.local_addr();
            tokio::spawn(bound.run());
            (
                format!("amqp://{addr}"),
                "ramqp-broker (in-process)".to_owned(),
            )
        }
    };
    println!("target:   {target}");
    println!("address:  {address}   payload: {size} B");

    let conn = ConnectionBuilder::new(&url).connect().await?;
    let session = conn.begin_session().await?;
    let producer = session.create_producer(&address).await?;
    let mut consumer = session.create_consumer(&address).await?;

    // ---- e2e latency: closed loop, one in flight ----
    // Warmup.
    for _ in 0..200 {
        producer.send_settled(Message::data(payload(size))).await?;
        let d = consumer.recv().await?;
        consumer.accept(&d).await?;
    }
    let mut samples = Vec::with_capacity(lat_n);
    for _ in 0..lat_n {
        producer.send_settled(Message::data(payload(size))).await?;
        let d = consumer.recv().await?;
        let latency = now_nanos().saturating_sub(sent_nanos(&d));
        consumer.accept(&d).await?;
        samples.push(latency);
    }
    samples.sort_unstable();
    let us = |n: u64| n as f64 / 1_000.0;
    println!(
        "latency ({lat_n} closed-loop samples, µs): p50 {:.1}  p90 {:.1}  p99 {:.1}  p99.9 {:.1}  max {:.1}",
        us(percentile(&samples, 0.50)),
        us(percentile(&samples, 0.90)),
        us(percentile(&samples, 0.99)),
        us(percentile(&samples, 0.999)),
        us(*samples.last().unwrap()),
    );

    // ---- throughput: blast then drain ----
    let start = Instant::now();
    for _ in 0..thr_n {
        producer.send_settled(Message::data(payload(size))).await?;
    }
    let mut received = 0usize;
    let mut last = None;
    while received < thr_n {
        let d = consumer.recv().await?;
        received += 1;
        if received == thr_n {
            last = Some(d);
        } else if received % 64 == 0 {
            // Batched settlement (cheap ranged disposition).
            consumer.accept_through(&d).await?;
        }
    }
    if let Some(d) = last {
        consumer.accept_through(&d).await?;
    }
    let elapsed = start.elapsed();
    println!(
        "throughput: {thr_n} msgs in {:.2}s = {:.0} msg/s ({} B settled sends, batched accepts)",
        elapsed.as_secs_f64(),
        thr_n as f64 / elapsed.as_secs_f64(),
        size,
    );

    if let Some(rss) = rss_kib() {
        println!("process RSS: {:.1} MiB ({})", rss as f64 / 1024.0, target);
    }

    conn.close().await?;
    Ok(())
}
