//! Deep-queue bench (broker.md Phase 7 / §8 #1 risk): does publish-confirm
//! tail latency stay flat — and RSS stay bounded — as a quorum queue's depth
//! grows into the millions?
//!
//! Fills a `/quorum/deep` queue in stages; at each depth checkpoint,
//! measures the closed-loop publish→accepted latency distribution and the
//! process RSS (in-process broker: RSS includes broker + client + harness),
//! then drains everything at the end and reports drain throughput.
//!
//! ```sh
//! # paged (bodies spill to disk):
//! DEPTH_DATA_DIR=/tmp/ramqp-depth cargo run -p ramqp-bench-compare --release --bin depth
//! # unpaged (everything resident), for the memory comparison:
//! cargo run -p ramqp-bench-compare --release --bin depth
//! ```
//!
//! Env knobs: `DEPTH_TARGET` (default 1_000_000), `DEPTH_PAYLOAD` (bytes,
//! default 256), `DEPTH_SAMPLES` (latency samples per checkpoint, default
//! 2000), `DEPTH_RESIDENT_MAX` (resident-body budget, default 64 MiB).

use std::time::Instant;

use ramqp::{ConnectionBuilder, Message};

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

fn rss_mib() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmRSS:"))?
                .split_whitespace()
                .nth(1)?
                .parse::<u64>()
                .ok()
        })
        .map(|kib| kib as f64 / 1024.0)
        .unwrap_or(0.0)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("warn")),
        )
        .try_init();
    let target = env_usize("DEPTH_TARGET", 1_000_000);
    let payload = env_usize("DEPTH_PAYLOAD", 256);
    let samples = env_usize("DEPTH_SAMPLES", 2_000);
    let data_dir = std::env::var("DEPTH_DATA_DIR").ok();

    let mut config = ramqp_broker::BrokerConfig::default();
    config.max_queue_depth = target + samples * 8 + 10_000;
    config.data_dir = data_dir.clone().map(Into::into);
    config.resident_bytes_max = env_usize("DEPTH_RESIDENT_MAX", 64 * 1024 * 1024);
    if let Some(dir) = &data_dir {
        // Fresh run: stale spill/snapshot dirs would skew nothing, but a
        // stale durable store would.
        std::fs::remove_dir_all(dir).ok();
    }
    let bound = ramqp_broker::Broker::new(config)
        .bind("127.0.0.1:0")
        .await?;
    let addr = bound.local_addr();
    tokio::spawn(bound.run());
    println!(
        "deep-queue bench: target depth {target}, {payload} B bodies, paging {}",
        match &data_dir {
            Some(dir) => format!("ON ({dir})"),
            None => "OFF (all resident)".to_owned(),
        }
    );

    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await?;
    let session = conn.begin_session().await?;
    let producer = session.create_producer("/quorum/deep").await?;

    let checkpoints = [
        0usize, 10_000, 100_000, 250_000, 500_000, 1_000_000, 2_000_000,
    ];
    let mut filled = 0usize;
    for &depth in checkpoints.iter().filter(|&&d| d <= target) {
        // Fill to the checkpoint (settled sends; confirmed batch-wise by
        // producer credit).
        while filled < depth {
            producer
                .send_settled(Message::data(vec![0u8; payload]))
                .await?;
            filled += 1;
        }
        // Closed-loop publish→accepted latency at this depth.
        let mut lat = Vec::with_capacity(samples);
        for _ in 0..samples {
            let start = Instant::now();
            let outcome = producer.send(Message::data(vec![1u8; payload])).await?;
            lat.push(start.elapsed().as_nanos() as u64);
            filled += 1;
            assert!(
                matches!(outcome, ramqp::types::messaging::DeliveryState::Accepted(_)),
                "publish refused at depth {filled}: {outcome:?}"
            );
        }
        lat.sort_unstable();
        let us = |n: u64| n as f64 / 1_000.0;
        println!(
            "depth {:>9}: publish-confirm µs p50 {:>7.1}  p99 {:>8.1}  p99.9 {:>8.1}  max {:>9.1}   RSS {:>7.1} MiB",
            filled,
            us(percentile(&lat, 0.50)),
            us(percentile(&lat, 0.99)),
            us(percentile(&lat, 0.999)),
            us(*lat.last().unwrap()),
            rss_mib(),
        );
    }

    // Drain everything: throughput out of a deep (possibly disk-backed) queue.
    let mut consumer = session.create_consumer("/quorum/deep").await?;
    let start = Instant::now();
    let mut received = 0usize;
    let mut last = None;
    while received < filled {
        let d = consumer.recv().await?;
        received += 1;
        if received == filled {
            last = Some(d);
        } else if received.is_multiple_of(64) {
            consumer.accept_through(&d).await?;
        }
    }
    if let Some(d) = last {
        consumer.accept_through(&d).await?;
    }
    let elapsed = start.elapsed();
    println!(
        "drain: {filled} msgs in {:.1}s = {:.0} msg/s   final RSS {:.1} MiB",
        elapsed.as_secs_f64(),
        filled as f64 / elapsed.as_secs_f64(),
        rss_mib(),
    );
    conn.close().await?;
    Ok(())
}
