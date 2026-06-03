//! Throughput / latency harness against a real broker.
//!
//! ```sh
//! RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
//! RAMQP_BROKER_ADDRESS=/queues/ramqp-perf \
//! RAMQP_COUNT=20000 RAMQP_SIZE=256 \
//!     cargo run --release --example throughput
//! ```

use std::time::Instant;

use bytes::Bytes;
use ramqp::{Connection, Message};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env("RAMQP_BROKER_URL", "amqp://guest:guest@localhost:5672");
    let address = env("RAMQP_BROKER_ADDRESS", "/queues/ramqp-perf");
    let count: usize = env("RAMQP_COUNT", "20000").parse()?;
    let size: usize = env("RAMQP_SIZE", "256").parse()?;
    let bytes_total = (count * size) as f64;

    let body = Bytes::from(vec![0x61u8; size]);
    let conn = Connection::open(&url).await?;
    let session = conn.begin_session().await?;
    let producer = session.create_producer(&address).await?;

    // --- produce throughput (pre-settled, fire-and-forget) ---
    let t = Instant::now();
    for _ in 0..count {
        producer.send_settled(Message::data(body.clone())).await?;
    }
    let produce = t.elapsed().as_secs_f64();
    println!(
        "produce (settled): {count} msgs of {size}B in {produce:.3}s = {:.0} msg/s, {:.1} MB/s",
        count as f64 / produce,
        bytes_total / produce / 1e6
    );

    // --- consume throughput ---
    let mut consumer = session.create_consumer(&address).await?;
    let t = Instant::now();
    for _ in 0..count {
        let d = consumer.recv().await?;
        consumer.accept(&d).await?;
    }
    let consume = t.elapsed().as_secs_f64();
    println!(
        "consume (accept): {count} msgs in {consume:.3}s = {:.0} msg/s, {:.1} MB/s",
        count as f64 / consume,
        bytes_total / consume / 1e6
    );

    // --- round-trip latency (awaited disposition, sequential) ---
    let rounds = 2000.min(count);
    let t = Instant::now();
    for _ in 0..rounds {
        let _ = producer.send(Message::data(body.clone())).await?;
    }
    let rt = t.elapsed();
    println!(
        "send+settle latency: {rounds} awaited sends, mean {:.1} µs/msg ({:.0} msg/s sequential)",
        rt.as_secs_f64() * 1e6 / rounds as f64,
        rounds as f64 / rt.as_secs_f64()
    );

    // drain the awaited batch so the queue ends empty
    for _ in 0..rounds {
        let d = consumer.recv().await?;
        consumer.accept(&d).await?;
    }

    conn.close().await?;
    Ok(())
}
