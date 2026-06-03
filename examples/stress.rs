//! Concurrency + large-message stress harness against a real broker.
//!
//! Exercises many producer/consumer pairs multiplexed over ONE connection
//! (stressing the single driver task) plus a large multi-frame message.
//!
//! Pre-declare queues `ramqp-stress-0..N-1` and `ramqp-large`, then:
//! ```sh
//! RAMQP_BROKER_URL=amqp://guest:guest@localhost:5672 \
//! RAMQP_PAIRS=8 RAMQP_PER_PAIR=2000 \
//!     cargo run --release --example stress
//! ```

use std::sync::Arc;
use std::time::Instant;

use bytes::Bytes;
use ramqp::{Connection, Message};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_string())
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env("RAMQP_BROKER_URL", "amqp://guest:guest@localhost:5672");
    let pairs: usize = env("RAMQP_PAIRS", "8").parse()?;
    let per_pair: usize = env("RAMQP_PER_PAIR", "2000").parse()?;

    let conn = Arc::new(Connection::open(&url).await?);

    // ---- concurrency: `pairs` producer/consumer pairs sharing one connection ----
    let t = Instant::now();
    let mut tasks = Vec::new();
    for i in 0..pairs {
        let conn = conn.clone();
        tasks.push(tokio::spawn(async move {
            let address = format!("/queues/ramqp-stress-{i}");
            let session = conn.begin_session().await.expect("begin");
            let producer = session.create_producer(&address).await.expect("producer");
            let mut consumer = session.create_consumer(&address).await.expect("consumer");

            // produce then consume on this pair's own queue
            for n in 0..per_pair {
                producer
                    .send(Message::text(format!("p{i}-{n}")))
                    .await
                    .expect("send");
            }
            for _ in 0..per_pair {
                let d = consumer.recv().await.expect("recv");
                consumer.accept(&d).await.expect("accept");
            }
            session.end().await.ok();
        }));
    }
    for t in tasks {
        t.await?;
    }
    let elapsed = t.elapsed().as_secs_f64();
    let total = (pairs * per_pair) as f64;
    println!(
        "concurrency: {pairs} pairs x {per_pair} msgs = {} round-trips in {elapsed:.2}s = {:.0} msg/s",
        pairs * per_pair,
        total / elapsed
    );

    // ---- large multi-frame message round-trip ----
    let session = conn.begin_session().await?;
    let producer = session.create_producer("/queues/ramqp-large").await?;
    let mut consumer = session.create_consumer("/queues/ramqp-large").await?;
    let big = Bytes::from(vec![0x7eu8; 512 * 1024]); // 512 KiB -> many frames
    producer.send(Message::data(big.clone())).await?;
    let d = consumer.recv().await?;
    let received = d.message()?;
    match &received.body {
        ramqp::types::messaging::Body::Data(parts) => {
            let len: usize = parts.iter().map(|p| p.len()).sum();
            assert_eq!(len, big.len(), "large message length mismatch");
            println!("large message: {} KiB round-tripped + reassembled OK", len / 1024);
        }
        other => panic!("unexpected body: {other:?}"),
    }
    consumer.accept(&d).await?;
    drop(producer);
    drop(consumer);
    session.end().await.ok();

    // All task clones have been joined, so this is the sole owner.
    if let Ok(conn) = Arc::try_unwrap(conn) {
        conn.close().await?;
    }
    Ok(())
}
