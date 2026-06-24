//! Transport comparison: ramqp receive throughput over the transport named by
//! `AMQP_URL`'s scheme (`amqp://` = TCP, `ws://` = WebSocket), against the SAME
//! broker. Uses one long-lived connection (a fresh session per trial) so the
//! measurement isolates the transport's steady-state cost and never depends on
//! reconnect behavior. Mirrors the rig's steady-state recv+accept loop.
//!
//!   AMQP_ADDRESS=ramqp_it AMQP_URL=amqp://guest:guest@localhost:5682 \
//!     cargo run -p ramqp-bench-compare --release --bin wscompare
//!   AMQP_ADDRESS=ramqp_it AMQP_URL=ws://guest:guest@localhost:5682 \
//!     cargo run -p ramqp-bench-compare --release --bin wscompare

use std::time::{Duration, Instant};

use ramqp::config::CreditMode;
use ramqp::{Connection, Message, Session};

const CREDIT: u32 = 1000;
const TRIALS: usize = 5;
const WARMUP: usize = 1;
const SIZES: [usize; 3] = [64, 1024, 8192];
const N: usize = 5000;

fn url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".into())
}
fn address() -> String {
    std::env::var("AMQP_ADDRESS").unwrap_or_else(|_| "ramqp_it".into())
}

fn stats(mut s: Vec<f64>) -> (f64, f64, f64) {
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (s[s.len() / 2], s[0], s[s.len() - 1])
}

async fn drain(session: &Session, addr: &str) {
    if let Ok(mut c) = session.create_consumer(addr).await {
        while let Ok(Ok(d)) = tokio::time::timeout(Duration::from_millis(300), c.recv()).await {
            let _ = c.accept(&d).await;
        }
        c.detach().await.ok();
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = if url().starts_with("ws") { "ws" } else { "tcp" };
    println!(
        "wscompare: transport={transport}, credit={CREDIT}, N={N}, trials={TRIALS} (+{WARMUP} warmup), broker={}\n",
        url()
    );
    println!(
        "{:<8} {:>8} {:>14} {:>14} {:>14}",
        "xport", "body", "recv med", "recv min", "recv max"
    );
    println!("{:-<8} {:->8} {:->14} {:->14} {:->14}", "", "", "", "", "");

    // One long-lived connection for the whole run (avoids reconnect churn).
    let conn = Connection::open(&url()).await?;

    for &size in &SIZES {
        let body = "x".repeat(size);
        let mut samples = Vec::new();
        for trial in 0..(WARMUP + TRIALS) {
            let session = conn.begin_session().await?;
            drain(&session, &address()).await;

            let producer = session.create_producer(&address()).await?;
            for _ in 0..N {
                producer.send(Message::text(&body)).await?;
            }
            producer.detach().await?;

            let mut consumer = session
                .create_consumer_with(
                    &address(),
                    CreditMode::Auto {
                        initial: CREDIT,
                        refill_threshold: CREDIT / 2,
                    },
                )
                .await?;
            let t = Instant::now();
            for _ in 0..N {
                let d = consumer.recv().await?;
                consumer.accept(&d).await?;
            }
            let rate = N as f64 / t.elapsed().as_secs_f64();
            consumer.detach().await?;
            session.end().await?;
            if trial >= WARMUP {
                samples.push(rate);
            }
        }
        let (med, lo, hi) = stats(samples);
        println!("{transport:<8} {size:>7}B {med:>14.0} {lo:>14.0} {hi:>14.0}");
    }

    conn.close().await?;
    Ok(())
}
