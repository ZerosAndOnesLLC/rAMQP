//! Rigorous, fair receive-throughput rig: ramqp vs fe2o3-amqp.
//!
//! Fairness controls (the earlier ad-hoc bench lacked these):
//!   * **Matched credit windows** — both clients use Auto credit = 1000.
//!     (Defaults differ: fe2o3 = 200, ramqp = 1000 — a 5x gap that alone
//!     explains much of the earlier delta and fe2o3's variance.)
//!   * **Warmup trial discarded** before timing.
//!   * **Steady-state receive** — prefill the queue with N, then time draining
//!     exactly N with recv + accept (settle each). Send is not timed here.
//!   * **Multiple body sizes**, multiple trials, report median / min / max.
//!
//! Each client produces its own messages (same-client encoding) and receives
//! them; only the receive loop is timed. Run one client per process so the
//! orchestrator can restart the broker between them:
//!
//!   AMQP_URL=... AMQP_ADDRESS=/queues/ramqp_it BENCH_CLIENT=ramqp \
//!     cargo run --release --bin rig

use std::time::{Duration, Instant};

const CREDIT: u32 = 1000;
const TRIALS: usize = 5;
const WARMUP: usize = 1;
const SIZES: [usize; 3] = [64, 1024, 8192];
const N: usize = 5000;

fn url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".into())
}
fn address() -> String {
    std::env::var("AMQP_ADDRESS").unwrap_or_else(|_| "/queues/ramqp_it".into())
}

fn stats(mut s: Vec<f64>) -> (f64, f64, f64) {
    s.sort_by(|a, b| a.partial_cmp(b).unwrap());
    (s[s.len() / 2], s[0], s[s.len() - 1]) // median, min, max
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = std::env::var("BENCH_CLIENT").unwrap_or_else(|_| "ramqp".into());
    println!(
        "rig: client={client}, credit={CREDIT}, N={N}, trials={TRIALS} (+{WARMUP} warmup), \
         broker={}\n",
        url()
    );
    println!(
        "{:<10} {:>8} {:>14} {:>14} {:>14}",
        "client", "body", "recv med", "recv min", "recv max"
    );
    println!("{:-<10} {:->8} {:->14} {:->14} {:->14}", "", "", "", "", "");

    for &size in &SIZES {
        let mut samples = Vec::new();
        for trial in 0..(WARMUP + TRIALS) {
            let rate = match client.as_str() {
                "fe2o3" => trial_fe2o3(size).await?,
                _ => trial_ramqp(size).await?,
            };
            if trial >= WARMUP {
                samples.push(rate);
            }
        }
        let (med, lo, hi) = stats(samples);
        println!(
            "{client:<10} {:>7}B {med:>14.0} {lo:>14.0} {hi:>14.0}",
            size
        );
    }
    Ok(())
}

async fn trial_ramqp(size: usize) -> Result<f64, Box<dyn std::error::Error>> {
    use ramqp::config::CreditMode;
    use ramqp::{Connection, Message};

    let body = "x".repeat(size);
    let conn = Connection::open(&url()).await?;
    let session = conn.begin_session().await?;
    drain_ramqp(&session).await;

    // prefill (untimed)
    let producer = session.create_producer(&address()).await?;
    for _ in 0..N {
        producer.send(Message::text(&body)).await?;
    }
    producer.detach().await?;

    // timed receive with matched credit
    let mut consumer = session
        .create_consumer_with(
            &address(),
            CreditMode::Auto {
                initial: CREDIT,
                refill_threshold: CREDIT / 2,
            },
        )
        .await?;
    // RAMQP_BATCH > 1 settles a whole range in one frame via accept_through()
    // (a capability fe2o3 lacks); default 1 = per-message accept (fair path).
    let batch: usize = std::env::var("RAMQP_BATCH")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(1);
    let t = Instant::now();
    for i in 0..N {
        let d = consumer.recv().await?;
        if batch <= 1 {
            consumer.accept(&d).await?;
        } else if (i + 1) % batch == 0 || i + 1 == N {
            consumer.accept_through(&d).await?; // settles everything up to d
        }
    }
    let rate = N as f64 / t.elapsed().as_secs_f64();
    consumer.detach().await?;
    session.end().await?;
    conn.close().await?;
    Ok(rate)
}

async fn drain_ramqp(session: &ramqp::Session) {
    if let Ok(mut c) = session.create_consumer(&address()).await {
        while let Ok(Ok(d)) = tokio::time::timeout(Duration::from_millis(300), c.recv()).await {
            let _ = c.accept(&d).await;
        }
        c.detach().await.ok();
    }
}

async fn trial_fe2o3(size: usize) -> Result<f64, Box<dyn std::error::Error>> {
    use fe2o3_amqp::link::receiver::CreditMode;
    use fe2o3_amqp::{Connection, Receiver, Sender, Session};

    let body = "x".repeat(size);
    let mut conn = Connection::open("rig", url().as_str()).await?;
    let mut session = Session::begin(&mut conn).await?;

    // drain leftovers (untimed)
    if let Ok(mut r) = Receiver::attach(&mut session, "rig-drain", address().as_str()).await {
        while let Ok(Ok(d)) =
            tokio::time::timeout(Duration::from_millis(300), r.recv::<String>()).await
        {
            let _ = r.accept(&d).await;
        }
        r.close().await.ok();
    }

    // prefill (untimed)
    let mut sender = Sender::attach(&mut session, "rig-send", address().as_str()).await?;
    for _ in 0..N {
        sender.send(body.as_str()).await?;
    }
    sender.close().await?;

    // timed receive with matched credit
    let mut receiver = Receiver::builder()
        .name("rig-recv")
        .source(address().as_str())
        .credit_mode(CreditMode::Auto(CREDIT))
        .attach(&mut session)
        .await?;
    let t = Instant::now();
    for _ in 0..N {
        let d = receiver.recv::<String>().await?;
        receiver.accept(&d).await?;
    }
    let rate = N as f64 / t.elapsed().as_secs_f64();
    receiver.close().await?;
    session.end().await?;
    conn.close().await?;
    Ok(rate)
}
