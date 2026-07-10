//! Sustained load generator for the soak / leak stage (`scripts/20-soak.sh`).
//!
//! Drives `LOAD_PAIRS` independent producer/consumer pairs against a broker for
//! `LOAD_SECS`, each a **closed loop** (send a window, drain it, accept) so the
//! queue depth stays bounded — any growth in the *broker's* RSS is then a real
//! leak, not backlog. With `LOAD_CHURN > 0` each pair tears down and reopens its
//! connection every N messages, exercising the connection open/close path that
//! hid the close-time settlement-drain requeue bug.
//!
//! Prints a throughput sample every `LOAD_REPORT_SECS` (`t=<s> total=<n>
//! rate=<msg/s>`) and a final summary line the soak script parses.
//!
//! Env: `LOAD_URL` `LOAD_ADDRESS` `LOAD_SECS` `LOAD_PAIRS` `LOAD_PAYLOAD`
//! `LOAD_WINDOW` `LOAD_CHURN` `LOAD_REPORT_SECS`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ramqp::{ConnectionBuilder, Message};

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}
fn env_string(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env_string("LOAD_URL", "amqp://127.0.0.1:5672");
    let base_addr = env_string("LOAD_ADDRESS", "/queues/soak");
    let secs = env_usize("LOAD_SECS", 60) as u64;
    let pairs = env_usize("LOAD_PAIRS", 8).max(1);
    let payload = env_usize("LOAD_PAYLOAD", 256).max(1);
    let window = env_usize("LOAD_WINDOW", 100).max(1);
    let churn = env_usize("LOAD_CHURN", 0); // reconnect every N msgs; 0 = never
    let report = env_usize("LOAD_REPORT_SECS", 5).max(1) as u64;

    println!(
        "loadgen: url={url} addr={base_addr}-<i> pairs={pairs} payload={payload}B \
         window={window} churn={churn} secs={secs}"
    );

    let total = Arc::new(AtomicU64::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let start = Instant::now();
    let deadline = start + Duration::from_secs(secs);

    let reporter = {
        let (total, stop) = (total.clone(), stop.clone());
        tokio::spawn(async move {
            let mut last = 0u64;
            let mut last_t = Instant::now();
            while !stop.load(Ordering::Relaxed) {
                tokio::time::sleep(Duration::from_secs(report)).await;
                let now = Instant::now();
                let cur = total.load(Ordering::Relaxed);
                let dt = now.duration_since(last_t).as_secs_f64().max(1e-9);
                println!(
                    "t={:.0}s total={} rate={:.0} msg/s",
                    now.duration_since(start).as_secs_f64(),
                    cur,
                    (cur - last) as f64 / dt
                );
                last = cur;
                last_t = now;
            }
        })
    };

    let mut tasks = Vec::new();
    for i in 0..pairs {
        let url = url.clone();
        let addr = format!("{base_addr}-{i}");
        let total = total.clone();
        tasks.push(tokio::spawn(async move {
            'session: while Instant::now() < deadline {
                // (Re)establish the whole client stack; any setup hiccup just
                // retries after a short backoff.
                let Ok(conn) = ConnectionBuilder::new(&url).connect().await else {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    continue;
                };
                let setup = async {
                    let session = conn.begin_session().await.ok()?;
                    let producer = session.create_producer(&addr).await.ok()?;
                    let consumer = session.create_consumer(&addr).await.ok()?;
                    Some((producer, consumer))
                };
                let Some((producer, mut consumer)) = setup.await else {
                    let _ = conn.close().await;
                    continue;
                };

                let mut since_reconnect = 0usize;
                loop {
                    if Instant::now() >= deadline {
                        let _ = conn.close().await;
                        break 'session;
                    }
                    // Send one window of settled messages.
                    for _ in 0..window {
                        if producer
                            .send_settled(Message::data(vec![0u8; payload]))
                            .await
                            .is_err()
                        {
                            let _ = conn.close().await;
                            continue 'session;
                        }
                    }
                    // Drain exactly that window and accept each.
                    for _ in 0..window {
                        match consumer.recv().await {
                            Ok(d) => {
                                if consumer.accept(&d).await.is_err() {
                                    let _ = conn.close().await;
                                    continue 'session;
                                }
                            }
                            Err(_) => {
                                let _ = conn.close().await;
                                continue 'session;
                            }
                        }
                    }
                    total.fetch_add(window as u64, Ordering::Relaxed);
                    since_reconnect += window;
                    if churn > 0 && since_reconnect >= churn {
                        // Graceful close then reconnect: this is the path that
                        // must not requeue already-acked messages.
                        let _ = conn.close().await;
                        continue 'session;
                    }
                }
            }
        }));
    }

    for t in tasks {
        let _ = t.await;
    }
    stop.store(true, Ordering::Relaxed);
    let _ = reporter.await;

    let elapsed = start.elapsed().as_secs_f64().max(1e-9);
    let n = total.load(Ordering::Relaxed);
    println!(
        "loadgen done: total={n} in {elapsed:.1}s = {:.0} msg/s",
        n as f64 / elapsed
    );
    Ok(())
}
