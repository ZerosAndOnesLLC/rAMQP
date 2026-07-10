//! Two-phase durability check for `scripts/30-chaos.sh` (part B).
//!
//! `RECOVER_PHASE=produce` publishes seq `0..N` to a durable/quorum queue,
//! confirming each is `Accepted` (the on-disk durability confirm), then exits.
//! The script then **SIGKILLs** the broker and starts a fresh process on the
//! same data dir. `RECOVER_PHASE=consume` then drains and asserts every seq
//! survived the crash. This exercises real crash recovery (kill -9 + cold
//! start from disk), which is stronger than the in-process suite's graceful
//! restart.
//!
//! Env: `RECOVER_URL` `RECOVER_ADDRESS` `RECOVER_N` `RECOVER_PHASE`.

use std::time::{Duration, Instant};

use ramqp::types::messaging::{Body, DeliveryState};
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

fn seq_of(d: &ramqp::Delivery) -> Option<u64> {
    match &d.message().ok()?.body {
        Body::Data(s) => Some(u64::from_be_bytes(s.first()?.get(..8)?.try_into().ok()?)),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env_string("RECOVER_URL", "amqp://127.0.0.1:5672");
    let addr = env_string("RECOVER_ADDRESS", "/durable/recovery");
    let n = env_usize("RECOVER_N", 5000) as u64;
    let phase = env_string("RECOVER_PHASE", "produce");

    let conn = ConnectionBuilder::new(&url).connect().await?;
    let session = conn.begin_session().await?;

    match phase.as_str() {
        "produce" => {
            let producer = session.create_producer(&addr).await?;
            for seq in 0..n {
                let mut body = vec![0u8; 16];
                body[..8].copy_from_slice(&seq.to_be_bytes());
                // Retry until the durability confirm lands.
                loop {
                    match producer.send(Message::data(body.clone())).await {
                        Ok(DeliveryState::Accepted(_)) => break,
                        _ => tokio::time::sleep(Duration::from_millis(20)).await,
                    }
                }
            }
            conn.close().await?;
            println!("recover/produce: {n} messages confirmed durable to {addr}");
        }
        "consume" => {
            let mut consumer = session.create_consumer(&addr).await?;
            let mut seen = vec![false; n as usize];
            let mut count = 0u64;
            let deadline = Instant::now() + Duration::from_secs(60);
            while count < n && Instant::now() < deadline {
                match tokio::time::timeout(Duration::from_secs(10), consumer.recv()).await {
                    Ok(Ok(d)) => {
                        if let Some(s) = seq_of(&d)
                            && s < n
                            && !seen[s as usize]
                        {
                            seen[s as usize] = true;
                            count += 1;
                        }
                        let _ = consumer.accept(&d).await;
                    }
                    _ => break,
                }
            }
            conn.close().await.ok();
            let missing = seen.iter().filter(|&&b| !b).count();
            println!("recover/consume: recovered {count}/{n} (missing {missing}) from {addr}");
            if missing > 0 {
                eprintln!("FAIL: {missing} durable message(s) did NOT survive the crash");
                std::process::exit(1);
            }
            println!("PASS: all {n} durable messages survived the crash");
        }
        other => {
            eprintln!("RECOVER_PHASE must be produce|consume, got {other}");
            std::process::exit(2);
        }
    }
    Ok(())
}
