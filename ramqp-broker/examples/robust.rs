//! Robustness / DoS-resilience driver for `scripts/50-robust.sh`.
//!
//! Hammers a **live** broker daemon with hostile traffic and, after each wave,
//! proves the broker is still alive by round-tripping a real message with the
//! `ramqp` client. The contract under test: adversarial peers get closed/reaped,
//! never crash the accept loop, exhaust fds, or wedge the broker for legitimate
//! clients.
//!
//! Waves: (1) connection flood — open/drop as fast as possible; (2) slow-loris —
//! connect, dribble a partial header, hold; (3) garbage flood — random bytes;
//! (4) malformed frames — a valid header then illegal frames.
//!
//! Env: `ROBUST_URL` (amqp URL), `ROBUST_SECS` (total), `ROBUST_CONNS` (fan-out).
//! Exits non-zero if any post-wave liveness check fails.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use ramqp::{ConnectionBuilder, Message};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

fn env_usize(k: &str, d: usize) -> usize {
    std::env::var(k)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(d)
}
fn env_string(k: &str, d: &str) -> String {
    std::env::var(k).unwrap_or_else(|_| d.to_owned())
}

static SEED: AtomicU64 = AtomicU64::new(0x9e3779b97f4a7c15);
fn xorshift() -> u64 {
    let mut x = SEED.fetch_add(0x2545f4914f6cdd1d, Ordering::Relaxed) | 1;
    x ^= x << 13;
    x ^= x >> 7;
    x ^= x << 17;
    x
}

/// One legitimate produce→consume round trip. `true` = the broker is healthy.
async fn liveness(url: &str) -> bool {
    let attempt = async {
        let conn = ConnectionBuilder::new(url).connect().await.ok()?;
        let session = conn.begin_session().await.ok()?;
        let producer = session
            .create_producer("/queues/robust-canary")
            .await
            .ok()?;
        let mut consumer = session
            .create_consumer("/queues/robust-canary")
            .await
            .ok()?;
        producer
            .send_settled(Message::data(b"ping".to_vec()))
            .await
            .ok()?;
        let d = consumer.recv().await.ok()?;
        consumer.accept(&d).await.ok()?;
        conn.close().await.ok()?;
        Some(())
    };
    matches!(
        tokio::time::timeout(Duration::from_secs(10), attempt).await,
        Ok(Some(()))
    )
}

async fn conn_flood(target: &str, conns: usize, until: Instant) {
    while Instant::now() < until {
        let mut js = Vec::with_capacity(conns);
        for _ in 0..conns {
            let t = target.to_string();
            js.push(tokio::spawn(async move {
                if let Ok(s) = TcpStream::connect(&t).await {
                    drop(s); // slam it shut with no handshake
                }
            }));
        }
        for j in js {
            let _ = j.await;
        }
    }
}

async fn slow_loris(target: &str, conns: usize, until: Instant) {
    // Open many sockets, send a fragment of the header, then just hold them —
    // the broker's inbound-handshake timeout must reap them.
    let mut held = Vec::new();
    for _ in 0..conns {
        if let Ok(mut s) = TcpStream::connect(target).await {
            let _ = s.write_all(b"AM").await; // partial protocol header, never completed
            held.push(s);
        }
    }
    while Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    drop(held);
}

async fn garbage_flood(target: &str, conns: usize, until: Instant) {
    while Instant::now() < until {
        let mut js = Vec::with_capacity(conns);
        for _ in 0..conns {
            let t = target.to_string();
            js.push(tokio::spawn(async move {
                if let Ok(mut s) = TcpStream::connect(&t).await {
                    let mut buf = [0u8; 256];
                    for b in buf.iter_mut() {
                        *b = xorshift() as u8;
                    }
                    let _ = s.write_all(&buf).await;
                    let mut sink = [0u8; 256];
                    let _ =
                        tokio::time::timeout(Duration::from_millis(200), s.read(&mut sink)).await;
                }
            }));
        }
        for j in js {
            let _ = j.await;
        }
    }
}

async fn malformed_frames(target: &str, until: Instant) {
    // A valid bare-AMQP header (the default broker offers it) then a series of
    // illegal frames: undersized, oversized, bad data-offset, random tail.
    let bad_frames: [Vec<u8>; 4] = [
        vec![0, 0, 0, 4, 2, 0, 0, 0],             // size 4 < header len 8
        vec![0xFF, 0xFF, 0xFF, 0xFF, 2, 0, 0, 0], // size ~4GiB > max-frame-size
        vec![0, 0, 0, 8, 1, 0, 0, 0],             // data-offset 1 → header_len 4 < 8
        vec![0, 0, 0, 12, 2, 0, 0, 0, 0xDE, 0xAD, 0xBE, 0xEF], // garbage body
    ];
    while Instant::now() < until {
        if let Ok(mut s) = TcpStream::connect(target).await {
            let _ = s.write_all(b"AMQP\x00\x01\x00\x00").await;
            let mut echo = [0u8; 8];
            let _ = tokio::time::timeout(Duration::from_millis(200), s.read_exact(&mut echo)).await;
            let f = &bad_frames[(xorshift() as usize) % bad_frames.len()];
            let _ = s.write_all(f).await;
            let mut sink = [0u8; 512];
            let _ = tokio::time::timeout(Duration::from_millis(200), s.read(&mut sink)).await;
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = env_string("ROBUST_URL", "amqp://127.0.0.1:5672");
    let secs = env_usize("ROBUST_SECS", 20) as u64;
    let conns = env_usize("ROBUST_CONNS", 200).max(1);
    let authority = url.split("://").nth(1).unwrap_or(&url);
    let target = authority.split('/').next().unwrap_or(authority).to_string();
    let phase = Duration::from_secs((secs / 4).max(2));

    println!(
        "robust: target={target} conns={conns} phase={}s",
        phase.as_secs()
    );

    let mut failures = 0usize;
    if !liveness(&url).await {
        eprintln!("FAIL: broker not healthy before attacks even began");
        std::process::exit(3);
    }
    println!("liveness: ok (baseline)");

    let waves: [(&str, _); 4] = [
        ("connection-flood", 0u8),
        ("slow-loris", 1u8),
        ("garbage-flood", 2u8),
        ("malformed-frames", 3u8),
    ];
    for (name, kind) in waves {
        println!("wave: {name} for {}s ...", phase.as_secs());
        let until = Instant::now() + phase;
        match kind {
            0 => conn_flood(&target, conns, until).await,
            1 => slow_loris(&target, conns, until).await,
            2 => garbage_flood(&target, conns, until).await,
            _ => malformed_frames(&target, until).await,
        }
        // Give the broker a beat to reap, then check it still serves clients.
        tokio::time::sleep(Duration::from_millis(500)).await;
        if liveness(&url).await {
            println!("liveness: ok (after {name})");
        } else {
            eprintln!("FAIL: broker unresponsive after {name}");
            failures += 1;
        }
    }

    if failures == 0 {
        println!("PASS: broker stayed live through all {} waves", waves.len());
        Ok(())
    } else {
        eprintln!("FAIL: {failures} liveness check(s) failed");
        std::process::exit(1);
    }
}
