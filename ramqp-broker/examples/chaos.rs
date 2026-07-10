//! Zero-loss verifier for the chaos / fault-injection stage
//! (`scripts/30-chaos.sh`). The script spawns a cluster and kills+restarts
//! nodes underneath this client; this binary proves the HA contract:
//!
//!   **every message the broker ACCEPTED is eventually delivered** — no
//!   accepted-message loss across leader/follower failovers (at-least-once, so
//!   duplicates are allowed and reported).
//!
//! A producer publishes seq `0..N` to a quorum queue, retrying each until it is
//! `Accepted` (the publisher-confirm pattern — a rejection during an election
//! is the cue to retry). A consumer drains concurrently, recording the set of
//! received seqs. At the end, every seq must have been received. The client
//! connects to a node the script keeps ALIVE, so the connection itself
//! survives; reconnect logic covers transient fabric errors regardless.
//!
//! Env: `CHAOS_PRODUCER_URL` `CHAOS_CONSUMER_URL` `CHAOS_QUEUE` `CHAOS_N`
//! `CHAOS_PAYLOAD` `CHAOS_DEADLINE_SECS`.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use ramqp::types::messaging::{Body, DeliveryState};
use ramqp::{Connection, ConnectionBuilder, Consumer, Message, Producer, Session};

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
    let msg = d.message().ok()?;
    match &msg.body {
        Body::Data(sections) => {
            let first = sections.first()?;
            Some(u64::from_be_bytes(first.get(..8)?.try_into().ok()?))
        }
        _ => None,
    }
}

async fn connect_producer(url: &str, addr: &str) -> Option<(Connection, Session, Producer)> {
    let conn = ConnectionBuilder::new(url).connect().await.ok()?;
    let session = conn.begin_session().await.ok()?;
    let producer = session.create_producer(addr).await.ok()?;
    Some((conn, session, producer))
}

async fn connect_consumer(url: &str, addr: &str) -> Option<(Connection, Session, Consumer)> {
    let conn = ConnectionBuilder::new(url).connect().await.ok()?;
    let session = conn.begin_session().await.ok()?;
    let consumer = session.create_consumer(addr).await.ok()?;
    Some((conn, session, consumer))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let purl = env_string("CHAOS_PRODUCER_URL", "amqp://127.0.0.1:5672");
    let curl = env_string("CHAOS_CONSUMER_URL", "amqp://127.0.0.1:5672");
    let queue = env_string("CHAOS_QUEUE", "/quorum/chaos");
    let n = env_usize("CHAOS_N", 20_000) as u64;
    let payload = env_usize("CHAOS_PAYLOAD", 64).max(8);
    let deadline_secs = env_usize("CHAOS_DEADLINE_SECS", 180) as u64;

    println!(
        "chaos: producer={purl} consumer={curl} queue={queue} n={n} deadline={deadline_secs}s"
    );

    let start = Instant::now();
    let deadline = start + Duration::from_secs(deadline_secs);
    let received: Arc<Vec<AtomicBool>> = Arc::new((0..n).map(|_| AtomicBool::new(false)).collect());
    let recv_count = Arc::new(AtomicUsize::new(0));
    let dupes = Arc::new(AtomicUsize::new(0));
    let accepted = Arc::new(AtomicUsize::new(0));
    let stop = Arc::new(AtomicBool::new(false));
    let step = (n / 10).max(1);

    // Producer: publish every seq, retrying until Accepted.
    let producer = {
        let (accepted, purl, queue) = (accepted.clone(), purl.clone(), queue.clone());
        tokio::spawn(async move {
            let mut bundle = None;
            let mut seq = 0u64;
            while seq < n {
                if Instant::now() >= deadline {
                    eprintln!("producer: DEADLINE with {seq}/{n} accepted");
                    break;
                }
                if bundle.is_none() {
                    bundle = connect_producer(&purl, &queue).await;
                    if bundle.is_none() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                let mut body = vec![0u8; payload];
                body[..8].copy_from_slice(&seq.to_be_bytes());
                let p = &bundle.as_ref().unwrap().2;
                match p.send(Message::data(body)).await {
                    Ok(DeliveryState::Accepted(_)) => {
                        let c = accepted.fetch_add(1, Ordering::Relaxed) + 1;
                        seq += 1;
                        if (c as u64).is_multiple_of(step) {
                            println!(
                                "producer: {c}/{n} accepted ({:.0}s)",
                                start.elapsed().as_secs_f64()
                            );
                        }
                    }
                    Ok(_) => tokio::time::sleep(Duration::from_millis(50)).await, // retry same seq
                    Err(_) => bundle = None,                                      // reconnect
                }
            }
        })
    };

    // Consumer: drain concurrently, dedupe by seq.
    let consumer = {
        let (received, recv_count, dupes, stop, curl, queue) = (
            received.clone(),
            recv_count.clone(),
            dupes.clone(),
            stop.clone(),
            curl.clone(),
            queue.clone(),
        );
        tokio::spawn(async move {
            let mut bundle = None;
            while !stop.load(Ordering::Relaxed) && Instant::now() < deadline {
                if bundle.is_none() {
                    bundle = connect_consumer(&curl, &queue).await;
                    if bundle.is_none() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                }
                let b = bundle.as_mut().unwrap();
                match tokio::time::timeout(Duration::from_secs(5), b.2.recv()).await {
                    Ok(Ok(d)) => {
                        if let Some(s) = seq_of(&d) {
                            if s < n && !received[s as usize].swap(true, Ordering::Relaxed) {
                                let c = recv_count.fetch_add(1, Ordering::Relaxed) + 1;
                                if (c as u64).is_multiple_of(step) {
                                    println!(
                                        "consumer: {c}/{n} received ({:.0}s)",
                                        start.elapsed().as_secs_f64()
                                    );
                                }
                            } else {
                                dupes.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        let _ = b.2.accept(&d).await;
                    }
                    Ok(Err(_)) => bundle = None, // link/conn error → reconnect
                    Err(_) => { /* recv timeout: loop and re-check counts */ }
                }
            }
        })
    };

    let _ = producer.await;
    // Producer done (or deadline). Let the consumer catch up to N or the deadline.
    while recv_count.load(Ordering::Relaxed) < n as usize && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    stop.store(true, Ordering::Relaxed);
    let _ = consumer.await;

    // Verdict.
    let acc = accepted.load(Ordering::Relaxed);
    let rec = recv_count.load(Ordering::Relaxed);
    let dup = dupes.load(Ordering::Relaxed);
    let missing: Vec<u64> = (0..n)
        .filter(|&s| !received[s as usize].load(Ordering::Relaxed))
        .collect();
    println!(
        "chaos result: accepted={acc}/{n} received={rec}/{n} duplicates={dup} missing={} elapsed={:.0}s",
        missing.len(),
        start.elapsed().as_secs_f64()
    );
    if acc < n as usize {
        eprintln!(
            "FAIL: producer could not get {} message(s) accepted before the deadline (liveness)",
            n as usize - acc
        );
        std::process::exit(2);
    }
    if !missing.is_empty() {
        let sample: Vec<u64> = missing.iter().take(20).copied().collect();
        eprintln!(
            "FAIL: {} ACCEPTED message(s) never delivered (loss). sample seqs: {sample:?}",
            missing.len()
        );
        std::process::exit(1);
    }
    println!("PASS: zero accepted-message loss across the chaos run");
    Ok(())
}
