//! Apples-to-apples throughput comparison: `ramqp` vs `fe2o3-amqp`.
//!
//! Both clients use default connection settings and the same broker + queue.
//! Each measures, over the same N messages of the same body size:
//!   * send throughput  — send + await per-message broker settlement
//!   * recv throughput   — recv + accept (settle) every message
//!
//! Each client only consumes the messages it produced, so body encoding is
//! self-consistent. Run against a live broker:
//!
//!   AMQP_URL=amqp://guest:guest@localhost:5672 \
//!   AMQP_ADDRESS=/queues/ramqp_it \
//!     cargo run --release --bin bench
//!
//! Env: BENCH_N (default 2000), BENCH_BODY_BYTES (default 256).

use std::time::Instant;

fn url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".into())
}
fn address() -> String {
    std::env::var("AMQP_ADDRESS").unwrap_or_else(|_| "/queues/ramqp_it".into())
}
fn n() -> usize {
    std::env::var("BENCH_N")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(2000)
}
fn body() -> String {
    let bytes = std::env::var("BENCH_BODY_BYTES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(256);
    "x".repeat(bytes)
}

struct Result {
    name: &'static str,
    send_per_s: f64,
    recv_per_s: f64,
}

#[tokio::main]
async fn main() -> std::result::Result<(), Box<dyn std::error::Error>> {
    let n = n();
    let body = body();
    println!(
        "benchmark: {n} messages, {} byte bodies, broker {}, queue {}\n",
        body.len(),
        url(),
        address()
    );

    // BENCH_CLIENT lets the caller run a single client per process so trials
    // can be isolated (fresh broker between them) instead of measured back to
    // back on a warm queue, which biases the second client.
    let which = std::env::var("BENCH_CLIENT").unwrap_or_else(|_| "both".into());
    let mut results = Vec::new();
    if which == "ramqp" || which == "both" {
        results.push(bench_ramqp(n, &body).await?);
    }
    if which == "fe2o3" || which == "both" {
        results.push(bench_fe2o3(n, &body).await?);
    }

    println!(
        "\n{:<14} {:>16} {:>16}",
        "client", "send msg/s", "recv msg/s"
    );
    println!("{:-<14} {:->16} {:->16}", "", "", "");
    for r in &results {
        println!(
            "{:<14} {:>16.0} {:>16.0}",
            r.name, r.send_per_s, r.recv_per_s
        );
    }
    Ok(())
}

async fn bench_ramqp(
    n: usize,
    body: &str,
) -> std::result::Result<Result, Box<dyn std::error::Error>> {
    use ramqp::{Connection, Message};

    let conn = Connection::open(&url()).await?;
    let session = conn.begin_session().await?;

    let producer = session.create_producer(&address()).await?;
    let t = Instant::now();
    for _ in 0..n {
        producer.send(Message::text(body)).await?;
    }
    let send_per_s = n as f64 / t.elapsed().as_secs_f64();
    producer.detach().await?;

    let mut consumer = session.create_consumer(&address()).await?;
    let t = Instant::now();
    for _ in 0..n {
        let d = consumer.recv().await?;
        consumer.accept(&d).await?;
    }
    let recv_per_s = n as f64 / t.elapsed().as_secs_f64();
    consumer.detach().await?;

    session.end().await?;
    conn.close().await?;
    Ok(Result {
        name: "ramqp",
        send_per_s,
        recv_per_s,
    })
}

async fn bench_fe2o3(
    n: usize,
    body: &str,
) -> std::result::Result<Result, Box<dyn std::error::Error>> {
    use fe2o3_amqp::{Connection, Receiver, Sender, Session};

    let mut conn = Connection::open("bench", url().as_str()).await?;
    let mut session = Session::begin(&mut conn).await?;

    let mut sender = Sender::attach(&mut session, "bench-sender", address().as_str()).await?;
    let t = Instant::now();
    for _ in 0..n {
        sender.send(body).await?;
    }
    let send_per_s = n as f64 / t.elapsed().as_secs_f64();
    sender.close().await?;

    let mut receiver = Receiver::attach(&mut session, "bench-receiver", address().as_str()).await?;
    let t = Instant::now();
    for _ in 0..n {
        let d = receiver.recv::<String>().await?;
        receiver.accept(&d).await?;
    }
    let recv_per_s = n as f64 / t.elapsed().as_secs_f64();
    receiver.close().await?;

    session.end().await?;
    conn.close().await?;
    Ok(Result {
        name: "fe2o3-amqp",
        send_per_s,
        recv_per_s,
    })
}
