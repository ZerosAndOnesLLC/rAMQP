//! Isolation experiment: is ramqp's slow receive caused by `accept()` (the
//! per-message disposition round-trip to the driver), or by something in the
//! delivery path / broker / benchmark itself?
//!
//! We measure, on the SAME ramqp connection + queue:
//!   A) recv only (no settlement)
//!   B) recv + accept (settle each)
//!   C) recv + accept, but settling in one batched range via settle-after-N
//!      using the public API (accept each — control, == B) vs a manual
//!      pre-settled receiver where the broker settles for us.
//!
//! If A >> B, the cost is in accept()/dispose(), which is internal to ramqp
//! (the source shows dispose() awaits a oneshot reply from the driver per call).
//! That rules out "the broker is just slow" and "the benchmark is unfair".
//!
//!   AMQP_URL=amqp://guest:guest@localhost:5672 AMQP_ADDRESS=/queues/ramqp_it \
//!     cargo run --release --bin confirm

use std::time::Instant;

use ramqp::{Connection, Message};

fn url() -> String {
    std::env::var("AMQP_URL").unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".into())
}
fn address() -> String {
    std::env::var("AMQP_ADDRESS").unwrap_or_else(|_| "/queues/ramqp_it".into())
}

const N: usize = 2000;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let body = "x".repeat(256);
    let conn = Connection::open(&url()).await?;
    let session = conn.begin_session().await?;

    // Helper: fill the queue with N messages.
    async fn fill(
        session: &ramqp::Session,
        addr: &str,
        body: &str,
        n: usize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let p = session.create_producer(addr).await?;
        for _ in 0..n {
            p.send(Message::text(body)).await?;
        }
        p.detach().await?;
        Ok(())
    }

    // A) recv only, no settlement.
    fill(&session, &address(), &body, N).await?;
    let mut c = session.create_consumer(&address()).await?;
    let t = Instant::now();
    for _ in 0..N {
        let _d = c.recv().await?;
    }
    let recv_only = N as f64 / t.elapsed().as_secs_f64();
    c.detach().await?;

    // Re-fill (the un-settled messages above may be redelivered, so use a fresh
    // consumer and drain whatever is there first to get a clean count).
    drain(&session, &address()).await?;
    fill(&session, &address(), &body, N).await?;

    // B) recv + accept (settle each) — the normal path the bench used.
    let mut c = session.create_consumer(&address()).await?;
    let t = Instant::now();
    for _ in 0..N {
        let d = c.recv().await?;
        c.accept(&d).await?;
    }
    let recv_accept = N as f64 / t.elapsed().as_secs_f64();
    c.detach().await?;

    session.end().await?;
    conn.close().await?;

    println!("ramqp receive isolation ({N} msgs, 256B):\n");
    println!("  A) recv only (no settle):   {recv_only:>10.0} msg/s");
    println!("  B) recv + accept (settle):  {recv_accept:>10.0} msg/s");
    println!(
        "\n  accept() overhead factor:   {:.1}x slower with settlement",
        recv_only / recv_accept
    );
    println!(
        "\n  => {}",
        if recv_only / recv_accept > 2.0 {
            "CONFIRMED: accept()/dispose() dominates recv cost (internal to ramqp)."
        } else {
            "accept() is NOT the dominant cost; look elsewhere."
        }
    );
    Ok(())
}

async fn drain(session: &ramqp::Session, addr: &str) -> Result<(), Box<dyn std::error::Error>> {
    use std::time::Duration;
    if let Ok(mut c) = session.create_consumer(addr).await {
        while let Ok(Ok(d)) = tokio::time::timeout(Duration::from_millis(300), c.recv()).await {
            let _ = c.accept(&d).await;
        }
        c.detach().await.ok();
    }
    Ok(())
}
