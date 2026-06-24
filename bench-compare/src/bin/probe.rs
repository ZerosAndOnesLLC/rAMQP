//! Minimal connectivity probe with per-step timeouts, for bringing up a new
//! broker/transport (prints exactly where a hang or error occurs).
//!
//!   AMQP_URL=amqp://guest:guest@localhost:5682 AMQP_ADDRESS=ramqp_it \
//!     cargo run -p ramqp-bench-compare --release --bin probe

use std::time::Duration;

use ramqp::{Connection, Message};
use tokio::time::timeout;

const T: Duration = Duration::from_secs(8);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("AMQP_URL")?;
    let addr = std::env::var("AMQP_ADDRESS")?;
    eprintln!("probe: url={url} addr={addr}");

    let conn = timeout(T, Connection::open(&url)).await??;
    eprintln!("  [ok] connected");
    let session = conn.begin_session().await?;
    eprintln!("  [ok] session begun");

    let producer = session.create_producer(&addr).await?;
    eprintln!("  [ok] producer attached");
    match timeout(T, producer.send(Message::text("probe"))).await {
        Ok(Ok(o)) => eprintln!("  [ok] send outcome: {o:?}"),
        Ok(Err(e)) => eprintln!("  [ERR] send: {e}"),
        Err(_) => eprintln!("  [TIMEOUT] send"),
    }
    producer.detach().await.ok();

    let mut consumer = session.create_consumer(&addr).await?;
    eprintln!("  [ok] consumer attached");
    match timeout(T, consumer.recv()).await {
        Ok(Ok(d)) => eprintln!("  [ok] recv: {:?}", d.message()),
        Ok(Err(e)) => eprintln!("  [ERR] recv: {e}"),
        Err(_) => eprintln!("  [TIMEOUT] recv"),
    }
    consumer.detach().await.ok();
    conn.close().await.ok();
    eprintln!("probe: done");
    Ok(())
}
