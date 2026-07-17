//! Diagnostic: attach a consumer and drain whatever a queue currently
//! holds, printing the count (and the embedded `latency`-bin timestamps if
//! present). `DRAIN_URL` / `DRAIN_ADDRESS` select the target.

use ramqp::ConnectionBuilder;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("DRAIN_URL").unwrap_or_else(|_| "amqp://127.0.0.1:5672".into());
    let address = std::env::var("DRAIN_ADDRESS").unwrap_or_else(|_| "/queues/bench-lat".into());
    let conn = ConnectionBuilder::new(&url).connect().await?;
    let session = conn.begin_session().await?;
    let mut consumer = session.create_consumer(&address).await?;
    let mut drained = 0usize;
    while let Ok(Ok(d)) =
        tokio::time::timeout(std::time::Duration::from_secs(1), consumer.recv()).await
    {
        drained += 1;
        consumer.accept(&d).await?;
    }
    println!("drained {drained} leftover messages from {address}");
    conn.close().await?;
    Ok(())
}
