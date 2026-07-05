//! Minimal produce + consume example.
//!
//! Requires a running AMQP 1.0 broker (e.g. ramqp-brokerd, RabbitMQ 4.x, or
//! ActiveMQ Artemis). Defaults to `amqp://guest:guest@localhost:5672`;
//! override with `RAMQP_URL` / `RAMQP_ADDRESS`.
//!
//! ```sh
//! cargo run --example produce_consume
//! RAMQP_URL=amqp://localhost:5673 RAMQP_ADDRESS=/queues/demo cargo run --example produce_consume
//! ```

use ramqp::{Connection, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let url = std::env::var("RAMQP_URL")
        .unwrap_or_else(|_| "amqp://guest:guest@localhost:5672".to_owned());
    let address = std::env::var("RAMQP_ADDRESS").unwrap_or_else(|_| "queue://demo".to_owned());

    // Open a connection (SASL PLAIN is derived from the URL credentials).
    let conn = Connection::open(&url).await?;
    let session = conn.begin_session().await?;

    // Produce a message and await its outcome.
    let producer = session.create_producer(&address).await?;
    let outcome = producer.send(Message::text("hello from ramqp")).await?;
    println!("send settled with: {outcome:?}");

    // Consume one message and accept it.
    let mut consumer = session.create_consumer(&address).await?;
    let delivery = consumer.recv().await?;
    println!("received: {:?}", delivery.message()?);
    consumer.accept(&delivery).await?;

    // Graceful shutdown.
    producer.detach().await?;
    consumer.detach().await?;
    session.end().await?;
    conn.close().await?;
    Ok(())
}
