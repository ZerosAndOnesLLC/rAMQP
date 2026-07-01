//! Minimal produce + consume example.
//!
//! Requires a running AMQP 1.0 broker (e.g. ActiveMQ Artemis or RabbitMQ with
//! the AMQP 1.0 plugin) reachable at the URL below.
//!
//! ```sh
//! cargo run --example produce_consume
//! ```

use ramqp::{Connection, Message};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Open a connection (SASL PLAIN is derived from the URL credentials).
    let conn = Connection::open("amqp://guest:guest@localhost:5672").await?;
    let session = conn.begin_session().await?;

    // Produce a message and await its outcome.
    let producer = session.create_producer("queue://demo").await?;
    let outcome = producer.send(Message::text("hello from ramqp")).await?;
    println!("send settled with: {outcome:?}");

    // Consume one message and accept it.
    let mut consumer = session.create_consumer("queue://demo").await?;
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
