//! Client-side helpers: connect the published `ramqp` client to a loopback
//! broker and pull typed bodies out of deliveries.

/// Connect the published `ramqp` client to a loopback broker.
pub async fn connect(url: &str) -> ramqp::Connection {
    ramqp::ConnectionBuilder::new(url)
        .connect()
        .await
        .expect("client connects to broker")
}

/// Extract the text body of a delivery, panicking if it is not a string value.
pub fn text_of(delivery: &ramqp::Delivery) -> String {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    let msg = delivery.message().expect("decodable message");
    match msg.body {
        Body::Value(Value::String(s)) => s,
        other => panic!("expected text body, got {other:?}"),
    }
}
