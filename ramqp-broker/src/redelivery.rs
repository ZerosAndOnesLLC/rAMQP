//! The message `header.delivery-count` on redelivery (AMQP 1.0 §3.2.1,
//! §3.4.5): the number of unsuccessful previous delivery attempts, which a
//! `modified{delivery-failed: true}` settlement increments. Queues count
//! those failures (for `max_delivery_attempts`); this writes the count into
//! the message they redeliver.

use bytes::{Bytes, BytesMut};
use ramqp_core::codec::{Decode, Descriptor, Encode, descriptors, peek_descriptor};
use ramqp_core::types::messaging::Header;

/// `body` (an encoded bare message) with its header's `delivery-count` set
/// to `failures`. Only the header section is rewritten — every other
/// section is passed through byte for byte — and a message without a header
/// gets one (default fields). `failures == 0` returns `body` unchanged, so
/// a first delivery costs nothing. An unparseable header is left alone.
pub(crate) fn with_delivery_count(body: &Bytes, failures: u32) -> Bytes {
    if failures == 0 {
        return body.clone();
    }
    let has_header = matches!(
        peek_descriptor(body),
        Ok(Descriptor::Code(descriptors::HEADER))
    );
    let (mut header, rest) = if has_header {
        let mut cursor = body.clone();
        match Header::decode(&mut cursor) {
            Ok(h) => (h, cursor),
            Err(_) => return body.clone(),
        }
    } else {
        (Header::default(), body.clone())
    };
    if header.delivery_count == failures {
        return body.clone();
    }
    header.delivery_count = failures;
    let mut out = BytesMut::with_capacity(body.len() + 16);
    header.encode(&mut out);
    out.extend_from_slice(&rest);
    out.freeze()
}

#[cfg(test)]
mod tests {
    use super::with_delivery_count;
    use bytes::{Bytes, BytesMut};
    use ramqp_core::codec::{Decode, Encode, Value};
    use ramqp_core::types::messaging::{Body, Header, Message, Properties};

    fn encode(m: &Message) -> Bytes {
        let mut b = BytesMut::new();
        m.encode(&mut b);
        b.freeze()
    }

    #[test]
    fn first_delivery_is_untouched() {
        let raw = encode(&Message::text("x"));
        assert_eq!(with_delivery_count(&raw, 0), raw);
    }

    #[test]
    fn adds_a_header_when_there_is_none() {
        let mut m = Message::text("body");
        m.properties = Some(Properties {
            subject: Some("s".into()),
            ..Default::default()
        });
        let raw = encode(&m);
        let out = with_delivery_count(&raw, 2);
        let back = Message::decode(&mut out.clone()).unwrap();
        assert_eq!(back.header.unwrap().delivery_count, 2);
        assert_eq!(back.properties.unwrap().subject.as_deref(), Some("s"));
        assert!(matches!(back.body, Body::Value(Value::String(ref t)) if t == "body"));
        // Everything after the new header is the original bytes.
        assert!(out.ends_with(&raw));
    }

    #[test]
    fn rewrites_an_existing_header_and_keeps_its_fields() {
        let mut m = Message::text("body");
        m.header = Some(Header {
            durable: true,
            priority: 7,
            ttl: Some(5_000),
            first_acquirer: false,
            delivery_count: 0,
        });
        let raw = encode(&m);
        let out = with_delivery_count(&raw, 3);
        let h = Message::decode(&mut out.clone()).unwrap().header.unwrap();
        assert_eq!(
            (h.durable, h.priority, h.ttl, h.delivery_count),
            (true, 7, Some(5_000), 3)
        );
        // Idempotent: rewriting to the same count is a no-op.
        assert_eq!(with_delivery_count(&out, 3), out);
    }
}
