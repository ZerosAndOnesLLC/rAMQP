//! The message `header.delivery-count` on redelivery (AMQP 1.0 §3.2.1,
//! §3.4.5): the number of unsuccessful previous delivery attempts, which a
//! `modified{delivery-failed: true}` settlement increments. Queues count
//! those failures (for `max_delivery_attempts`); this writes the count into
//! the message they redeliver.

use bytes::{Bytes, BytesMut};
use ramqp_core::codec::{Decode, Descriptor, Encode, descriptors, peek_descriptor};
use ramqp_core::types::messaging::Header;

/// `body` (an encoded bare message, as stored — never rewritten) with its
/// header's `delivery-count` raised by `failures` (keeping the count the
/// publisher sent) and `first-acquirer` cleared. Only the header section is
/// rewritten — every other section passes through byte for byte — and a
/// message without a header gets one. `failures == 0` returns `body`
/// unchanged, so a first delivery costs nothing. Bytes that do not start
/// with a described section, or an unparseable header, are left alone.
pub(crate) fn with_delivery_count(body: &Bytes, failures: u32) -> Bytes {
    if failures == 0 {
        return body.clone();
    }
    let (mut header, rest) = match peek_descriptor(body) {
        Ok(Descriptor::Code(descriptors::HEADER)) => {
            let mut cursor = body.clone();
            match Header::decode(&mut cursor) {
                Ok(h) => (h, cursor),
                Err(_) => return body.clone(),
            }
        }
        // Another section first: the message has no header yet.
        Ok(_) => (Header::default(), body.clone()),
        // Not a described section at all: not a message we can annotate.
        Err(_) => return body.clone(),
    };
    // The count the publisher sent (the stored copy keeps it) plus the
    // failures here; a redelivered message was acquired before.
    header.delivery_count = header.delivery_count.saturating_add(failures);
    header.first_acquirer = false;
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
    fn keeps_the_publishers_count_and_clears_first_acquirer() {
        let mut m = Message::text("b");
        m.header = Some(Header {
            delivery_count: 3,
            first_acquirer: true,
            ..Header::default()
        });
        let raw = encode(&m);
        let h = Message::decode(&mut with_delivery_count(&raw, 2))
            .unwrap()
            .header
            .unwrap();
        assert_eq!((h.delivery_count, h.first_acquirer), (5, false));
        // Not a message: left alone.
        let junk = Bytes::from_static(b"\x45not-amqp");
        assert_eq!(with_delivery_count(&junk, 1), junk);
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
        // The stored (original) message is what gets rewritten each time.
        assert_eq!(with_delivery_count(&raw, 3), out);
    }
}
