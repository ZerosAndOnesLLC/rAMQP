//! The message `header` as a delivery sees it (AMQP 1.0 §3.2.1):
//! - `delivery-count`: unsuccessful previous delivery attempts — a
//!   `modified{delivery-failed: true}`, or a delivery lost with its link or
//!   connection. Queues count those failures (for `max_delivery_attempts`);
//!   this writes the count into the message they redeliver.
//! - `first-acquirer`: whether no link has acquired the message before.
//! - `ttl`: the message's own lifetime from arrival at the broker, which
//!   queues honour when they would dispatch it.

use bytes::{Bytes, BytesMut};
use ramqp_core::codec::{Decode, Descriptor, Encode, descriptors, peek_descriptor};
use ramqp_core::types::messaging::Header;

/// The message as it goes out on this delivery: `header.delivery-count`
/// raised by `failures` (failed attempts so far, added to any count the
/// publisher set) and `header.first-acquirer` set to `first` (no link has
/// acquired it before; AMQP 1.0 §3.2.1). Only the header section is
/// rewritten — the others pass through byte for byte — and only when one of
/// those fields actually changes; otherwise the same bytes go out (no copy).
pub(crate) fn for_delivery(body: &Bytes, failures: u32, first: bool) -> Bytes {
    let first = first && failures == 0;
    let (header, rest) = match peek_descriptor(body) {
        Ok(Descriptor::Code(descriptors::HEADER)) => {
            let mut cursor = body.clone();
            match Header::decode(&mut cursor) {
                Ok(h) => (Some(h), cursor),
                Err(_) => return body.clone(),
            }
        }
        Ok(_) => (None, body.clone()),
        Err(_) => return body.clone(),
    };
    let had_first = header.as_ref().is_some_and(|h| h.first_acquirer);
    if failures == 0 && had_first == first {
        return body.clone();
    }
    let mut header = header.unwrap_or_default();
    header.delivery_count = header.delivery_count.saturating_add(failures);
    header.first_acquirer = first;
    let mut out = BytesMut::with_capacity(body.len() + 16);
    header.encode(&mut out);
    out.extend_from_slice(&rest);
    out.freeze()
}

/// The message's `header.ttl` in milliseconds, if it has one (AMQP 1.0
/// §3.2.1: its lifetime from arrival at this broker).
pub(crate) fn ttl(body: &Bytes) -> Option<u64> {
    match peek_descriptor(body) {
        Ok(Descriptor::Code(descriptors::HEADER)) => {
            Header::decode(&mut body.clone()).ok()?.ttl.map(u64::from)
        }
        _ => None,
    }
}

/// `body` without its `header.ttl`, for a dead letter: as in RabbitMQ, a
/// dead-lettered message no longer expires (it would otherwise expire again
/// in the dead-letter queue, possibly at once). Unchanged — no copy — when
/// it has no ttl.
pub(crate) fn without_ttl(body: Bytes) -> Bytes {
    if !matches!(
        peek_descriptor(&body),
        Ok(Descriptor::Code(descriptors::HEADER))
    ) {
        return body;
    }
    let mut rest = body.clone();
    let Ok(mut header) = Header::decode(&mut rest) else {
        return body;
    };
    if header.ttl.is_none() {
        return body;
    }
    header.ttl = None;
    let mut out = BytesMut::with_capacity(body.len());
    header.encode(&mut out);
    out.extend_from_slice(&rest);
    out.freeze()
}

/// Whether a message enqueued at `enqueued_ms` has outlived its own
/// `header.ttl` at `now_ms`.
pub(crate) fn expired(body: &Bytes, enqueued_ms: u64, now_ms: u64) -> bool {
    ttl(body).is_some_and(|ttl| now_ms.saturating_sub(enqueued_ms) >= ttl)
}

#[cfg(test)]
mod tests {
    use super::{expired, for_delivery, ttl, without_ttl};
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
        let h = Message::decode(&mut for_delivery(&raw, 2, false))
            .unwrap()
            .header
            .unwrap();
        assert_eq!((h.delivery_count, h.first_acquirer), (5, false));
        // Not a message: left alone.
        let junk = Bytes::from_static(b"\x45not-amqp");
        assert_eq!(for_delivery(&junk, 1, false), junk);
    }

    #[test]
    fn nothing_to_change_means_the_same_bytes() {
        let raw = encode(&Message::text("x"));
        assert_eq!(for_delivery(&raw, 0, false), raw);
    }

    #[test]
    fn first_acquirer_is_set_on_a_first_delivery_only() {
        let raw = encode(&Message::text("x"));
        let first = Message::decode(&mut for_delivery(&raw, 0, true)).unwrap();
        assert!(first.header.unwrap().first_acquirer);
        // Acquired before (released, or a failed attempt): false.
        let mut m = Message::text("x");
        m.header = Some(Header {
            first_acquirer: true,
            ..Header::default()
        });
        let raw = encode(&m);
        let again = Message::decode(&mut for_delivery(&raw, 0, false)).unwrap();
        assert!(!again.header.unwrap().first_acquirer);
        let failed = Message::decode(&mut for_delivery(&raw, 1, true)).unwrap();
        assert!(
            !failed.header.unwrap().first_acquirer,
            "a failed attempt was an acquisition"
        );
    }

    #[test]
    fn ttl_counts_from_arrival() {
        let mut m = Message::text("x");
        m.header = Some(Header {
            ttl: Some(200),
            ..Header::default()
        });
        let raw = encode(&m);
        assert_eq!(ttl(&raw), Some(200));
        assert!(!expired(&raw, 1_000, 1_199));
        assert!(expired(&raw, 1_000, 1_200));
        let plain = encode(&Message::text("x"));
        assert_eq!(ttl(&plain), None);
        assert!(!expired(&plain, 0, u64::MAX));
        // A dead letter loses its ttl; nothing else changes.
        let dead = without_ttl(raw);
        assert_eq!(ttl(&dead), None);
        assert!(
            matches!(Message::decode(&mut dead.clone()).unwrap().body, Body::Value(Value::String(ref t)) if t == "x")
        );
        assert_eq!(without_ttl(plain.clone()), plain);
    }

    #[test]
    fn adds_a_header_when_there_is_none() {
        let mut m = Message::text("body");
        m.properties = Some(Properties {
            subject: Some("s".into()),
            ..Default::default()
        });
        let raw = encode(&m);
        let out = for_delivery(&raw, 2, false);
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
        let out = for_delivery(&raw, 3, false);
        let h = Message::decode(&mut out.clone()).unwrap().header.unwrap();
        assert_eq!(
            (h.durable, h.priority, h.ttl, h.delivery_count),
            (true, 7, Some(5_000), 3)
        );
        // The stored (original) message is what gets rewritten each time.
        assert_eq!(for_delivery(&raw, 3, false), out);
    }
}
