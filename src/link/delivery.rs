//! Delivery assembly and the lazy, zero-copy [`Delivery`] (WP-4.5, decision D-3).
//!
//! Inbound transfers are assembled into one [`Delivery`] holding the body as
//! [`Bytes`]; typed deserialization is an explicit, cheap opt-in via
//! [`Delivery::decode`] / [`Delivery::message`].

use bytes::{Bytes, BytesMut};

use crate::codec::Decode;
use crate::error::RecvError;
use crate::ids::DeliveryId;
use crate::types::messaging::{DeliveryState, Message};

/// A received delivery. The body is exposed as raw [`Bytes`] by default; typed
/// access is opt-in (the inversion of `fe2o3-amqp`'s eager-deserialize default).
#[derive(Debug, Clone)]
pub struct Delivery {
    /// The peer-assigned delivery id.
    pub delivery_id: DeliveryId,
    /// The peer-chosen delivery tag.
    pub delivery_tag: Bytes,
    /// Whether the peer pre-settled the delivery.
    pub settled: bool,
    state: Option<DeliveryState>,
    body: Bytes,
}

impl Delivery {
    /// Construct a delivery from assembled parts.
    pub fn new(delivery_id: DeliveryId, delivery_tag: Bytes, settled: bool, body: Bytes) -> Self {
        Delivery {
            delivery_id,
            delivery_tag,
            settled,
            state: None,
            body,
        }
    }

    /// Attach the sender-declared delivery state (from the transfer's `state`).
    pub fn with_state(mut self, state: Option<DeliveryState>) -> Self {
        self.state = state;
        self
    }

    /// The delivery state the sender declared on the transfer, if any.
    pub fn state(&self) -> Option<&DeliveryState> {
        self.state.as_ref()
    }

    /// The raw (zero-copy) message bytes.
    pub fn raw(&self) -> &Bytes {
        &self.body
    }

    /// Consume into the raw message bytes.
    pub fn into_raw(self) -> Bytes {
        self.body
    }

    /// Decode the body as a full AMQP [`Message`] (sections + body).
    pub fn message(&self) -> Result<Message, RecvError> {
        let mut buf = self.body.clone();
        Message::decode(&mut buf).map_err(RecvError::from)
    }

    /// Decode the body as a single typed value `T` (e.g. the `amqp-value` body
    /// after stripping sections, when the caller knows the schema).
    pub fn decode<T: Decode>(&self) -> Result<T, RecvError> {
        let mut buf = self.body.clone();
        T::decode(&mut buf).map_err(RecvError::from)
    }
}

/// Accumulates a (possibly multi-frame) delivery as continuation transfers
/// arrive (`more = true` until the final frame).
#[derive(Debug)]
pub struct PartialDelivery {
    delivery_id: DeliveryId,
    delivery_tag: Bytes,
    settled: bool,
    state: Option<DeliveryState>,
    buf: BytesMut,
}

impl PartialDelivery {
    /// Begin a delivery from its first transfer's metadata and payload. The
    /// sender-declared `state` (the transfer's `state` field, present only on the
    /// first frame) is carried through to the completed [`Delivery`].
    pub fn new(
        delivery_id: DeliveryId,
        delivery_tag: Bytes,
        settled: bool,
        state: Option<DeliveryState>,
        first: &[u8],
    ) -> Self {
        let mut buf = BytesMut::with_capacity(first.len());
        buf.extend_from_slice(first);
        PartialDelivery {
            delivery_id,
            delivery_tag,
            settled,
            state,
            buf,
        }
    }

    /// The delivery id being assembled.
    pub fn delivery_id(&self) -> DeliveryId {
        self.delivery_id
    }

    /// Bytes accumulated so far (for the message-size cap).
    pub fn len(&self) -> usize {
        self.buf.len()
    }

    /// Whether nothing has been accumulated yet.
    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    /// Append a continuation frame's payload.
    pub fn append(&mut self, payload: &[u8]) {
        self.buf.extend_from_slice(payload);
    }

    /// Finish assembly into a [`Delivery`].
    pub fn complete(self) -> Delivery {
        Delivery::new(
            self.delivery_id,
            self.delivery_tag,
            self.settled,
            self.buf.freeze(),
        )
        .with_state(self.state)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::to_vec;
    use crate::types::messaging::Message;

    #[test]
    fn lazy_body_and_typed_decode() {
        let msg = Message::text("hello");
        let bytes = Bytes::from(to_vec(&msg));
        let d = Delivery::new(
            DeliveryId(1),
            Bytes::from_static(b"tag"),
            false,
            bytes.clone(),
        );
        assert_eq!(d.raw(), &bytes);
        assert_eq!(d.message().unwrap(), msg);
    }

    #[test]
    fn multi_frame_assembly() {
        let msg = Message::data(Bytes::from_static(b"abcdefgh"));
        let full = to_vec(&msg);
        let (a, b) = full.split_at(full.len() / 2);
        let mut partial =
            PartialDelivery::new(DeliveryId(2), Bytes::from_static(b"t"), false, None, a);
        partial.append(b);
        let delivery = partial.complete();
        assert_eq!(delivery.message().unwrap(), msg);
    }
}
