//! AMQP 1.0 messaging types (core spec §3): addressing ([`Source`]/[`Target`]),
//! delivery states/outcomes, the bare-message sections, and the assembled
//! [`Message`].

use bytes::{BufMut, Bytes, BytesMut};
use uuid::Uuid;

use crate::amqp_composite;
use crate::codec::described::{descriptors, expect_descriptor, peek_descriptor};
use crate::codec::encode::{close_compound, encode_descriptor, open_compound};
use crate::codec::{Decode, DecodeError, Descriptor, Encode, OrderedMap, Symbol, Value, codes};

use super::definitions::{Error, Fields, Milliseconds, SequenceNo};

/// A polymorphic annotations map (symbol-keyed; values are arbitrary).
pub type Annotations = OrderedMap<Symbol, Value>;

// ---------------------------------------------------------------------------
// Timestamp & message-id
// ---------------------------------------------------------------------------

/// An absolute time as milliseconds since the Unix epoch (`timestamp`, `0x83`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Timestamp(pub i64);

impl Encode for Timestamp {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::TIMESTAMP);
        buf.put_i64(self.0);
    }
}

impl Decode for Timestamp {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match crate::codec::decode::read_u8(buf)? {
            codes::TIMESTAMP => Ok(Timestamp(crate::codec::decode::read_u64(buf)? as i64)),
            c => Err(DecodeError::InvalidFormatCode {
                code: c,
                expected: "timestamp",
            }),
        }
    }
}

/// A message or correlation identifier (one of four primitive shapes).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MessageId {
    /// A `ulong` id.
    Ulong(u64),
    /// A `uuid` id.
    Uuid(Uuid),
    /// A `binary` id.
    Binary(Bytes),
    /// A `string` id.
    String(String),
}

impl Encode for MessageId {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            MessageId::Ulong(v) => v.encode(buf),
            MessageId::Uuid(v) => v.encode(buf),
            MessageId::Binary(v) => v.encode(buf),
            MessageId::String(v) => v.encode(buf),
        }
    }
}

impl Decode for MessageId {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match crate::codec::peek_code(buf) {
            Some(codes::ULONG_0 | codes::SMALL_ULONG | codes::ULONG) => {
                Ok(MessageId::Ulong(u64::decode(buf)?))
            }
            Some(codes::UUID) => Ok(MessageId::Uuid(Uuid::decode(buf)?)),
            Some(codes::VBIN8 | codes::VBIN32) => Ok(MessageId::Binary(Bytes::decode(buf)?)),
            Some(codes::STR8 | codes::STR32) => Ok(MessageId::String(String::decode(buf)?)),
            Some(c) => Err(DecodeError::InvalidFormatCode {
                code: c,
                expected: "message-id (ulong/uuid/binary/string)",
            }),
            None => Err(DecodeError::Eof { needed: 1 }),
        }
    }
}

// ---------------------------------------------------------------------------
// Delivery states & outcomes
// ---------------------------------------------------------------------------

amqp_composite! {
    /// The `received` delivery state (resume / partial-transfer position).
    pub struct Received : descriptors::RECEIVED => {
        section_number: u32 = req("section-number"),
        section_offset: u64 = req("section-offset"),
    }
}

amqp_composite! {
    /// The `accepted` terminal outcome.
    pub struct Accepted : descriptors::ACCEPTED => {}
}

amqp_composite! {
    /// The `released` terminal outcome.
    pub struct Released : descriptors::RELEASED => {}
}

amqp_composite! {
    /// The `rejected` terminal outcome, optionally carrying an error.
    pub struct Rejected : descriptors::REJECTED => {
        error: Option<Error> = opt(),
    }
}

amqp_composite! {
    /// The `modified` terminal outcome.
    pub struct Modified : descriptors::MODIFIED => {
        delivery_failed: Option<bool> = opt(),
        undeliverable_here: Option<bool> = opt(),
        message_annotations: Option<Fields> = opt(),
    }
}

/// A terminal delivery outcome (the subset of delivery-states a settled
/// delivery may reach).
#[derive(Debug, Clone, PartialEq)]
pub enum Outcome {
    /// Accepted by the receiver.
    Accepted(Accepted),
    /// Rejected with an optional error.
    Rejected(Rejected),
    /// Released back to the sender.
    Released(Released),
    /// Modified (with disposition hints).
    Modified(Modified),
}

impl Encode for Outcome {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Outcome::Accepted(v) => v.encode(buf),
            Outcome::Rejected(v) => v.encode(buf),
            Outcome::Released(v) => v.encode(buf),
            Outcome::Modified(v) => v.encode(buf),
        }
    }
}

impl Decode for Outcome {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match peek_descriptor(buf)? {
            Descriptor::Code(descriptors::ACCEPTED) => {
                Ok(Outcome::Accepted(Accepted::decode(buf)?))
            }
            Descriptor::Code(descriptors::REJECTED) => {
                Ok(Outcome::Rejected(Rejected::decode(buf)?))
            }
            Descriptor::Code(descriptors::RELEASED) => {
                Ok(Outcome::Released(Released::decode(buf)?))
            }
            Descriptor::Code(descriptors::MODIFIED) => {
                Ok(Outcome::Modified(Modified::decode(buf)?))
            }
            other => Err(DecodeError::InvalidValue(format!(
                "unexpected outcome descriptor {other}"
            ))),
        }
    }
}

/// The full delivery state (outcomes plus the non-terminal `received`).
#[derive(Debug, Clone, PartialEq)]
pub enum DeliveryState {
    /// Partial receipt / resume position.
    Received(Received),
    /// Accepted.
    Accepted(Accepted),
    /// Rejected.
    Rejected(Rejected),
    /// Released.
    Released(Released),
    /// Modified.
    Modified(Modified),
    /// Any other described state (e.g. a transactional state, or a future
    /// extension), preserved verbatim rather than rejected.
    Other(Value),
}

impl DeliveryState {
    /// Whether this state is terminal (a settled outcome).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, DeliveryState::Received(_))
    }
}

impl Encode for DeliveryState {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            DeliveryState::Received(v) => v.encode(buf),
            DeliveryState::Accepted(v) => v.encode(buf),
            DeliveryState::Rejected(v) => v.encode(buf),
            DeliveryState::Released(v) => v.encode(buf),
            DeliveryState::Modified(v) => v.encode(buf),
            DeliveryState::Other(v) => v.encode(buf),
        }
    }
}

impl Decode for DeliveryState {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match peek_descriptor(buf)? {
            Descriptor::Code(descriptors::RECEIVED) => {
                Ok(DeliveryState::Received(Received::decode(buf)?))
            }
            Descriptor::Code(descriptors::ACCEPTED) => {
                Ok(DeliveryState::Accepted(Accepted::decode(buf)?))
            }
            Descriptor::Code(descriptors::REJECTED) => {
                Ok(DeliveryState::Rejected(Rejected::decode(buf)?))
            }
            Descriptor::Code(descriptors::RELEASED) => {
                Ok(DeliveryState::Released(Released::decode(buf)?))
            }
            Descriptor::Code(descriptors::MODIFIED) => {
                Ok(DeliveryState::Modified(Modified::decode(buf)?))
            }
            // Unknown / transactional state: keep the raw described value.
            _ => Ok(DeliveryState::Other(Value::decode(buf)?)),
        }
    }
}

impl From<Outcome> for DeliveryState {
    fn from(o: Outcome) -> Self {
        match o {
            Outcome::Accepted(v) => DeliveryState::Accepted(v),
            Outcome::Rejected(v) => DeliveryState::Rejected(v),
            Outcome::Released(v) => DeliveryState::Released(v),
            Outcome::Modified(v) => DeliveryState::Modified(v),
        }
    }
}

// ---------------------------------------------------------------------------
// Addressing: source / target / coordinator
// ---------------------------------------------------------------------------

amqp_composite! {
    /// A message `source` (the node a receiver reads from).
    pub struct Source : descriptors::SOURCE => {
        address: Option<String> = opt(),
        durable: u32 = default(0),
        expiry_policy: Symbol = default(Symbol::new("session-end")),
        timeout: u32 = default(0),
        dynamic: bool = default(false),
        dynamic_node_properties: Option<Fields> = opt(),
        distribution_mode: Option<Symbol> = opt(),
        filter: Option<Fields> = opt(),
        default_outcome: Option<Outcome> = opt(),
        outcomes: Vec<Symbol> = symbols(),
        capabilities: Vec<Symbol> = symbols(),
    }
}

amqp_composite! {
    /// A message `target` (the node a sender writes to).
    pub struct Target : descriptors::TARGET => {
        address: Option<String> = opt(),
        durable: u32 = default(0),
        expiry_policy: Symbol = default(Symbol::new("session-end")),
        timeout: u32 = default(0),
        dynamic: bool = default(false),
        dynamic_node_properties: Option<Fields> = opt(),
        capabilities: Vec<Symbol> = symbols(),
    }
}

amqp_composite! {
    /// A transaction `coordinator` target.
    pub struct Coordinator : descriptors::COORDINATOR => {
        capabilities: Vec<Symbol> = symbols(),
    }
}

impl Source {
    /// A simple source addressing `address`.
    pub fn new(address: impl Into<String>) -> Self {
        Source {
            address: Some(address.into()),
            expiry_policy: Symbol::new("session-end"),
            ..Default::default()
        }
    }
}

impl Target {
    /// A simple target addressing `address`.
    pub fn new(address: impl Into<String>) -> Self {
        Target {
            address: Some(address.into()),
            expiry_policy: Symbol::new("session-end"),
            ..Default::default()
        }
    }
}

/// The `target` field of an attach: an ordinary [`Target`] or, for transaction
/// control links, a [`Coordinator`].
#[derive(Debug, Clone, PartialEq)]
pub enum TargetArchetype {
    /// A normal message target.
    Target(Box<Target>),
    /// A transaction coordinator.
    Coordinator(Coordinator),
}

impl Encode for TargetArchetype {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            TargetArchetype::Target(t) => t.encode(buf),
            TargetArchetype::Coordinator(c) => c.encode(buf),
        }
    }
}

impl Decode for TargetArchetype {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match peek_descriptor(buf)? {
            Descriptor::Code(descriptors::TARGET) => {
                Ok(TargetArchetype::Target(Box::new(Target::decode(buf)?)))
            }
            Descriptor::Code(descriptors::COORDINATOR) => {
                Ok(TargetArchetype::Coordinator(Coordinator::decode(buf)?))
            }
            other => Err(DecodeError::InvalidValue(format!(
                "unexpected target archetype descriptor {other}"
            ))),
        }
    }
}

impl From<Target> for TargetArchetype {
    fn from(t: Target) -> Self {
        TargetArchetype::Target(Box::new(t))
    }
}

// ---------------------------------------------------------------------------
// Bare-message sections
// ---------------------------------------------------------------------------

amqp_composite! {
    /// The standard message `header` section.
    pub struct Header : descriptors::HEADER => {
        durable: bool = default(false),
        priority: u8 = default(4),
        ttl: Option<Milliseconds> = opt(),
        first_acquirer: bool = default(false),
        delivery_count: u32 = default(0),
    }
}

amqp_composite! {
    /// The standard message `properties` section.
    pub struct Properties : descriptors::PROPERTIES => {
        message_id: Option<MessageId> = opt(),
        user_id: Option<Bytes> = opt(),
        to: Option<String> = opt(),
        subject: Option<String> = opt(),
        reply_to: Option<String> = opt(),
        correlation_id: Option<MessageId> = opt(),
        content_type: Option<Symbol> = opt(),
        content_encoding: Option<Symbol> = opt(),
        absolute_expiry_time: Option<Timestamp> = opt(),
        creation_time: Option<Timestamp> = opt(),
        group_id: Option<String> = opt(),
        group_sequence: Option<SequenceNo> = opt(),
        reply_to_group_id: Option<String> = opt(),
    }
}

macro_rules! described_map_section {
    ($(#[$m:meta])* $name:ident, $desc:expr, $kty:ty) => {
        $(#[$m])*
        #[derive(Debug, Clone, PartialEq, Default)]
        pub struct $name(pub OrderedMap<$kty, Value>);

        impl Encode for $name {
            fn encode(&self, buf: &mut BytesMut) {
                buf.put_u8(codes::DESCRIBED);
                encode_descriptor(buf, $desc);
                self.0.encode(buf);
            }
        }

        impl Decode for $name {
            fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
                expect_descriptor(buf, $desc)?;
                let map = Option::<OrderedMap<$kty, Value>>::decode(buf)?.unwrap_or_default();
                Ok($name(map))
            }
        }
    };
}

described_map_section!(
    /// `delivery-annotations` (transport-scoped, symbol-keyed).
    DeliveryAnnotations,
    descriptors::DELIVERY_ANNOTATIONS,
    Symbol
);
described_map_section!(
    /// `message-annotations` (message-scoped, symbol-keyed).
    MessageAnnotations,
    descriptors::MESSAGE_ANNOTATIONS,
    Symbol
);
described_map_section!(
    /// `footer` (message-scoped, symbol-keyed).
    Footer,
    descriptors::FOOTER,
    Symbol
);
described_map_section!(
    /// `application-properties` (string-keyed).
    ApplicationProperties,
    descriptors::APPLICATION_PROPERTIES,
    String
);

/// A `data` section: an opaque binary body part.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Data(pub Bytes);

impl Encode for Data {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::DESCRIBED);
        encode_descriptor(buf, descriptors::DATA);
        self.0.encode(buf);
    }
}

impl Decode for Data {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        expect_descriptor(buf, descriptors::DATA)?;
        Ok(Data(Option::<Bytes>::decode(buf)?.unwrap_or_default()))
    }
}

/// An `amqp-value` section: a single typed value body.
#[derive(Debug, Clone, PartialEq)]
pub struct AmqpValue(pub Value);

impl Encode for AmqpValue {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::DESCRIBED);
        encode_descriptor(buf, descriptors::AMQP_VALUE);
        self.0.encode(buf);
    }
}

impl Decode for AmqpValue {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        expect_descriptor(buf, descriptors::AMQP_VALUE)?;
        Ok(AmqpValue(Value::decode(buf)?))
    }
}

/// An `amqp-sequence` section: a list of typed values.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct AmqpSequence(pub Vec<Value>);

impl Encode for AmqpSequence {
    fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(codes::DESCRIBED);
        encode_descriptor(buf, descriptors::AMQP_SEQUENCE);
        let (s, c, start) = open_compound(buf, codes::LIST32);
        for v in &self.0 {
            v.encode(buf);
        }
        close_compound(buf, s, c, start, self.0.len() as u32);
    }
}

impl Decode for AmqpSequence {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        expect_descriptor(buf, descriptors::AMQP_SEQUENCE)?;
        match Value::decode(buf)? {
            Value::List(items) => Ok(AmqpSequence(items)),
            other => Err(DecodeError::InvalidValue(format!(
                "amqp-sequence body must be a list, got {other:?}"
            ))),
        }
    }
}

// ---------------------------------------------------------------------------
// Assembled message
// ---------------------------------------------------------------------------

/// The body of a message: one or more `data` parts, one or more
/// `amqp-sequence` parts, a single `amqp-value`, or empty.
#[derive(Debug, Clone, PartialEq, Default)]
pub enum Body {
    /// One or more binary `data` sections.
    Data(Vec<Bytes>),
    /// One or more `amqp-sequence` sections.
    Sequence(Vec<Vec<Value>>),
    /// A single `amqp-value` section.
    Value(Value),
    /// No body.
    #[default]
    Empty,
}

/// A complete AMQP message: optional annotation/header/property sections, a
/// body, and an optional footer.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Message {
    /// The transport `header`.
    pub header: Option<Header>,
    /// Transport-scoped `delivery-annotations`.
    pub delivery_annotations: Option<DeliveryAnnotations>,
    /// Message-scoped `message-annotations`.
    pub message_annotations: Option<MessageAnnotations>,
    /// Standard `properties`.
    pub properties: Option<Properties>,
    /// `application-properties`.
    pub application_properties: Option<ApplicationProperties>,
    /// The message body.
    pub body: Body,
    /// Message `footer`.
    pub footer: Option<Footer>,
}

impl Message {
    /// A message with a single binary `data` body.
    pub fn data(bytes: impl Into<Bytes>) -> Self {
        Message {
            body: Body::Data(vec![bytes.into()]),
            ..Default::default()
        }
    }

    /// A message with an `amqp-value` body.
    pub fn value(value: Value) -> Self {
        Message {
            body: Body::Value(value),
            ..Default::default()
        }
    }

    /// A message with a UTF-8 string `amqp-value` body.
    pub fn text(s: impl Into<String>) -> Self {
        Message::value(Value::String(s.into()))
    }
}

impl Encode for Message {
    fn encode(&self, buf: &mut BytesMut) {
        if let Some(h) = &self.header {
            h.encode(buf);
        }
        if let Some(da) = &self.delivery_annotations {
            da.encode(buf);
        }
        if let Some(ma) = &self.message_annotations {
            ma.encode(buf);
        }
        if let Some(p) = &self.properties {
            p.encode(buf);
        }
        if let Some(ap) = &self.application_properties {
            ap.encode(buf);
        }
        match &self.body {
            Body::Data(parts) => {
                for part in parts {
                    Data(part.clone()).encode(buf);
                }
            }
            Body::Sequence(seqs) => {
                for seq in seqs {
                    AmqpSequence(seq.clone()).encode(buf);
                }
            }
            Body::Value(v) => AmqpValue(v.clone()).encode(buf),
            Body::Empty => {}
        }
        if let Some(f) = &self.footer {
            f.encode(buf);
        }
    }
}

impl Decode for Message {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        let mut msg = Message::default();
        let mut data_parts: Vec<Bytes> = Vec::new();
        let mut seq_parts: Vec<Vec<Value>> = Vec::new();

        while !buf.is_empty() {
            let Ok(desc) = peek_descriptor(buf) else {
                break;
            };
            match desc {
                Descriptor::Code(descriptors::HEADER) => msg.header = Some(Header::decode(buf)?),
                Descriptor::Code(descriptors::DELIVERY_ANNOTATIONS) => {
                    msg.delivery_annotations = Some(DeliveryAnnotations::decode(buf)?)
                }
                Descriptor::Code(descriptors::MESSAGE_ANNOTATIONS) => {
                    msg.message_annotations = Some(MessageAnnotations::decode(buf)?)
                }
                Descriptor::Code(descriptors::PROPERTIES) => {
                    msg.properties = Some(Properties::decode(buf)?)
                }
                Descriptor::Code(descriptors::APPLICATION_PROPERTIES) => {
                    msg.application_properties = Some(ApplicationProperties::decode(buf)?)
                }
                Descriptor::Code(descriptors::DATA) => data_parts.push(Data::decode(buf)?.0),
                Descriptor::Code(descriptors::AMQP_SEQUENCE) => {
                    seq_parts.push(AmqpSequence::decode(buf)?.0)
                }
                Descriptor::Code(descriptors::AMQP_VALUE) => {
                    msg.body = Body::Value(AmqpValue::decode(buf)?.0)
                }
                Descriptor::Code(descriptors::FOOTER) => msg.footer = Some(Footer::decode(buf)?),
                // Unknown described section: stop parsing the message.
                _ => break,
            }
        }

        if !data_parts.is_empty() {
            msg.body = Body::Data(data_parts);
        } else if !seq_parts.is_empty() {
            msg.body = Body::Sequence(seq_parts);
        }
        Ok(msg)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_slice, to_vec};

    fn rt<T: Encode + Decode + PartialEq + std::fmt::Debug>(v: T) {
        let bytes = to_vec(&v);
        let back: T = from_slice(&bytes).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn outcomes_round_trip() {
        rt(Outcome::Accepted(Accepted::default()));
        rt(Outcome::Released(Released::default()));
        rt(Outcome::Rejected(Rejected {
            error: Some(Error::new(
                crate::types::definitions::AmqpError::NotFound,
                Some("nope".into()),
            )),
        }));
        rt(Outcome::Modified(Modified {
            delivery_failed: Some(true),
            undeliverable_here: Some(false),
            message_annotations: None,
        }));
    }

    #[test]
    fn delivery_states_round_trip() {
        rt(DeliveryState::Received(Received {
            section_number: 0,
            section_offset: 1024,
        }));
        rt(DeliveryState::Accepted(Accepted::default()));
        assert!(DeliveryState::Accepted(Accepted::default()).is_terminal());
        assert!(!DeliveryState::Received(Received::default()).is_terminal());
    }

    #[test]
    fn source_target_round_trip() {
        rt(Source::new("queue://in"));
        rt(Target::new("queue://out"));
        let mut s = Source::new("topic");
        s.capabilities = vec![Symbol::new("topic"), Symbol::new("shared")];
        s.distribution_mode = Some(Symbol::new("copy"));
        rt(s);
        rt(TargetArchetype::from(Target::new("q")));
        rt(TargetArchetype::Coordinator(Coordinator {
            capabilities: vec![Symbol::new("amqp:local-transactions")],
        }));
    }

    #[test]
    fn properties_round_trip() {
        let p = Properties {
            message_id: Some(MessageId::Ulong(42)),
            correlation_id: Some(MessageId::String("corr".into())),
            content_type: Some(Symbol::new("application/json")),
            creation_time: Some(Timestamp(1_700_000_000_000)),
            group_sequence: Some(7),
            to: Some("queue://out".into()),
            ..Default::default()
        };
        rt(p);
    }

    #[test]
    fn message_bodies_round_trip() {
        rt(Message::data(Bytes::from_static(b"hello world")));
        rt(Message::text("a string body"));
        rt(Message::value(Value::Uint(1234)));

        let mut m = Message::data(Bytes::from_static(b"payload"));
        m.properties = Some(Properties {
            message_id: Some(MessageId::Uuid(Uuid::from_u128(1))),
            ..Default::default()
        });
        m.application_properties = Some(ApplicationProperties(OrderedMap::from(vec![(
            "key".to_string(),
            Value::Uint(9),
        )])));
        m.header = Some(Header {
            durable: true,
            ..Default::default()
        });
        rt(m);

        // multi-part data body
        rt(Message {
            body: Body::Data(vec![Bytes::from_static(b"a"), Bytes::from_static(b"bc")]),
            ..Default::default()
        });
    }
}
