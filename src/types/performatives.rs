//! The nine AMQP 1.0 transport performatives (core spec §2.7) and a
//! [`Performative`] dispatch enum for decoding inbound frame bodies.

use bytes::{Bytes, BytesMut};

use crate::amqp_composite;
use crate::codec::described::{descriptors, peek_descriptor};
use crate::codec::{Decode, DecodeError, Descriptor, Encode, OrderedMap, Symbol};

use super::definitions::{
    DeliveryNumber, DeliveryTag, Error, Fields, Handle, MessageFormat, Milliseconds,
    ReceiverSettleMode, Role, SenderSettleMode, SequenceNo, TransferNumber,
};
use super::messaging::{DeliveryState, Source, TargetArchetype};

/// The `unsettled` map carried by `attach`: delivery-tag → delivery-state.
pub type Unsettled = OrderedMap<DeliveryTag, DeliveryState>;

amqp_composite! {
    /// `open` (`0x10`): begins a connection and negotiates connection-wide
    /// parameters.
    pub struct Open : descriptors::OPEN => {
        container_id: String = req("container-id"),
        hostname: Option<String> = opt(),
        max_frame_size: u32 = default(u32::MAX),
        channel_max: u16 = default(u16::MAX),
        idle_time_out: Option<Milliseconds> = opt(),
        outgoing_locales: Vec<Symbol> = symbols(),
        incoming_locales: Vec<Symbol> = symbols(),
        offered_capabilities: Vec<Symbol> = symbols(),
        desired_capabilities: Vec<Symbol> = symbols(),
        properties: Option<Fields> = opt(),
    }
}

impl Open {
    /// A minimal `open` for the given container id (spec-default parameters).
    pub fn new(container_id: impl Into<String>) -> Self {
        Open {
            container_id: container_id.into(),
            max_frame_size: u32::MAX,
            channel_max: u16::MAX,
            ..Default::default()
        }
    }
}

amqp_composite! {
    /// `begin` (`0x11`): begins a session on a connection channel.
    pub struct Begin : descriptors::BEGIN => {
        remote_channel: Option<u16> = opt(),
        next_outgoing_id: TransferNumber = req("next-outgoing-id"),
        incoming_window: u32 = req("incoming-window"),
        outgoing_window: u32 = req("outgoing-window"),
        handle_max: Handle = default(u32::MAX),
        offered_capabilities: Vec<Symbol> = symbols(),
        desired_capabilities: Vec<Symbol> = symbols(),
        properties: Option<Fields> = opt(),
    }
}

amqp_composite! {
    /// `attach` (`0x12`): attaches a link to a session.
    pub struct Attach : descriptors::ATTACH => {
        name: String = req("name"),
        handle: Handle = req("handle"),
        role: Role = req("role"),
        snd_settle_mode: SenderSettleMode = default(SenderSettleMode::Mixed),
        rcv_settle_mode: ReceiverSettleMode = default(ReceiverSettleMode::First),
        source: Option<Source> = opt(),
        target: Option<TargetArchetype> = opt(),
        unsettled: Option<Unsettled> = opt(),
        incomplete_unsettled: bool = default(false),
        initial_delivery_count: Option<SequenceNo> = opt(),
        max_message_size: Option<u64> = opt(),
        offered_capabilities: Vec<Symbol> = symbols(),
        desired_capabilities: Vec<Symbol> = symbols(),
        properties: Option<Fields> = opt(),
    }
}

amqp_composite! {
    /// `flow` (`0x13`): session- and link-level flow control.
    pub struct Flow : descriptors::FLOW => {
        next_incoming_id: Option<TransferNumber> = opt(),
        incoming_window: u32 = req("incoming-window"),
        next_outgoing_id: TransferNumber = req("next-outgoing-id"),
        outgoing_window: u32 = req("outgoing-window"),
        handle: Option<Handle> = opt(),
        delivery_count: Option<SequenceNo> = opt(),
        link_credit: Option<u32> = opt(),
        available: Option<u32> = opt(),
        drain: bool = default(false),
        echo: bool = default(false),
        properties: Option<Fields> = opt(),
    }
}

amqp_composite! {
    /// `transfer` (`0x14`): transfers a (possibly multi-frame) message.
    pub struct Transfer : descriptors::TRANSFER => {
        handle: Handle = req("handle"),
        delivery_id: Option<DeliveryNumber> = opt(),
        delivery_tag: Option<DeliveryTag> = opt(),
        message_format: Option<MessageFormat> = opt(),
        settled: Option<bool> = opt(),
        more: bool = default(false),
        rcv_settle_mode: Option<ReceiverSettleMode> = opt(),
        state: Option<DeliveryState> = opt(),
        resume: bool = default(false),
        aborted: bool = default(false),
        batchable: bool = default(false),
    }
}

amqp_composite! {
    /// `disposition` (`0x15`): changes the state of in-flight deliveries.
    pub struct Disposition : descriptors::DISPOSITION => {
        role: Role = req("role"),
        first: DeliveryNumber = req("first"),
        last: Option<DeliveryNumber> = opt(),
        settled: bool = default(false),
        state: Option<DeliveryState> = opt(),
        batchable: bool = default(false),
    }
}

amqp_composite! {
    /// `detach` (`0x16`): detaches a link, optionally closing it.
    pub struct Detach : descriptors::DETACH => {
        handle: Handle = req("handle"),
        closed: bool = default(false),
        error: Option<Error> = opt(),
    }
}

amqp_composite! {
    /// `end` (`0x17`): ends a session.
    pub struct End : descriptors::END => {
        error: Option<Error> = opt(),
    }
}

amqp_composite! {
    /// `close` (`0x18`): closes a connection.
    pub struct Close : descriptors::CLOSE => {
        error: Option<Error> = opt(),
    }
}

/// An inbound transport performative, decoded by descriptor.
// Short-lived decode-and-dispatch enum; boxing every variant is not worth the
// churn for the size delta.
#[derive(Debug, Clone, PartialEq)]
#[allow(missing_docs, clippy::large_enum_variant)]
pub enum Performative {
    Open(Open),
    Begin(Begin),
    Attach(Attach),
    Flow(Flow),
    Transfer(Transfer),
    Disposition(Disposition),
    Detach(Detach),
    End(End),
    Close(Close),
}

impl Performative {
    /// A short static name for logging/metrics.
    pub fn kind(&self) -> &'static str {
        match self {
            Performative::Open(_) => "open",
            Performative::Begin(_) => "begin",
            Performative::Attach(_) => "attach",
            Performative::Flow(_) => "flow",
            Performative::Transfer(_) => "transfer",
            Performative::Disposition(_) => "disposition",
            Performative::Detach(_) => "detach",
            Performative::End(_) => "end",
            Performative::Close(_) => "close",
        }
    }
}

impl Encode for Performative {
    fn encode(&self, buf: &mut BytesMut) {
        match self {
            Performative::Open(p) => p.encode(buf),
            Performative::Begin(p) => p.encode(buf),
            Performative::Attach(p) => p.encode(buf),
            Performative::Flow(p) => p.encode(buf),
            Performative::Transfer(p) => p.encode(buf),
            Performative::Disposition(p) => p.encode(buf),
            Performative::Detach(p) => p.encode(buf),
            Performative::End(p) => p.encode(buf),
            Performative::Close(p) => p.encode(buf),
        }
    }
}

impl Decode for Performative {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(match peek_descriptor(buf)? {
            Descriptor::Code(descriptors::OPEN) => Performative::Open(Open::decode(buf)?),
            Descriptor::Code(descriptors::BEGIN) => Performative::Begin(Begin::decode(buf)?),
            Descriptor::Code(descriptors::ATTACH) => Performative::Attach(Attach::decode(buf)?),
            Descriptor::Code(descriptors::FLOW) => Performative::Flow(Flow::decode(buf)?),
            Descriptor::Code(descriptors::TRANSFER) => {
                Performative::Transfer(Transfer::decode(buf)?)
            }
            Descriptor::Code(descriptors::DISPOSITION) => {
                Performative::Disposition(Disposition::decode(buf)?)
            }
            Descriptor::Code(descriptors::DETACH) => Performative::Detach(Detach::decode(buf)?),
            Descriptor::Code(descriptors::END) => Performative::End(End::decode(buf)?),
            Descriptor::Code(descriptors::CLOSE) => Performative::Close(Close::decode(buf)?),
            other => {
                return Err(DecodeError::InvalidValue(format!(
                    "unknown performative descriptor {other}"
                )));
            }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_slice, to_vec};
    use crate::types::messaging::{Accepted, Target};

    fn rt(p: Performative) {
        let bytes = to_vec(&p);
        let back: Performative = from_slice(&bytes).unwrap();
        assert_eq!(p, back, "round trip failed for {}", p.kind());
    }

    #[test]
    fn all_performatives_round_trip() {
        rt(Performative::Open({
            let mut o = Open::new("client-1");
            o.hostname = Some("broker.example".into());
            o.max_frame_size = 65536;
            o.channel_max = 256;
            o.idle_time_out = Some(30_000);
            o.offered_capabilities = vec![Symbol::new("ANONYMOUS-RELAY")];
            o
        }));
        rt(Performative::Begin(Begin {
            remote_channel: None,
            next_outgoing_id: 0,
            incoming_window: 2048,
            outgoing_window: 2048,
            handle_max: 1024,
            ..Default::default()
        }));
        rt(Performative::Attach(Attach {
            name: "sender-link".into(),
            handle: 0,
            role: Role::Sender,
            source: Some(Source::new("ignored")),
            target: Some(TargetArchetype::from(Target::new("queue://out"))),
            initial_delivery_count: Some(0),
            max_message_size: Some(1_048_576),
            ..Default::default()
        }));
        rt(Performative::Flow(Flow {
            next_incoming_id: Some(5),
            incoming_window: 100,
            next_outgoing_id: 5,
            outgoing_window: 100,
            handle: Some(0),
            link_credit: Some(50),
            drain: true,
            ..Default::default()
        }));
        rt(Performative::Transfer(Transfer {
            handle: 0,
            delivery_id: Some(1),
            delivery_tag: Some(Bytes::from_static(b"tag-1")),
            message_format: Some(0),
            settled: Some(false),
            more: true,
            ..Default::default()
        }));
        rt(Performative::Disposition(Disposition {
            role: Role::Receiver,
            first: 1,
            last: Some(3),
            settled: true,
            state: Some(DeliveryState::Accepted(Accepted::default())),
            ..Default::default()
        }));
        rt(Performative::Detach(Detach {
            handle: 0,
            closed: true,
            error: None,
        }));
        rt(Performative::End(End::default()));
        rt(Performative::Close(Close::default()));
    }

    #[test]
    fn open_golden_descriptor() {
        // open begins with 0x00 0x53 0x10 (described, smallulong 0x10)
        let bytes = to_vec(&Open::new("c"));
        assert_eq!(&bytes[..3], &[0x00, 0x53, 0x10]);
    }

    #[test]
    fn mandatory_field_missing_errors() {
        // a described list with descriptor ATTACH and zero fields must fail on
        // the first mandatory field (`name`).
        let mut buf = BytesMut::new();
        crate::codec::encode_described_list(&mut buf, descriptors::ATTACH, |_fw| {});
        let r: Result<Attach, _> = from_slice(&buf);
        assert!(matches!(r, Err(DecodeError::MissingField("name"))));
    }
}

