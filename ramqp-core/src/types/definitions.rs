//! AMQP 1.0 transport `definitions` (core spec §2.8): the error model, the
//! `role`/settle-mode restricted types, and the protocol-wide type aliases.

use bytes::{Bytes, BytesMut};

use crate::amqp_composite;
use crate::codec::described::descriptors;
use crate::codec::{Decode, DecodeError, Encode, OrderedMap, Symbol, Value};

/// A polymorphic map keyed by symbol, used for open/attach/etc. `properties`.
pub type Fields = OrderedMap<Symbol, Value>;
/// An RFC-1766/BCP-47 language tag, carried as a symbol.
pub type IetfLanguageTag = Symbol;
/// A duration in milliseconds.
pub type Milliseconds = u32;
/// A duration in seconds.
pub type Seconds = u32;
/// A link handle (per-session scoped link identifier).
pub type Handle = u32;
/// A monotonically increasing delivery identifier within a session.
pub type DeliveryNumber = u32;
/// A session/transfer sequence number (serial; wraps).
pub type SequenceNo = u32;
/// A transfer-id (alias of [`SequenceNo`]).
pub type TransferNumber = SequenceNo;
/// A delivery tag chosen by the sender (≤ 32 octets).
pub type DeliveryTag = Bytes;
/// A message format code (`0` = AMQP).
pub type MessageFormat = u32;

/// The directional role of a link endpoint. Encoded as a `boolean`
/// (`false` = sender, `true` = receiver).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum Role {
    /// The endpoint is the sending side.
    #[default]
    Sender,
    /// The endpoint is the receiving side.
    Receiver,
}

impl Role {
    /// The mirror role — what the *other* end of a link plays. A peer's `attach`
    /// carrying role `R` references our local link endpoint of role `R.opposite()`.
    pub fn opposite(self) -> Role {
        match self {
            Role::Sender => Role::Receiver,
            Role::Receiver => Role::Sender,
        }
    }
}

impl Encode for Role {
    fn encode(&self, buf: &mut BytesMut) {
        matches!(self, Role::Receiver).encode(buf)
    }
}

impl Decode for Role {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(if bool::decode(buf)? {
            Role::Receiver
        } else {
            Role::Sender
        })
    }
}

/// How the sending endpoint settles deliveries. Encoded as a `ubyte`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum SenderSettleMode {
    /// All deliveries are sent unsettled and settled only after the receiver.
    Unsettled = 0,
    /// All deliveries are sent pre-settled.
    Settled = 1,
    /// The sender may send a mix (the default).
    #[default]
    Mixed = 2,
}

impl Encode for SenderSettleMode {
    fn encode(&self, buf: &mut BytesMut) {
        (*self as u8).encode(buf)
    }
}

impl Decode for SenderSettleMode {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            0 => Ok(SenderSettleMode::Unsettled),
            1 => Ok(SenderSettleMode::Settled),
            2 => Ok(SenderSettleMode::Mixed),
            n => Err(DecodeError::InvalidValue(format!(
                "invalid sender-settle-mode {n}"
            ))),
        }
    }
}

/// How the receiving endpoint settles deliveries. Encoded as a `ubyte`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum ReceiverSettleMode {
    /// The receiver settles on receipt; no second round-trip (the default).
    #[default]
    First = 0,
    /// The receiver settles only after the sender settles ("second").
    Second = 1,
}

impl Encode for ReceiverSettleMode {
    fn encode(&self, buf: &mut BytesMut) {
        (*self as u8).encode(buf)
    }
}

impl Decode for ReceiverSettleMode {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        match u8::decode(buf)? {
            0 => Ok(ReceiverSettleMode::First),
            1 => Ok(ReceiverSettleMode::Second),
            n => Err(DecodeError::InvalidValue(format!(
                "invalid receiver-settle-mode {n}"
            ))),
        }
    }
}

macro_rules! condition_enum {
    ($(#[$m:meta])* $name:ident { $( $variant:ident => $sym:literal ),* $(,)? } default $def:ident) => {
        $(#[$m])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq)]
        #[allow(missing_docs)]
        pub enum $name { $( $variant ),* }

        impl $name {
            /// The AMQP symbol string for this condition.
            pub fn as_str(&self) -> &'static str {
                match self { $( $name::$variant => $sym ),* }
            }
            /// Parse a condition from its symbol string.
            #[allow(clippy::should_implement_trait)]
            pub fn from_str(s: &str) -> Option<Self> {
                match s { $( $sym => Some($name::$variant), )* _ => None }
            }
        }

        impl Default for $name {
            fn default() -> Self { $name::$def }
        }
    };
}

condition_enum! {
    /// Conditions in the `amqp:` domain (general protocol errors).
    AmqpError {
        InternalError => "amqp:internal-error",
        NotFound => "amqp:not-found",
        UnauthorizedAccess => "amqp:unauthorized-access",
        DecodeError => "amqp:decode-error",
        ResourceLimitExceeded => "amqp:resource-limit-exceeded",
        NotAllowed => "amqp:not-allowed",
        InvalidField => "amqp:invalid-field",
        NotImplemented => "amqp:not-implemented",
        ResourceLocked => "amqp:resource-locked",
        PreconditionFailed => "amqp:precondition-failed",
        ResourceDeleted => "amqp:resource-deleted",
        IllegalState => "amqp:illegal-state",
        FrameSizeTooSmall => "amqp:frame-size-too-small",
    } default InternalError
}

condition_enum! {
    /// Conditions in the `amqp:connection:` domain.
    ConnectionError {
        ConnectionForced => "amqp:connection:forced",
        FramingError => "amqp:connection:framing-error",
        Redirect => "amqp:connection:redirect",
    } default ConnectionForced
}

condition_enum! {
    /// Conditions in the `amqp:session:` domain.
    SessionError {
        WindowViolation => "amqp:session:window-violation",
        ErrantLink => "amqp:session:errant-link",
        HandleInUse => "amqp:session:handle-in-use",
        UnattachedHandle => "amqp:session:unattached-handle",
    } default WindowViolation
}

condition_enum! {
    /// Conditions in the `amqp:transaction:` domain (spec part 4).
    TransactionError {
        UnknownId => "amqp:transaction:unknown-id",
        TransactionRollback => "amqp:transaction:rollback",
        TransactionTimeout => "amqp:transaction:timeout",
    } default UnknownId
}

condition_enum! {
    /// Conditions in the `amqp:link:` domain.
    LinkError {
        DetachForced => "amqp:link:detach-forced",
        TransferLimitExceeded => "amqp:link:transfer-limit-exceeded",
        MessageSizeExceeded => "amqp:link:message-size-exceeded",
        Redirect => "amqp:link:redirect",
        Stolen => "amqp:link:stolen",
    } default DetachForced
}

/// A symbolic error condition. Well-known conditions decode into the typed
/// domain enums; anything else is preserved verbatim as [`Custom`].
///
/// [`Custom`]: ErrorCondition::Custom
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ErrorCondition {
    /// A condition in the `amqp:` domain.
    Amqp(AmqpError),
    /// A condition in the `amqp:connection:` domain.
    Connection(ConnectionError),
    /// A condition in the `amqp:session:` domain.
    Session(SessionError),
    /// A condition in the `amqp:link:` domain.
    Link(LinkError),
    /// A condition in the `amqp:transaction:` domain.
    Transaction(TransactionError),
    /// Any other (vendor or extension) condition symbol.
    Custom(Symbol),
}

impl ErrorCondition {
    /// The underlying condition symbol string.
    pub fn as_str(&self) -> &str {
        match self {
            ErrorCondition::Amqp(e) => e.as_str(),
            ErrorCondition::Connection(e) => e.as_str(),
            ErrorCondition::Session(e) => e.as_str(),
            ErrorCondition::Link(e) => e.as_str(),
            ErrorCondition::Transaction(e) => e.as_str(),
            ErrorCondition::Custom(s) => s.as_str(),
        }
    }

    fn from_symbol(s: Symbol) -> Self {
        let st = s.as_str();
        if let Some(e) = AmqpError::from_str(st) {
            ErrorCondition::Amqp(e)
        } else if let Some(e) = ConnectionError::from_str(st) {
            ErrorCondition::Connection(e)
        } else if let Some(e) = SessionError::from_str(st) {
            ErrorCondition::Session(e)
        } else if let Some(e) = LinkError::from_str(st) {
            ErrorCondition::Link(e)
        } else if let Some(e) = TransactionError::from_str(st) {
            ErrorCondition::Transaction(e)
        } else {
            ErrorCondition::Custom(s)
        }
    }
}

impl Default for ErrorCondition {
    fn default() -> Self {
        ErrorCondition::Amqp(AmqpError::InternalError)
    }
}

impl From<AmqpError> for ErrorCondition {
    fn from(e: AmqpError) -> Self {
        ErrorCondition::Amqp(e)
    }
}
impl From<ConnectionError> for ErrorCondition {
    fn from(e: ConnectionError) -> Self {
        ErrorCondition::Connection(e)
    }
}
impl From<SessionError> for ErrorCondition {
    fn from(e: SessionError) -> Self {
        ErrorCondition::Session(e)
    }
}
impl From<LinkError> for ErrorCondition {
    fn from(e: LinkError) -> Self {
        ErrorCondition::Link(e)
    }
}
impl From<TransactionError> for ErrorCondition {
    fn from(e: TransactionError) -> Self {
        ErrorCondition::Transaction(e)
    }
}

impl Encode for ErrorCondition {
    fn encode(&self, buf: &mut BytesMut) {
        Symbol::new(self.as_str()).encode(buf)
    }
}

impl Decode for ErrorCondition {
    fn decode(buf: &mut Bytes) -> Result<Self, DecodeError> {
        Ok(ErrorCondition::from_symbol(Symbol::decode(buf)?))
    }
}

impl std::fmt::Display for ErrorCondition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

amqp_composite! {
    /// The AMQP `error` composite (descriptor `0x1d`): a peer-sent error with a
    /// condition, optional human-readable description, and optional info map.
    pub struct Error : descriptors::ERROR => {
        condition: ErrorCondition = req("condition"),
        description: Option<String> = opt(),
        info: Option<Fields> = opt(),
    }
}

impl Error {
    /// Construct an error from a condition and optional description.
    pub fn new(condition: impl Into<ErrorCondition>, description: Option<String>) -> Self {
        Error {
            condition: condition.into(),
            description,
            info: None,
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.condition)?;
        if let Some(d) = &self.description {
            write!(f, ": {d}")?;
        }
        Ok(())
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{from_slice, to_vec};

    #[test]
    fn role_and_settle_modes_round_trip() {
        for r in [Role::Sender, Role::Receiver] {
            assert_eq!(r, from_slice(&to_vec(&r)).unwrap());
        }
        // role encodes as a boolean
        assert_eq!(to_vec(&Role::Sender), [0x42]);
        assert_eq!(to_vec(&Role::Receiver), [0x41]);
        for m in [
            SenderSettleMode::Unsettled,
            SenderSettleMode::Settled,
            SenderSettleMode::Mixed,
        ] {
            assert_eq!(m, from_slice(&to_vec(&m)).unwrap());
        }
        assert_eq!(to_vec(&SenderSettleMode::Mixed), [0x50, 0x02]);
    }

    #[test]
    fn error_round_trips_and_classifies() {
        let e = Error::new(
            AmqpError::ResourceLimitExceeded,
            Some("too many links".into()),
        );
        let back: Error = from_slice(&to_vec(&e)).unwrap();
        assert_eq!(e, back);
        assert_eq!(
            back.condition,
            ErrorCondition::Amqp(AmqpError::ResourceLimitExceeded)
        );

        // unknown condition is preserved as Custom
        let custom = Error::new(ErrorCondition::Custom(Symbol::new("vendor:weird")), None);
        let back: Error = from_slice(&to_vec(&custom)).unwrap();
        assert_eq!(
            back.condition,
            ErrorCondition::Custom(Symbol::new("vendor:weird"))
        );
    }

    #[test]
    fn error_condition_symbol_mapping() {
        assert_eq!(
            ErrorCondition::Link(LinkError::DetachForced).as_str(),
            "amqp:link:detach-forced"
        );
        assert_eq!(
            ErrorCondition::from_symbol(Symbol::new("amqp:session:handle-in-use")),
            ErrorCondition::Session(SessionError::HandleInUse)
        );
    }
}
