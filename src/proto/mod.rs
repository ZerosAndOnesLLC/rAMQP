//! Internal protocol command/event enums — the "alphabet" the actor layers
//! exchange over channels (WP-0.5).
//!
//! User-facing handles never touch protocol state directly: they send a
//! [`DriverCommand`] to the connection driver and await a `oneshot` [`Reply`].
//! The driver routes inbound performatives back out as [`SessionEvent`]s and
//! [`LinkEvent`]s on per-session / per-link channels.

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::config::CreditMode;
use crate::error::{ConnectError, LinkError, RecvError, SendError, SessionError};
use crate::ids::{ChannelId, DeliveryId, Handle, SessionId};
use crate::types::definitions::Error as AmqpError;
use crate::types::messaging::DeliveryState;
use crate::types::performatives::{Attach, Begin, Flow};

/// A one-shot reply channel for a command.
pub type Reply<T, E> = oneshot::Sender<Result<T, E>>;

/// Commands sent from user handles to the connection driver.
///
/// Each variant maps onto one or more AMQP performatives the driver emits:
/// `BeginSession`→`begin`, `EndSession`→`end`, `AttachLink`→`attach`,
/// `DetachLink`→`detach`, `SendTransfer`→`transfer`, `SendDisposition`→
/// `disposition`, `SendFlow`→`flow`, `CloseConnection`→`close`.
#[derive(Debug)]
pub enum DriverCommand {
    /// Begin a new session; the driver allocates a channel and replies with it.
    BeginSession {
        /// The `begin` performative template (windows, capabilities).
        begin: Box<Begin>,
        /// Channel on which the driver delivers this session's events.
        events: mpsc::UnboundedSender<SessionEvent>,
        /// Reply with the opened session details.
        reply: Reply<SessionOpened, SessionError>,
    },
    /// End an existing session.
    EndSession {
        /// The session's wire channel.
        channel: ChannelId,
        /// Optional error to send in `end`.
        error: Option<AmqpError>,
        /// Reply once the session is ended.
        reply: Reply<(), SessionError>,
    },
    /// Attach a link to a session.
    AttachLink {
        /// The owning session's channel.
        channel: ChannelId,
        /// The `attach` performative.
        attach: Box<Attach>,
        /// The receiver credit strategy (ignored for sender links).
        credit_mode: CreditMode,
        /// Channel on which the driver delivers this link's events.
        events: mpsc::Sender<LinkEvent>,
        /// Reply with the attached link details (incl. the peer's `attach`).
        reply: Reply<LinkAttached, LinkError>,
    },
    /// Detach (and optionally close) a link.
    DetachLink {
        /// The owning session's channel.
        channel: ChannelId,
        /// The link handle.
        handle: Handle,
        /// Whether to close the link (vs. a recoverable detach).
        closed: bool,
        /// Optional error to send in `detach`.
        error: Option<AmqpError>,
        /// Reply once the link is detached.
        reply: Reply<(), LinkError>,
    },
    /// Send a message on a sender link. The driver assigns the delivery id/tag,
    /// honors credit and the session window, and performs multi-frame splitting.
    SendTransfer {
        /// The owning session's channel.
        channel: ChannelId,
        /// The sender link handle.
        handle: Handle,
        /// The pre-encoded message bytes.
        body: Bytes,
        /// Whether to send the delivery pre-settled (fire-and-forget).
        settled: bool,
        /// The message format (`0` = AMQP).
        message_format: u32,
        /// If present, the driver replies with the terminal outcome once settled.
        reply: Option<Reply<DeliveryState, SendError>>,
    },
    /// Settle / update the state of received deliveries on a receiver link.
    SendDisposition {
        /// The owning session's channel.
        channel: ChannelId,
        /// The receiver link handle (so the driver can honor its settle mode).
        handle: Handle,
        /// First delivery id in the (inclusive) range.
        first: DeliveryId,
        /// Last delivery id in the range (defaults to `first`).
        last: Option<DeliveryId>,
        /// The delivery state to apply.
        state: DeliveryState,
        /// Whether the receiver settles as part of this disposition.
        settled: bool,
        /// Optional reply once the disposition is written.
        reply: Option<Reply<(), RecvError>>,
    },
    /// Issue link/session flow (credit, drain, echo).
    SendFlow {
        /// The owning session's channel.
        channel: ChannelId,
        /// The `flow` performative.
        flow: Box<Flow>,
    },
    /// Grant *additional* receiver credit (consumer-driven auto-replenish). Keeps
    /// outstanding credit bounded so the broker never overruns the delivery
    /// channel.
    GrantCredit {
        /// The owning session's channel.
        channel: ChannelId,
        /// The receiver link handle.
        handle: Handle,
        /// Credit units to add.
        credit: u32,
    },
    /// Close the whole connection.
    CloseConnection {
        /// Optional error to send in `close`.
        error: Option<AmqpError>,
        /// Reply once the connection is closed.
        reply: Reply<(), ConnectError>,
    },
}

/// The successful result of [`DriverCommand::BeginSession`].
#[derive(Debug, Clone)]
pub struct SessionOpened {
    /// The allocated wire channel for the session.
    pub channel: ChannelId,
    /// The process-local logical session id (stable across reconnects).
    pub session_id: SessionId,
}

/// The successful result of [`DriverCommand::AttachLink`].
#[derive(Debug)]
pub struct LinkAttached {
    /// The link handle in use.
    pub handle: Handle,
    /// The peer's responding `attach` (carries the negotiated source/target,
    /// initial delivery count, max message size, etc.).
    pub remote: Box<Attach>,
}

/// Events the driver pushes to a session handle.
#[derive(Debug, Clone)]
pub enum SessionEvent {
    /// The session ended (locally or by the peer).
    Ended {
        /// The peer's error, if the end was errored.
        error: Option<AmqpError>,
    },
}

/// A fully assembled inbound delivery handed to a consumer.
#[derive(Debug, Clone)]
pub struct IncomingDelivery {
    /// The delivery id assigned by the peer.
    pub delivery_id: DeliveryId,
    /// The delivery tag chosen by the peer.
    pub delivery_tag: Bytes,
    /// The assembled (possibly multi-frame) message bytes — decoded lazily.
    pub message: Bytes,
    /// Whether the peer pre-settled the delivery.
    pub settled: bool,
}

/// Events the driver pushes to a link handle (producer or consumer).
#[derive(Debug, Clone)]
pub enum LinkEvent {
    /// A complete inbound delivery (consumer links).
    Delivery(IncomingDelivery),
    /// A disposition update for one of our outbound deliveries (sender links).
    Disposition {
        /// The delivery whose state changed.
        delivery_id: DeliveryId,
        /// The new delivery state.
        state: DeliveryState,
        /// Whether the peer settled.
        settled: bool,
    },
    /// Link credit was granted/updated (sender links).
    Credit {
        /// The current link credit.
        credit: u32,
        /// Whether the peer requested drain.
        drain: bool,
    },
    /// The link was detached (locally or by the peer).
    Detached {
        /// The peer's error, if the detach was errored.
        error: Option<AmqpError>,
    },
}
