//! Sender link state (WP-4.1). The send orchestration (credit/window gating,
//! multi-frame splitting, disposition resolution) lives in the session, which
//! owns the transport; this struct holds the per-link state it operates on.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::config::CreditMode;
use crate::error::{LinkError, SendError};
use crate::link::credit::LinkCredit;
use crate::link::settlement::UnsettledMap;
use crate::proto::{LinkAttached, LinkEvent, Reply};
use crate::types::definitions::SenderSettleMode;
use crate::types::messaging::DeliveryState;

/// A queued outbound message awaiting credit/window before it can be sent.
#[derive(Debug)]
pub struct PendingSend {
    /// The pre-encoded message bytes.
    pub body: Bytes,
    /// Whether to send pre-settled.
    pub settled: bool,
    /// The message format.
    pub message_format: u32,
    /// Reply with the terminal outcome once settled (if awaited).
    pub reply: Option<Reply<DeliveryState, SendError>>,
}

/// Per-sender-link state owned by the session.
#[derive(Debug)]
pub struct SenderLink {
    /// Our local handle.
    pub handle: u32,
    /// The peer's handle (from its responding attach).
    pub remote_handle: Option<u32>,
    /// The link name.
    pub name: String,
    /// Whether the link is attached.
    pub attached: bool,
    /// Channel to the producer handle.
    pub events: mpsc::Sender<LinkEvent>,
    /// The attach reply, held until the peer responds.
    pub pending_attach: Option<Reply<LinkAttached, LinkError>>,
    /// Credit / flow state.
    pub credit: LinkCredit,
    /// Unsettled (sent, awaiting disposition) deliveries.
    pub unsettled: UnsettledMap,
    /// Per-delivery settle-awaiting replies.
    pub pending: HashMap<u32, Reply<DeliveryState, SendError>>,
    /// Messages queued because there was no credit/window when sent.
    pub outbox: VecDeque<PendingSend>,
    /// Our requested sender settle mode.
    pub settle_mode: SenderSettleMode,
    next_tag: u64,
}

impl SenderLink {
    /// Create a sender link in the unattached state.
    pub fn new(
        handle: u32,
        name: String,
        events: mpsc::Sender<LinkEvent>,
        pending_attach: Reply<LinkAttached, LinkError>,
        settle_mode: SenderSettleMode,
        credit_mode: CreditMode,
    ) -> Self {
        SenderLink {
            handle,
            remote_handle: None,
            name,
            attached: false,
            events,
            pending_attach: Some(pending_attach),
            credit: LinkCredit::new(0, credit_mode),
            unsettled: UnsettledMap::new(),
            pending: HashMap::new(),
            outbox: VecDeque::new(),
            settle_mode,
            next_tag: 0,
        }
    }

    /// Allocate the next delivery tag (an 8-byte big-endian counter).
    pub fn next_delivery_tag(&mut self) -> Bytes {
        let tag = self.next_tag;
        self.next_tag = self.next_tag.wrapping_add(1);
        Bytes::copy_from_slice(&tag.to_be_bytes())
    }
}
