//! Receiver link state (WP-4.2). Transfer assembly, credit issuance, and
//! disposition emission are orchestrated by the session; this struct holds the
//! per-link state.

use tokio::sync::mpsc;

use crate::config::CreditMode;
use crate::error::LinkError;
use crate::link::credit::LinkCredit;
use crate::link::delivery::PartialDelivery;
use crate::link::settlement::UnsettledMap;
use crate::proto::{LinkAttached, LinkEvent, Reply};
use crate::types::definitions::ReceiverSettleMode;

/// Absolute ceiling on an assembled delivery, applied even when the link
/// advertises no `max-message-size` (defense against unbounded assembly).
pub const HARD_MAX_MESSAGE_SIZE: u64 = 256 * 1024 * 1024;

/// Per-receiver-link state owned by the session.
#[derive(Debug)]
pub struct ReceiverLink {
    /// Our local handle.
    pub handle: u32,
    /// The peer's handle (from its responding attach).
    pub remote_handle: Option<u32>,
    /// The link name.
    pub name: String,
    /// Whether the link is attached.
    pub attached: bool,
    /// Channel to the consumer handle.
    pub events: mpsc::Sender<LinkEvent>,
    /// The attach reply, held until the peer responds.
    pub pending_attach: Option<Reply<LinkAttached, LinkError>>,
    /// Credit / flow state.
    pub credit: LinkCredit,
    /// Unsettled (received, awaiting settle) deliveries.
    pub unsettled: UnsettledMap,
    /// A multi-frame delivery being assembled.
    pub partial: Option<PartialDelivery>,
    /// Our requested receiver settle mode.
    pub settle_mode: ReceiverSettleMode,
    /// The maximum assembled message size we accept (`None` = only the hard cap).
    pub max_message_size: Option<u64>,
}

impl ReceiverLink {
    /// Create a receiver link in the unattached state.
    pub fn new(
        handle: u32,
        name: String,
        events: mpsc::Sender<LinkEvent>,
        pending_attach: Reply<LinkAttached, LinkError>,
        settle_mode: ReceiverSettleMode,
        credit_mode: CreditMode,
        max_message_size: Option<u64>,
    ) -> Self {
        ReceiverLink {
            handle,
            remote_handle: None,
            name,
            attached: false,
            events,
            pending_attach: Some(pending_attach),
            credit: LinkCredit::new(0, credit_mode),
            unsettled: UnsettledMap::new(),
            partial: None,
            settle_mode,
            max_message_size,
        }
    }

    /// The effective per-delivery byte cap (configured size, bounded by the
    /// hard ceiling).
    pub fn size_cap(&self) -> u64 {
        self.max_message_size
            .map(|m| m.min(HARD_MAX_MESSAGE_SIZE))
            .unwrap_or(HARD_MAX_MESSAGE_SIZE)
    }

    /// The configured initial auto-credit, if any.
    pub fn initial_credit(&self) -> u32 {
        match self.credit.mode() {
            CreditMode::Auto { initial, .. } => initial,
            CreditMode::Manual => 0,
        }
    }
}
