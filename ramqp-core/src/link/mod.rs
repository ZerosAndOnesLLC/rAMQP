//! Link runtime (Phase 4): sender/receiver state machines, owner-local
//! settlement tracking, credit/flow control, delivery assembly, and recovery.

pub mod credit;
pub mod delivery;
pub mod receiver;
pub mod resume;
pub mod sender;
pub mod settlement;

pub use delivery::Delivery;

use tokio::sync::mpsc;

use crate::error::LinkError;
use crate::proto::{LinkAttached, LinkEvent, Reply};

use receiver::ReceiverLink;
use sender::SenderLink;

/// A link owned by a session: either a sender or a receiver.
#[derive(Debug)]
pub enum Link {
    /// A sender link (producer side).
    Sender(SenderLink),
    /// A receiver link (consumer side).
    Receiver(ReceiverLink),
}

impl Link {
    /// Our local handle.
    pub fn handle(&self) -> u32 {
        match self {
            Link::Sender(l) => l.handle,
            Link::Receiver(l) => l.handle,
        }
    }

    /// The peer's handle, once attached.
    pub fn remote_handle(&self) -> Option<u32> {
        match self {
            Link::Sender(l) => l.remote_handle,
            Link::Receiver(l) => l.remote_handle,
        }
    }

    /// Record the peer's handle.
    pub fn set_remote_handle(&mut self, handle: u32) {
        match self {
            Link::Sender(l) => l.remote_handle = Some(handle),
            Link::Receiver(l) => l.remote_handle = Some(handle),
        }
    }

    /// Mark the link attached.
    pub fn mark_attached(&mut self) {
        match self {
            Link::Sender(l) => l.attached = true,
            Link::Receiver(l) => l.attached = true,
        }
    }

    /// The event channel to the user handle.
    pub fn events(&self) -> &mpsc::Sender<LinkEvent> {
        match self {
            Link::Sender(l) => &l.events,
            Link::Receiver(l) => &l.events,
        }
    }

    /// Take the pending attach reply (completed on the peer's responding attach).
    pub fn take_pending_attach(&mut self) -> Option<Reply<LinkAttached, LinkError>> {
        match self {
            Link::Sender(l) => l.pending_attach.take(),
            Link::Receiver(l) => l.pending_attach.take(),
        }
    }
}
