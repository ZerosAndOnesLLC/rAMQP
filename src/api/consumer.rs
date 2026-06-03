//! The [`Consumer`] handle (WP-5.5): receive and settle deliveries.

use futures_core::Stream;
use tokio::sync::{mpsc, oneshot};

use crate::api::lifecycle::detach_on_drop;
use crate::error::{ErrorKind, LinkError, RecvError, RemoteError};
use crate::ids::{ChannelId, Handle};
use crate::link::Delivery;
use crate::proto::{DriverCommand, LinkEvent};
use crate::types::definitions::Error as AmqpError;
use crate::types::messaging::{
    Accepted, DeliveryState, Modified, Outcome, Rejected, Released,
};

/// A handle for receiving and settling deliveries on a receiver link.
#[derive(Debug)]
pub struct Consumer {
    commands: mpsc::Sender<DriverCommand>,
    channel: ChannelId,
    handle: Handle,
    events: mpsc::Receiver<LinkEvent>,
}

impl Consumer {
    pub(crate) fn new(
        commands: mpsc::Sender<DriverCommand>,
        channel: ChannelId,
        handle: Handle,
        events: mpsc::Receiver<LinkEvent>,
    ) -> Self {
        Consumer {
            commands,
            channel,
            handle,
            events,
        }
    }

    /// The link handle.
    pub fn handle(&self) -> Handle {
        self.handle
    }

    /// Await the next delivery (auto-credit keeps the link fed; for manual
    /// credit, call [`credit`](Consumer::credit) first).
    pub async fn recv(&mut self) -> Result<Delivery, RecvError> {
        loop {
            match self.events.recv().await {
                Some(LinkEvent::Delivery(d)) => {
                    return Ok(Delivery::new(d.delivery_id, d.delivery_tag, d.settled, d.message));
                }
                Some(LinkEvent::Detached { error }) => {
                    return Err(detached_error(error));
                }
                // Credit / disposition events are not relevant to a consumer's recv.
                Some(_) => continue,
                None => return Err(RecvError::msg(ErrorKind::Detached, "link closed")),
            }
        }
    }

    /// Accept a delivery (settles it).
    pub async fn accept(&self, delivery: &Delivery) -> Result<(), RecvError> {
        self.dispose(delivery, DeliveryState::Accepted(Accepted::default()))
            .await
    }

    /// Reject a delivery with an optional error.
    pub async fn reject(&self, delivery: &Delivery, error: Option<AmqpError>) -> Result<(), RecvError> {
        self.dispose(delivery, DeliveryState::Rejected(Rejected { error }))
            .await
    }

    /// Release a delivery back to the sender.
    pub async fn release(&self, delivery: &Delivery) -> Result<(), RecvError> {
        self.dispose(delivery, DeliveryState::Released(Released::default()))
            .await
    }

    /// Modify a delivery (with disposition hints).
    pub async fn modify(&self, delivery: &Delivery, modified: Modified) -> Result<(), RecvError> {
        self.dispose(delivery, DeliveryState::Modified(modified)).await
    }

    /// Settle a delivery with an arbitrary terminal [`Outcome`].
    pub async fn settle(&self, delivery: &Delivery, outcome: Outcome) -> Result<(), RecvError> {
        self.dispose(delivery, DeliveryState::from(outcome)).await
    }

    async fn dispose(&self, delivery: &Delivery, state: DeliveryState) -> Result<(), RecvError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::SendDisposition {
                channel: self.channel,
                first: delivery.delivery_id,
                last: None,
                state,
                settled: true,
                reply: Some(tx),
            })
            .await
            .map_err(|_| RecvError::msg(ErrorKind::NotConnected, "connection closed"))?;
        rx.await
            .map_err(|_| RecvError::msg(ErrorKind::Cancelled, "driver dropped"))?
    }

    /// Grant additional link credit (manual credit mode).
    pub async fn credit(&self, credit: u32) -> Result<(), RecvError> {
        let flow = crate::types::performatives::Flow {
            handle: Some(self.handle.value()),
            link_credit: Some(credit),
            drain: false,
            ..Default::default()
        };
        self.commands
            .send(DriverCommand::SendFlow {
                channel: self.channel,
                flow: Box::new(flow),
            })
            .await
            .map_err(|_| RecvError::msg(ErrorKind::NotConnected, "connection closed"))
    }

    /// Detach the link and await completion.
    pub async fn detach(mut self) -> Result<(), LinkError> {
        let (tx, rx) = oneshot::channel();
        let commands = self.commands.clone();
        let handle = std::mem::replace(&mut self.handle, Handle(u32::MAX));
        commands
            .send(DriverCommand::DetachLink {
                channel: self.channel,
                handle,
                closed: true,
                error: None,
                reply: tx,
            })
            .await
            .map_err(|_| LinkError::msg(ErrorKind::NotConnected, "connection closed"))?;
        rx.await
            .map_err(|_| LinkError::msg(ErrorKind::Cancelled, "driver dropped"))?
    }

    /// Convert into a [`Stream`] of deliveries (ends when the link detaches).
    pub fn into_stream(self) -> impl Stream<Item = Result<Delivery, RecvError>> {
        futures_util::stream::unfold(self, |mut consumer| async move {
            match consumer.recv().await {
                Ok(delivery) => Some((Ok(delivery), consumer)),
                Err(e) if e.kind() == ErrorKind::Detached => None,
                Err(e) => Some((Err(e), consumer)),
            }
        })
    }
}

fn detached_error(error: Option<AmqpError>) -> RecvError {
    match error {
        Some(e) => RecvError::from_remote(ErrorKind::Detached, RemoteError::new(e)),
        None => RecvError::msg(ErrorKind::Detached, "link detached"),
    }
}

impl Drop for Consumer {
    fn drop(&mut self) {
        if self.handle != Handle(u32::MAX) {
            detach_on_drop(&self.commands, self.channel, self.handle);
        }
    }
}
