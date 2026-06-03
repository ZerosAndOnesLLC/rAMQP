//! The [`Producer`] handle (WP-5.4): send messages on a sender link.

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use crate::api::lifecycle::detach_on_drop;
use crate::codec::to_bytes;
use crate::error::{ErrorKind, LinkError, SendError};
use crate::ids::{ChannelId, Handle};
use crate::proto::{DriverCommand, LinkEvent};
use crate::types::messaging::{DeliveryState, Message};

/// A handle for sending on an attached sender link.
#[derive(Debug)]
pub struct Producer {
    commands: mpsc::Sender<DriverCommand>,
    channel: ChannelId,
    handle: Handle,
    #[allow(dead_code)]
    events: mpsc::Receiver<LinkEvent>,
}

impl Producer {
    pub(crate) fn new(
        commands: mpsc::Sender<DriverCommand>,
        channel: ChannelId,
        handle: Handle,
        events: mpsc::Receiver<LinkEvent>,
    ) -> Self {
        Producer {
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

    /// Send a message and await its terminal delivery state (the outcome the
    /// peer settled with — `Accepted`, `Rejected`, …).
    pub async fn send(&self, message: Message) -> Result<DeliveryState, SendError> {
        self.send_bytes(to_bytes(&message).freeze(), false).await
    }

    /// Send a pre-encoded body and await its outcome.
    pub async fn send_bytes(&self, body: Bytes, settled: bool) -> Result<DeliveryState, SendError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::SendTransfer {
                channel: self.channel,
                handle: self.handle,
                body,
                settled,
                message_format: 0,
                reply: Some(reply_tx),
            })
            .await
            .map_err(|_| SendError::msg(ErrorKind::NotConnected, "connection closed"))?;
        reply_rx
            .await
            .map_err(|_| SendError::msg(ErrorKind::Cancelled, "driver dropped"))?
    }

    /// Send a message pre-settled (fire-and-forget; no disposition awaited).
    ///
    /// This trades flow control for throughput: messages are buffered locally
    /// while awaiting broker credit, so a sustained fire-and-forget burst against
    /// a slow broker can grow memory unbounded. Use [`send`](Producer::send) when
    /// you need the credit→disposition loop to back-pressure the producer.
    pub async fn send_settled(&self, message: Message) -> Result<(), SendError> {
        self.commands
            .send(DriverCommand::SendTransfer {
                channel: self.channel,
                handle: self.handle,
                body: to_bytes(&message).freeze(),
                settled: true,
                message_format: 0,
                reply: None,
            })
            .await
            .map_err(|_| SendError::msg(ErrorKind::NotConnected, "connection closed"))
    }

    /// Detach the link and await completion.
    pub async fn detach(mut self) -> Result<(), LinkError> {
        let (tx, rx) = oneshot::channel();
        let commands = self.commands.clone();
        // Prevent the Drop impl from also detaching.
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
}

impl Drop for Producer {
    fn drop(&mut self) {
        if self.handle != Handle(u32::MAX) {
            detach_on_drop(&self.commands, self.channel, self.handle);
        }
    }
}
