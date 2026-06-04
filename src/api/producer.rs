//! The [`Producer`] handle (WP-5.4): send messages on a sender link.

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{Semaphore, mpsc, oneshot};

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
    /// Bounds buffered fire-and-forget sends: one permit per unwritten
    /// [`send_settled`](Self::send_settled) message, released when the driver
    /// writes it. `None` = unbounded (`max_outbox == 0`).
    outbox: Option<Arc<Semaphore>>,
}

impl Producer {
    pub(crate) fn new(
        commands: mpsc::Sender<DriverCommand>,
        channel: ChannelId,
        handle: Handle,
        events: mpsc::Receiver<LinkEvent>,
        max_outbox: usize,
    ) -> Self {
        Producer {
            commands,
            channel,
            handle,
            events,
            outbox: (max_outbox > 0).then(|| Arc::new(Semaphore::new(max_outbox))),
        }
    }

    /// The link handle.
    pub fn handle(&self) -> Handle {
        self.handle
    }

    /// Send a message and await its terminal delivery state (the outcome the
    /// peer settled with — `Accepted`, `Rejected`, …).
    ///
    /// # Examples
    /// ```no_run
    /// # async fn ex(producer: &ramqp::Producer) -> Result<(), Box<dyn std::error::Error>> {
    /// use ramqp::{Message, types::messaging::DeliveryState};
    ///
    /// match producer.send(Message::text("order-42")).await? {
    ///     DeliveryState::Accepted(_) => println!("durably enqueued"),
    ///     other => eprintln!("broker did not accept: {other:?}"),
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn send(&self, message: Message) -> Result<DeliveryState, SendError> {
        self.send_bytes(to_bytes(&message).freeze(), false).await
    }

    /// Send a message carrying an explicit delivery `state` and await its outcome.
    ///
    /// The primary use is enlisting a message in a transaction: pass the
    /// `transactional-state` built from a declared `txn-id` (see
    /// [`txn::transactional_state`](crate::txn::transactional_state) under the
    /// `transaction` feature).
    pub async fn send_with_state(
        &self,
        message: Message,
        state: DeliveryState,
    ) -> Result<DeliveryState, SendError> {
        self.send_bytes_with_state(to_bytes(&message).freeze(), false, Some(state))
            .await
    }

    /// Send a pre-encoded body and await its outcome.
    pub async fn send_bytes(&self, body: Bytes, settled: bool) -> Result<DeliveryState, SendError> {
        self.send_bytes_with_state(body, settled, None).await
    }

    async fn send_bytes_with_state(
        &self,
        body: Bytes,
        settled: bool,
        state: Option<DeliveryState>,
    ) -> Result<DeliveryState, SendError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::SendTransfer {
                channel: self.channel,
                handle: self.handle,
                body,
                settled,
                message_format: 0,
                state,
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
    /// This trades the disposition round-trip for throughput, but is bounded:
    /// at most `max_outbox` (see [`LinkConfig`](crate::config::LinkConfig))
    /// unwritten messages may be buffered while awaiting broker credit, after
    /// which the call back-pressures (awaits an outbox slot) rather than growing
    /// memory unbounded. Set `max_outbox = 0` to opt back into unbounded
    /// buffering. Use [`send`](Producer::send) when you need the full
    /// credit→disposition loop and the broker's terminal outcome.
    ///
    /// # Examples
    /// ```no_run
    /// # async fn ex(producer: &ramqp::Producer) -> Result<(), Box<dyn std::error::Error>> {
    /// use ramqp::Message;
    /// // Fire a burst; the bounded outbox back-pressures instead of buffering forever.
    /// for i in 0..1000 {
    ///     producer.send_settled(Message::text(format!("event-{i}"))).await?;
    /// }
    /// # Ok(()) }
    /// ```
    pub async fn send_settled(&self, message: Message) -> Result<(), SendError> {
        let body = to_bytes(&message).freeze();
        match &self.outbox {
            // Bounded: hold a permit until the driver confirms the write.
            Some(sem) => {
                let permit = sem
                    .clone()
                    .acquire_owned()
                    .await
                    .map_err(|_| SendError::msg(ErrorKind::Cancelled, "producer closed"))?;
                let (reply_tx, reply_rx) = oneshot::channel();
                self.commands
                    .send(DriverCommand::SendTransfer {
                        channel: self.channel,
                        handle: self.handle,
                        body,
                        settled: true,
                        message_format: 0,
                        state: None,
                        reply: Some(reply_tx),
                    })
                    .await
                    .map_err(|_| SendError::msg(ErrorKind::NotConnected, "connection closed"))?;
                // Free the slot once the message is written (or the link drops).
                tokio::spawn(async move {
                    let _ = reply_rx.await;
                    drop(permit);
                });
                Ok(())
            }
            // Unbounded (opt-in): pure fire-and-forget, no completion tracked.
            None => self
                .commands
                .send(DriverCommand::SendTransfer {
                    channel: self.channel,
                    handle: self.handle,
                    body,
                    settled: true,
                    message_format: 0,
                    state: None,
                    reply: None,
                })
                .await
                .map_err(|_| SendError::msg(ErrorKind::NotConnected, "connection closed")),
        }
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
