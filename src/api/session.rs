//! The [`Session`] handle (WP-5.3): creates producers/consumers and manages the
//! session lifecycle.

use std::sync::Arc;

use tokio::sync::{mpsc, oneshot};

use crate::api::consumer::Consumer;
use crate::api::producer::Producer;
use crate::config::{Config, CreditMode};
use crate::error::{ErrorKind, LinkError, SessionError};
use crate::ids::{ChannelId, LinkName};
use crate::proto::{DriverCommand, SessionEvent};
use crate::types::definitions::Role;
use crate::types::messaging::{Source, Target, TargetArchetype};
use crate::types::performatives::Attach;

/// An open session. Create producers/consumers on it, then [`end`](Session::end).
#[derive(Debug)]
pub struct Session {
    commands: mpsc::Sender<DriverCommand>,
    channel: ChannelId,
    events: mpsc::UnboundedReceiver<SessionEvent>,
    config: Arc<Config>,
}

impl Session {
    pub(crate) fn new(
        commands: mpsc::Sender<DriverCommand>,
        channel: ChannelId,
        events: mpsc::UnboundedReceiver<SessionEvent>,
        config: Arc<Config>,
    ) -> Self {
        Session {
            commands,
            channel,
            events,
            config,
        }
    }

    /// The session's wire channel.
    pub fn channel(&self) -> ChannelId {
        self.channel
    }

    /// Attach a sender link writing to `address`.
    pub async fn create_producer(&self, address: &str) -> Result<Producer, LinkError> {
        let attach = Attach {
            name: LinkName::generate("sender").into_inner(),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: self.config.link.sender_settle_mode,
            rcv_settle_mode: self.config.link.receiver_settle_mode,
            source: Some(Source::default()),
            target: Some(TargetArchetype::from(Target::new(address))),
            initial_delivery_count: Some(0),
            max_message_size: self.config.link.max_message_size,
            ..Default::default()
        };
        let (evt_tx, evt_rx) = mpsc::channel(256);
        let attached = self
            .attach(attach, self.config.link.credit_mode, evt_tx)
            .await?;
        Ok(Producer::new(
            self.commands.clone(),
            self.channel,
            attached,
            evt_rx,
            self.config.link.max_outbox,
        ))
    }

    /// Attach a receiver link reading from `address` (config credit mode).
    pub async fn create_consumer(&self, address: &str) -> Result<Consumer, LinkError> {
        self.create_consumer_with(address, self.config.link.credit_mode)
            .await
    }

    /// Attach a receiver link with an explicit credit mode.
    pub async fn create_consumer_with(
        &self,
        address: &str,
        credit_mode: CreditMode,
    ) -> Result<Consumer, LinkError> {
        let attach = Attach {
            name: LinkName::generate("receiver").into_inner(),
            handle: 0,
            role: Role::Receiver,
            snd_settle_mode: self.config.link.sender_settle_mode,
            rcv_settle_mode: self.config.link.receiver_settle_mode,
            source: Some(Source::new(address)),
            target: Some(TargetArchetype::from(Target::default())),
            max_message_size: self.config.link.max_message_size,
            ..Default::default()
        };
        let (window, low_water) = match credit_mode {
            CreditMode::Auto {
                initial,
                refill_threshold,
            } => (initial, refill_threshold),
            CreditMode::Manual => (0, 0),
        };
        // The delivery channel must hold the full credit window so the driver
        // never blocks handing off a delivery (no connection-wide head-of-line
        // blocking from a slow consumer).
        let capacity = (window as usize).max(1) + 64;
        let (evt_tx, evt_rx) = mpsc::channel(capacity);
        let attached = self.attach(attach, credit_mode, evt_tx).await?;
        Ok(Consumer::new(
            self.commands.clone(),
            self.channel,
            attached,
            evt_rx,
            window,
            low_water,
        ))
    }

    /// Attach a transaction control link and return a [`TransactionController`].
    ///
    /// [`TransactionController`]: crate::txn::TransactionController
    #[cfg(feature = "transaction")]
    pub async fn create_transaction_controller(
        &self,
    ) -> Result<crate::txn::TransactionController, LinkError> {
        use crate::types::definitions::SenderSettleMode;
        use crate::types::messaging::{Coordinator, TargetArchetype};

        let attach = Attach {
            name: LinkName::generate("txn-ctrl").into_inner(),
            handle: 0,
            role: Role::Sender,
            snd_settle_mode: SenderSettleMode::Unsettled,
            source: Some(Source::default()),
            target: Some(TargetArchetype::Coordinator(Coordinator {
                capabilities: vec![crate::codec::Symbol::new(
                    crate::txn::capabilities::LOCAL_TRANSACTIONS,
                )],
            })),
            initial_delivery_count: Some(0),
            ..Default::default()
        };
        let (evt_tx, evt_rx) = mpsc::channel(64);
        let attached = self.attach(attach, self.config.link.credit_mode, evt_tx).await?;
        Ok(crate::txn::TransactionController::new(Producer::new(
            self.commands.clone(),
            self.channel,
            attached,
            evt_rx,
            self.config.link.max_outbox,
        )))
    }

    async fn attach(
        &self,
        attach: Attach,
        credit_mode: CreditMode,
        events: mpsc::Sender<crate::proto::LinkEvent>,
    ) -> Result<crate::ids::Handle, LinkError> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::AttachLink {
                channel: self.channel,
                attach: Box::new(attach),
                credit_mode,
                events,
                reply: reply_tx,
            })
            .await
            .map_err(|_| LinkError::msg(ErrorKind::NotConnected, "connection closed"))?;
        let attached = reply_rx
            .await
            .map_err(|_| LinkError::msg(ErrorKind::Cancelled, "driver dropped"))??;
        Ok(attached.handle)
    }

    /// Receive the next session-level event, if any.
    pub async fn next_event(&mut self) -> Option<SessionEvent> {
        self.events.recv().await
    }

    /// End the session and await completion.
    pub async fn end(self) -> Result<(), SessionError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::EndSession {
                channel: self.channel,
                error: None,
                reply: tx,
            })
            .await
            .map_err(|_| SessionError::msg(ErrorKind::NotConnected, "connection closed"))?;
        rx.await
            .map_err(|_| SessionError::msg(ErrorKind::Cancelled, "driver dropped"))?
    }
}
