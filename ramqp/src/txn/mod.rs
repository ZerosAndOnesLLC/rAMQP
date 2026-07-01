//! Transaction controller (clean-room, AMQP 1.0 spec part 4), behind the
//! `transaction` feature.
//!
//! A [`TransactionController`] attaches a control link to a coordinator and
//! exchanges `declare`/`discharge` control messages to begin and commit/abort
//! local transactions. The wire types live in `ramqp-core` and are re-exported
//! here, so existing `ramqp::txn::...` paths keep working.

// Role-neutral wire types + helpers from ramqp-core.
pub use ramqp_core::txn::{
    Declare, Declared, Discharge, TransactionalState, TxnId, capabilities, transactional_state,
};

use ramqp_core::txn::{control_message, declared_txn_id};

use crate::api::producer::Producer;
use crate::error::{ErrorKind, SendError};
use crate::types::messaging::DeliveryState;

/// A handle to a transaction control link.
///
/// ```no_run
/// # async fn ex(producer: &ramqp::Producer, ctl: &ramqp::txn::TransactionController) -> Result<(), Box<dyn std::error::Error>> {
/// use ramqp::{Message, txn};
/// let txn_id = ctl.declare().await?;
/// producer
///     .send_with_state(Message::text("in a txn"), txn::transactional_state(txn_id.clone(), None))
///     .await?;
/// ctl.commit(txn_id).await?;
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct TransactionController {
    control: Producer,
}

impl TransactionController {
    /// Wrap a control-link producer.
    pub fn new(control: Producer) -> Self {
        TransactionController { control }
    }

    /// Declare a new transaction, returning its id.
    pub async fn declare(&self) -> Result<TxnId, SendError> {
        let body = control_message(&Declare { global_id: None });
        let outcome = self.control.send_bytes(body, false).await?;
        declared_txn_id(&outcome).ok_or_else(|| {
            SendError::msg(
                ErrorKind::ProtocolViolation,
                "coordinator did not return a declared outcome",
            )
        })
    }

    /// Discharge (commit or roll back) a transaction.
    pub async fn discharge(&self, txn_id: TxnId, fail: bool) -> Result<(), SendError> {
        let body = control_message(&Discharge { txn_id, fail });
        let outcome = self.control.send_bytes(body, false).await?;
        // Per spec §4.3 a discharge must be answered with accepted or rejected;
        // any other (non-terminal Released/Modified, or unknown) outcome is a
        // coordinator protocol violation, not a silent success.
        match outcome {
            DeliveryState::Accepted(_) => Ok(()),
            DeliveryState::Rejected(r) => {
                let mut msg = String::from("transaction discharge was rejected");
                if let Some(e) = &r.error {
                    msg.push_str(": ");
                    msg.push_str(&e.to_string());
                }
                Err(SendError::msg(ErrorKind::ProtocolViolation, msg))
            }
            other => Err(SendError::msg(
                ErrorKind::ProtocolViolation,
                format!("unexpected discharge outcome: {other:?}"),
            )),
        }
    }

    /// Commit a transaction (`discharge` with `fail = false`).
    pub async fn commit(&self, txn_id: TxnId) -> Result<(), SendError> {
        self.discharge(txn_id, false).await
    }

    /// Roll back a transaction (`discharge` with `fail = true`).
    pub async fn rollback(&self, txn_id: TxnId) -> Result<(), SendError> {
        self.discharge(txn_id, true).await
    }
}
