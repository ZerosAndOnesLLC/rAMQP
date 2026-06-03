//! Transaction coordinator (clean-room, AMQP 1.0 spec part 4), behind the
//! `transaction` feature.
//!
//! A [`TransactionController`] attaches a control link to a coordinator and
//! exchanges `declare`/`discharge` control messages to begin and commit/abort
//! local transactions.

use bytes::{BufMut, Bytes, BytesMut};

use crate::amqp_composite;
use crate::api::producer::Producer;
use crate::codec::described::descriptors;
use crate::codec::encode::encode_descriptor;
use crate::codec::{Encode, from_slice, to_vec};
use crate::error::{ErrorKind, SendError};
use crate::types::messaging::{DeliveryState, Outcome};

/// A transaction identifier (a binary handle assigned by the coordinator).
pub type TxnId = Bytes;

/// Well-known transaction capability symbols.
pub mod capabilities {
    /// Local transactions.
    pub const LOCAL_TRANSACTIONS: &str = "amqp:local-transactions";
    /// Distributed transactions.
    pub const DISTRIBUTED_TRANSACTIONS: &str = "amqp:distributed-transactions";
    /// Multi-txns-per-ssn.
    pub const MULTI_TXNS_PER_SSN: &str = "amqp:multi-txns-per-ssn";
    /// Multi-ssns-per-txn.
    pub const MULTI_SSNS_PER_TXN: &str = "amqp:multi-ssns-per-txn";
}

amqp_composite! {
    /// `declare` (`0x31`): request a new transaction.
    pub struct Declare : descriptors::DECLARE => {
        global_id: Option<Bytes> = opt(),
    }
}

amqp_composite! {
    /// `discharge` (`0x32`): commit (`fail = false`) or roll back (`fail = true`).
    pub struct Discharge : descriptors::DISCHARGE => {
        txn_id: Bytes = req("txn-id"),
        fail: bool = default(false),
    }
}

amqp_composite! {
    /// `declared` (`0x33`): the coordinator's response carrying the new `txn-id`.
    pub struct Declared : descriptors::DECLARED => {
        txn_id: Bytes = req("txn-id"),
    }
}

amqp_composite! {
    /// `transactional-state` (`0x34`): binds a delivery to a transaction.
    pub struct TransactionalState : descriptors::TRANSACTIONAL_STATE => {
        txn_id: Bytes = req("txn-id"),
        outcome: Option<Outcome> = opt(),
    }
}

/// Encode a control message body: a single `amqp-value` section wrapping the
/// declare/discharge described type.
fn control_message<T: Encode>(content: &T) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(crate::codec::codes::DESCRIBED);
    encode_descriptor(&mut buf, descriptors::AMQP_VALUE);
    content.encode(&mut buf);
    buf.freeze()
}

/// Try to interpret a delivery state as a `declared` outcome and extract its
/// `txn-id`.
fn declared_txn_id(state: &DeliveryState) -> Option<TxnId> {
    if let DeliveryState::Other(value) = state {
        let bytes = to_vec(value);
        if let Ok(declared) = from_slice::<Declared>(&bytes) {
            return Some(declared.txn_id);
        }
    }
    None
}

/// A handle to a transaction control link.
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
        match outcome {
            DeliveryState::Accepted(_) => Ok(()),
            DeliveryState::Rejected(_) => Err(SendError::msg(
                ErrorKind::ProtocolViolation,
                "transaction discharge was rejected",
            )),
            _ => Ok(()),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{Decode, Symbol};
    use crate::types::messaging::{Accepted, TargetArchetype};

    fn rt<T: Encode + Decode + PartialEq + std::fmt::Debug>(v: T) {
        let back: T = from_slice(&to_vec(&v)).unwrap();
        assert_eq!(v, back);
    }

    #[test]
    fn txn_types_round_trip() {
        rt(Declare { global_id: None });
        rt(Declare {
            global_id: Some(Bytes::from_static(b"global")),
        });
        rt(Discharge {
            txn_id: Bytes::from_static(b"txn-1"),
            fail: true,
        });
        rt(Declared {
            txn_id: Bytes::from_static(b"txn-1"),
        });
        rt(TransactionalState {
            txn_id: Bytes::from_static(b"txn-1"),
            outcome: Some(Outcome::Accepted(Accepted::default())),
        });
    }

    #[test]
    fn declared_outcome_extraction() {
        // A disposition state carrying `declared` decodes into DeliveryState::Other.
        let declared = Declared {
            txn_id: Bytes::from_static(b"abc"),
        };
        let bytes = to_vec(&declared);
        let state: DeliveryState = from_slice(&bytes).unwrap();
        assert!(matches!(state, DeliveryState::Other(_)));
        assert_eq!(declared_txn_id(&state), Some(Bytes::from_static(b"abc")));
    }

    #[test]
    fn coordinator_target_uses_capability() {
        let coord = crate::types::messaging::Coordinator {
            capabilities: vec![Symbol::new(capabilities::LOCAL_TRANSACTIONS)],
        };
        let archetype = TargetArchetype::Coordinator(coord);
        rt(archetype);
    }
}
