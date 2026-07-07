//! Transaction wire types (clean-room, AMQP 1.0 spec part 4), behind the
//! `transaction` feature.
//!
//! These are the role-neutral described types and helpers shared by every
//! transactional peer: the client-side controller (in `ramqp`) drives
//! `declare`/`discharge` against a coordinator; the broker-side coordinator
//! (in `ramqp-broker`) decodes them and answers with `declared` outcomes.

use bytes::{BufMut, Bytes, BytesMut};

use crate::amqp_composite;
use crate::codec::described::descriptors;
use crate::codec::encode::encode_descriptor;
use crate::codec::{Encode, from_slice, to_vec};
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
pub fn control_message<T: Encode>(content: &T) -> Bytes {
    let mut buf = BytesMut::new();
    buf.put_u8(crate::codec::codes::DESCRIBED);
    encode_descriptor(&mut buf, descriptors::AMQP_VALUE);
    content.encode(&mut buf);
    buf.freeze()
}

/// Build the `transactional-state` delivery state that enlists a message in the
/// transaction `txn_id`; `outcome` is the provisional outcome (usually `None`
/// on a send — the coordinator records the result at `discharge`). Attach it
/// to a transfer/disposition `state` field to send or settle within the
/// transaction.
///
/// ```
/// use ramqp_core::txn::transactional_state;
/// use ramqp_core::types::messaging::DeliveryState;
///
/// let state = transactional_state(bytes::Bytes::from_static(b"txn-1"), None);
/// // Carried verbatim on the wire as a described value.
/// assert!(matches!(state, DeliveryState::Other(_)));
/// ```
pub fn transactional_state(txn_id: TxnId, outcome: Option<Outcome>) -> DeliveryState {
    let ts = TransactionalState { txn_id, outcome };
    // The transfer/disposition `state` field carries the described type verbatim
    // as `DeliveryState::Other`; reuse the codec to obtain that raw value.
    let value = from_slice::<crate::codec::Value>(&to_vec(&ts))
        .expect("transactional-state encodes to a valid described value");
    DeliveryState::Other(value)
}

/// Build the `declared` outcome carrying `txn_id` — what a coordinator
/// settles a `declare` control message with.
pub fn declared_state(txn_id: TxnId) -> DeliveryState {
    let declared = Declared { txn_id };
    let value = from_slice::<crate::codec::Value>(&to_vec(&declared))
        .expect("declared encodes to a valid described value");
    DeliveryState::Other(value)
}

/// Try to interpret a delivery state as a `transactional-state` and decode it.
pub fn txn_state(state: &DeliveryState) -> Option<TransactionalState> {
    if let DeliveryState::Other(value) = state {
        return from_slice::<TransactionalState>(&to_vec(value)).ok();
    }
    None
}

/// Try to interpret a delivery state as a `declared` outcome and extract its
/// `txn-id`.
pub fn declared_txn_id(state: &DeliveryState) -> Option<TxnId> {
    if let DeliveryState::Other(value) = state {
        let bytes = to_vec(value);
        if let Ok(declared) = from_slice::<Declared>(&bytes) {
            return Some(declared.txn_id);
        }
    }
    None
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
    fn transactional_state_round_trips_to_txn_state() {
        let st = transactional_state(Bytes::from_static(b"txn-1"), None);
        assert!(matches!(st, DeliveryState::Other(_)));
        // It must encode to the wire as a transactional-state described type.
        let ts: TransactionalState = from_slice(&to_vec(&st)).unwrap();
        assert_eq!(ts.txn_id, Bytes::from_static(b"txn-1"));
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
