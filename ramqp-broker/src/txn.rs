//! The transaction coordinator (broker.md Phase 8): the `amqp:coordinator`
//! target, spec part 4.
//!
//! A client attaches a sender link whose target is a **coordinator** and
//! drives it with `declare` / `discharge` control messages. Between the two,
//! work enlists in the transaction by carrying `transactional-state` — a
//! producer's transfers stage their enqueues, a consumer's accepts stage
//! their settlements — and nothing touches a queue until `discharge`:
//! commit publishes every staged enqueue (each confirmed by its queue —
//! Raft-committed or fsynced for replicated/durable queues, which is what
//! makes the coordinator cluster-aware for free) and applies every staged
//! settlement; rollback drops staged enqueues and requeues staged
//! settlements.
//!
//! Scope (deliberate): **local transactions**, one connection's worth —
//! transactions die (roll back) with their connection, per the local-
//! transactions capability the coordinator advertises. Staging is bounded
//! (`MAX_TXNS`, `MAX_STAGED`) so a transaction cannot become an unbounded
//! buffer (§3.2).

use bytes::Bytes;
use tokio::sync::mpsc;

use ramqp_core::codec::from_slice;
use ramqp_core::txn::{Declare, Discharge, TxnId};

use crate::queue::{QueueMsg, SettleOutcome, SubId};

/// Concurrent transactions per connection.
const MAX_TXNS: usize = 64;
/// Staged operations (enqueues + settlements) per transaction.
const MAX_STAGED: usize = 10_000;

/// A decoded coordinator control message.
#[derive(Debug)]
pub(crate) enum TxnControl {
    /// Begin a transaction.
    Declare,
    /// End one: `fail == false` → commit, `true` → roll back.
    Discharge { txn_id: TxnId, fail: bool },
}

/// Decode a control-message body (one `amqp-value` section wrapping
/// `declare` or `discharge`).
pub(crate) fn decode_control(body: &[u8]) -> Option<TxnControl> {
    // Strip the amqp-value section header: DESCRIBED byte + descriptor.
    // The content is itself a described type; try both shapes.
    if from_slice::<AmqpValue<Declare>>(body).is_ok() {
        return Some(TxnControl::Declare);
    }
    if let Ok(v) = from_slice::<AmqpValue<Discharge>>(body) {
        return Some(TxnControl::Discharge {
            txn_id: v.0.txn_id,
            fail: v.0.fail,
        });
    }
    None
}

/// An `amqp-value`-section wrapper for decoding control messages.
struct AmqpValue<T>(T);

impl<T: ramqp_core::codec::Decode> ramqp_core::codec::Decode for AmqpValue<T> {
    fn decode(bytes: &mut Bytes) -> Result<Self, ramqp_core::codec::DecodeError> {
        use ramqp_core::codec::described::{descriptors, expect_descriptor};
        expect_descriptor(bytes, descriptors::AMQP_VALUE)?;
        Ok(AmqpValue(T::decode(bytes)?))
    }
}

/// One staged enqueue: where it goes and what it says.
pub(crate) struct StagedPublish {
    pub queue: mpsc::Sender<QueueMsg>,
    pub queue_name: String,
    pub body: Bytes,
}

/// One staged settlement: which dispatch it resolves and how.
pub(crate) struct StagedSettle {
    pub queue: mpsc::Sender<QueueMsg>,
    pub sub: SubId,
    pub msg_id: u64,
    pub outcome: SettleOutcome,
}

/// One open transaction's staged work.
#[derive(Default)]
pub(crate) struct Txn {
    pub publishes: Vec<StagedPublish>,
    pub settles: Vec<StagedSettle>,
}

impl Txn {
    fn staged(&self) -> usize {
        self.publishes.len() + self.settles.len()
    }
}

/// The per-connection transaction table.
#[derive(Default)]
pub(crate) struct TxnManager {
    next_id: u64,
    txns: std::collections::HashMap<TxnId, Txn>,
}

impl std::fmt::Debug for TxnManager {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnManager")
            .field("open", &self.txns.len())
            .finish()
    }
}

impl TxnManager {
    /// Begin a transaction; `None` at the concurrent-transaction cap.
    pub fn declare(&mut self) -> Option<TxnId> {
        if self.txns.len() >= MAX_TXNS {
            return None;
        }
        self.next_id += 1;
        let id = Bytes::from(format!("txn-{}", self.next_id));
        self.txns.insert(id.clone(), Txn::default());
        Some(id)
    }

    /// Stage an enqueue under `txn_id`; `false` if the txn is unknown or at
    /// its staging cap (the publish must then be rejected).
    pub fn stage_publish(&mut self, txn_id: &TxnId, publish: StagedPublish) -> bool {
        match self.txns.get_mut(txn_id) {
            Some(txn) if txn.staged() < MAX_STAGED => {
                txn.publishes.push(publish);
                true
            }
            _ => false,
        }
    }

    /// Stage a settlement under `txn_id`; on `false` the settlement is
    /// dropped and the message stays in flight (requeued by teardown —
    /// at-least-once).
    pub fn stage_settle(&mut self, txn_id: &TxnId, settle: StagedSettle) -> bool {
        match self.txns.get_mut(txn_id) {
            Some(txn) if txn.staged() < MAX_STAGED => {
                txn.settles.push(settle);
                true
            }
            _ => false,
        }
    }

    /// Close a transaction, taking its staged work.
    pub fn take(&mut self, txn_id: &TxnId) -> Option<Txn> {
        self.txns.remove(txn_id)
    }
}
