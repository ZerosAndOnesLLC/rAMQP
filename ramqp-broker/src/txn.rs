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
//! in ops (`MAX_TXNS`, `MAX_STAGED`) *and* bytes (`MAX_STAGED_BYTES`, per
//! connection) so a transaction cannot become an unbounded buffer (§3.2).
//!
//! # Commit atomicity
//! A commit runs in two phases: capacity slots are **reserved** on every
//! target queue first ([`crate::queue::QueueMsg::Reserve`]), and only when
//! every queue holds its slots do the enqueues publish (each awaiting its
//! queue's own durability confirm). Every deterministic refusal — a full
//! queue, a dead or deleted actor — therefore aborts the transaction before
//! a single message lands. A *non-deterministic* failure mid-apply (an fsync
//! error, Raft leadership loss) can still leave earlier enqueues applied;
//! that residue is reported honestly as [`DischargeOutcome::Partial`] (the
//! client is told how much landed instead of a false "rolled back").
//! Discharge execution is detached from the connection, so a connection
//! dying mid-commit never strands a half-applied transaction.

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};

use ramqp_core::codec::from_slice;
use ramqp_core::txn::{Declare, Discharge, TxnId};

use crate::queue::{ConnCmd, PublishAck, QueueMsg, SettleOutcome, SubId};

/// Concurrent transactions per connection.
const MAX_TXNS: usize = 64;
/// Staged operations (enqueues + settlements) per transaction.
const MAX_STAGED: usize = 10_000;
/// Staged publish BYTES per connection, across all its transactions. The
/// op-count caps alone still admit 64 × 10k × max-message-size — hundreds
/// of GiB of pinned bodies from one authorized client; this is the actual
/// memory bound (§3.2).
const MAX_STAGED_BYTES: usize = 64 * 1024 * 1024;

/// A decoded coordinator control message.
#[derive(Debug)]
pub(crate) enum TxnControl {
    /// Begin a transaction. `global_id` is set when the client requested a
    /// distributed transaction — which this broker (local transactions
    /// only) does not support and must reject rather than silently treat as
    /// a local declare (LOW-15).
    Declare { global_id: bool },
    /// End one: `fail == false` → commit, `true` → roll back.
    Discharge { txn_id: TxnId, fail: bool },
}

/// Decode a control-message body (one `amqp-value` section wrapping
/// `declare` or `discharge`).
pub(crate) fn decode_control(body: &[u8]) -> Option<TxnControl> {
    // Strip the amqp-value section header: DESCRIBED byte + descriptor.
    // The content is itself a described type; try both shapes.
    if let Ok(v) = from_slice::<AmqpValue<Declare>>(body) {
        return Some(TxnControl::Declare {
            global_id: v.0.global_id.is_some(),
        });
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
    /// Set when a staged operation had to be refused (the staging cap): part
    /// of the transaction's work is missing, so a commit would be silently
    /// partial — discharge must fail and roll back instead.
    pub rollback_only: bool,
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
    /// Total bytes of staged publish bodies across all open transactions
    /// (bounded by [`MAX_STAGED_BYTES`]).
    staged_bytes: usize,
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

    /// Stage an enqueue under `txn_id`. A limit refusal on a known
    /// transaction also poisons it (rollback-only): its staged work is
    /// incomplete, so a commit would be silently partial even though the
    /// producer saw the rejection. The result distinguishes an unknown txn
    /// (`amqp:transaction:unknown-id`) from a staging-limit refusal
    /// (`amqp:resource-limit-exceeded`) so the disposition is spec-accurate
    /// (LOW-15).
    pub fn stage_publish(&mut self, txn_id: &TxnId, publish: StagedPublish) -> PublishStage {
        let over_bytes = self.staged_bytes + publish.body.len() > MAX_STAGED_BYTES;
        match self.txns.get_mut(txn_id) {
            Some(txn) if !over_bytes && txn.staged() < MAX_STAGED => {
                self.staged_bytes += publish.body.len();
                txn.publishes.push(publish);
                PublishStage::Staged
            }
            Some(txn) => {
                txn.rollback_only = true;
                PublishStage::Capped
            }
            None => PublishStage::UnknownTxn,
        }
    }

    /// Stage a settlement under `txn_id`. A refusal hands the settle back so
    /// the caller can requeue the message — leaving it in flight would
    /// strand it invisibly (no redelivery timer; only connection teardown
    /// would ever release it). A refusal at the cap also poisons the
    /// transaction ([`Txn::rollback_only`]): its staged work is incomplete,
    /// so the discharge must fail rather than commit silently partial work.
    pub fn stage_settle(&mut self, txn_id: &TxnId, settle: StagedSettle) -> SettleStage {
        match self.txns.get_mut(txn_id) {
            Some(txn) if txn.staged() < MAX_STAGED => {
                txn.settles.push(settle);
                SettleStage::Staged
            }
            Some(txn) => {
                txn.rollback_only = true;
                SettleStage::Refused {
                    settle,
                    known_txn: true,
                }
            }
            None => SettleStage::Refused {
                settle,
                known_txn: false,
            },
        }
    }

    /// Close a transaction, taking its staged work (its bytes leave the
    /// staging budget).
    pub fn take(&mut self, txn_id: &TxnId) -> Option<Txn> {
        let txn = self.txns.remove(txn_id)?;
        let bytes: usize = txn.publishes.iter().map(|p| p.body.len()).sum();
        self.staged_bytes = self.staged_bytes.saturating_sub(bytes);
        Some(txn)
    }

    /// Drain every open transaction (coordinator link detach / connection
    /// teardown): each is returned for rollback so its staged settles
    /// requeue and its staged publish bytes free, and the `MAX_TXNS` slots
    /// are reclaimed.
    pub fn take_all(&mut self) -> Vec<Txn> {
        self.staged_bytes = 0;
        self.txns.drain().map(|(_, txn)| txn).collect()
    }
}

/// How [`TxnManager::stage_publish`] resolved.
pub(crate) enum PublishStage {
    /// Staged under the transaction.
    Staged,
    /// The transaction does not exist (→ `amqp:transaction:unknown-id`).
    UnknownTxn,
    /// A staging limit was reached; the txn is now rollback-only
    /// (→ `amqp:resource-limit-exceeded`).
    Capped,
}

/// How [`TxnManager::stage_settle`] resolved.
pub(crate) enum SettleStage {
    /// Staged; applied or undone at discharge.
    Staged,
    /// Not staged — the caller must requeue the message. `known_txn` says
    /// whether the transaction exists (cap reached, now rollback-only) or
    /// was already discharged (the disposition raced the discharge frame).
    Refused {
        settle: StagedSettle,
        known_txn: bool,
    },
}

/// How a discharge resolved (reported to the client as its disposition).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DischargeOutcome {
    /// The discharge applied in full: a commit landed every staged enqueue
    /// and settlement, or a rollback undid everything.
    Complete,
    /// The commit failed before any enqueue landed; all staged work was
    /// rolled back (staged settlements requeued). Atomicity held.
    RolledBack,
    /// The commit failed after `applied` of `total` staged enqueues had
    /// already been confirmed by their queues (an fsync error, Raft
    /// leadership loss, or actor death mid-apply). Those messages cannot be
    /// withdrawn; the client must be told the truth so a retry's duplicates
    /// are expected.
    Partial { applied: usize, total: usize },
    /// The coordinator task died before reporting (should not happen).
    Unknown,
}

/// Commit a discharged transaction: reserve → publish → settle.
///
/// Runs detached from the owning connection (see the module docs on commit
/// atomicity). Staged settlements apply their outcomes only when every
/// enqueue landed; otherwise they requeue (the consumer's work is undone
/// with the transaction, at-least-once).
pub(crate) async fn execute_commit(txn: Txn) -> DischargeOutcome {
    // Group the staged publishes by target queue for slot reservation.
    let mut groups: Vec<(mpsc::Sender<QueueMsg>, String, u32)> = Vec::new();
    for p in &txn.publishes {
        match groups.iter_mut().find(|(_, name, _)| *name == p.queue_name) {
            Some((_, _, n)) => *n += 1,
            None => groups.push((p.queue.clone(), p.queue_name.clone(), 1)),
        }
    }

    // Phase 1 — reserve capacity on every target queue. Any refusal aborts
    // the whole commit before a single enqueue lands.
    for (held, (queue, name, count)) in groups.iter().enumerate() {
        let (reply_tx, reply_rx) = oneshot::channel();
        let ok = queue
            .send(QueueMsg::Reserve {
                count: *count,
                reply: reply_tx,
            })
            .await
            .is_ok()
            && reply_rx.await.unwrap_or(false);
        if !ok {
            tracing::debug!(queue = %name, "transaction refused at reserve; rolling back");
            // groups[..held] already hold reservations: release them.
            for (queue, _, count) in &groups[..held] {
                let _ = queue.send(QueueMsg::Unreserve { count: *count }).await;
            }
            requeue_settles(txn.settles).await;
            return DischargeOutcome::RolledBack;
        }
    }

    // Phase 2 — publish into the reserved slots, awaiting each queue's own
    // durability confirm (fsync / Raft commit). Only non-deterministic
    // failures can refuse here.
    let total = txn.publishes.len();
    let mut remaining: Vec<u32> = groups.iter().map(|(_, _, n)| *n).collect();
    for (applied, publish) in txn.publishes.iter().enumerate() {
        let gi = groups
            .iter()
            .position(|(_, name, _)| *name == publish.queue_name)
            .expect("grouped above");
        let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<ConnCmd>();
        let sent = publish
            .queue
            .send(QueueMsg::PublishReserved {
                body: publish.body.clone(),
                ack: Some(PublishAck {
                    conn: ack_tx,
                    channel: 0,
                    handle: 0,
                    binding_gen: 0,
                    delivery_id: 0,
                }),
            })
            .await
            .is_ok();
        remaining[gi] -= 1;
        let accepted = sent
            && matches!(
                ack_rx.recv().await,
                Some(ConnCmd::SettleIncoming { accepted: true, .. })
            );
        if !accepted {
            tracing::error!(
                queue = %publish.queue_name,
                applied,
                total,
                "transactional publish refused after reservation; aborting commit"
            );
            for ((queue, _, _), rest) in groups.iter().zip(&remaining) {
                if *rest > 0 {
                    let _ = queue.send(QueueMsg::Unreserve { count: *rest }).await;
                }
            }
            requeue_settles(txn.settles).await;
            return if applied == 0 {
                DischargeOutcome::RolledBack
            } else {
                DischargeOutcome::Partial { applied, total }
            };
        }
    }

    // Every enqueue landed: apply the staged settlements.
    for settle in txn.settles {
        let _ = settle
            .queue
            .send(QueueMsg::Settle {
                sub: settle.sub,
                msg_id: settle.msg_id,
                outcome: settle.outcome,
            })
            .await;
    }
    DischargeOutcome::Complete
}

/// Roll a discharged transaction back: staged enqueues drop; staged
/// settlements requeue their (still in-flight) messages.
pub(crate) async fn execute_rollback(txn: Txn) {
    requeue_settles(txn.settles).await;
}

async fn requeue_settles(settles: Vec<StagedSettle>) {
    for settle in settles {
        let _ = settle
            .queue
            .send(QueueMsg::Settle {
                sub: settle.sub,
                msg_id: settle.msg_id,
                outcome: SettleOutcome::Requeue,
            })
            .await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::policy::EffectivePolicy;
    use crate::queue::{self, QueueHandle, QueueStats};

    fn staged(queue: &QueueHandle, body: &'static [u8]) -> StagedPublish {
        StagedPublish {
            queue: queue.tx.clone(),
            queue_name: queue.name.clone(),
            body: Bytes::from_static(body),
        }
    }

    async fn stats(q: &QueueHandle) -> QueueStats {
        let (tx, rx) = oneshot::channel();
        q.tx.send(QueueMsg::Stats { reply: tx }).await.unwrap();
        rx.await.unwrap()
    }

    /// The CRIT-1 regression: a commit spanning two queues where one refuses
    /// (full, no drop-head) must land NOTHING on the healthy queue.
    #[tokio::test]
    async fn commit_is_atomic_across_queues_when_one_is_full() {
        let healthy = queue::spawn("healthy".into(), EffectivePolicy::depth_only(100));
        let full = queue::spawn("full".into(), EffectivePolicy::depth_only(1));
        // Fill the bounded queue.
        full.tx
            .send(QueueMsg::Publish {
                body: Bytes::from_static(b"occupier"),
                ack: None,
            })
            .await
            .unwrap();

        let txn = Txn {
            publishes: vec![staged(&healthy, b"one"), staged(&full, b"two")],
            settles: Vec::new(),
            rollback_only: false,
        };
        let outcome = execute_commit(txn).await;
        assert_eq!(outcome, DischargeOutcome::RolledBack);
        assert_eq!(
            stats(&healthy).await.ready,
            0,
            "no partial application: the healthy queue must stay empty"
        );
        assert_eq!(stats(&full).await.ready, 1, "occupier untouched");

        // The failed commit released its reservation on the healthy queue:
        // ordinary publishes still fit.
        healthy
            .tx
            .send(QueueMsg::Publish {
                body: Bytes::from_static(b"later"),
                ack: None,
            })
            .await
            .unwrap();
        assert_eq!(stats(&healthy).await.ready, 1);
    }

    /// A commit against a dead queue actor rolls back without touching the
    /// live queues.
    #[tokio::test]
    async fn commit_is_atomic_when_an_actor_is_dead() {
        let healthy = queue::spawn("healthy".into(), EffectivePolicy::depth_only(100));
        // A closed mailbox stands in for a dead actor.
        let dead_tx = {
            let (tx, rx) = mpsc::channel(1);
            drop(rx);
            tx
        };

        let txn = Txn {
            publishes: vec![
                staged(&healthy, b"one"),
                StagedPublish {
                    queue: dead_tx,
                    queue_name: "dead".into(),
                    body: Bytes::from_static(b"two"),
                },
            ],
            settles: Vec::new(),
            rollback_only: false,
        };
        let outcome = execute_commit(txn).await;
        assert_eq!(outcome, DischargeOutcome::RolledBack);
        assert_eq!(stats(&healthy).await.ready, 0, "nothing landed");
    }

    /// A settle refused at the staging cap poisons the transaction
    /// (rollback-only) and is handed back for requeueing; a settle for an
    /// already-discharged transaction is handed back too (HIGH-5).
    #[tokio::test]
    async fn refused_settles_are_returned_and_poison_the_txn() {
        let (queue_tx, _queue_rx) = mpsc::channel(1);
        let mk_settle = || StagedSettle {
            queue: queue_tx.clone(),
            sub: 1,
            msg_id: 1,
            outcome: SettleOutcome::Ack,
        };

        let mut txns = TxnManager::default();
        let id = txns.declare().expect("declare");
        // Fill the transaction to its cap.
        for _ in 0..super::MAX_STAGED {
            assert!(matches!(
                txns.stage_settle(&id, mk_settle()),
                SettleStage::Staged
            ));
        }
        // One more: refused, known txn, and the txn is now rollback-only.
        match txns.stage_settle(&id, mk_settle()) {
            SettleStage::Refused { known_txn: true, .. } => {}
            _ => panic!("expected a known-txn refusal at the cap"),
        }
        let txn = txns.take(&id).expect("take");
        assert!(txn.rollback_only, "cap refusal must poison the transaction");

        // A settle for a discharged (unknown) transaction is refused too.
        match txns.stage_settle(&id, mk_settle()) {
            SettleStage::Refused {
                known_txn: false, ..
            } => {}
            _ => panic!("expected an unknown-txn refusal"),
        }
    }

    /// HIGH-4 (issue #19): staged publish BYTES are bounded per connection —
    /// the op-count caps alone admitted hundreds of GiB of pinned bodies.
    /// A byte refusal poisons the transaction, and discharge frees the
    /// budget.
    #[tokio::test]
    async fn staged_publish_bytes_are_bounded_and_released() {
        let (queue_tx, _queue_rx) = mpsc::channel(1);
        let big = Bytes::from(vec![0u8; super::MAX_STAGED_BYTES / 2 + 1]);
        let mk = |body: &Bytes| StagedPublish {
            queue: queue_tx.clone(),
            queue_name: "q".into(),
            body: body.clone(),
        };

        let mut txns = TxnManager::default();
        let id = txns.declare().expect("declare");
        assert!(
            matches!(txns.stage_publish(&id, mk(&big)), PublishStage::Staged),
            "first half fits"
        );
        assert!(
            matches!(txns.stage_publish(&id, mk(&big)), PublishStage::Capped),
            "second half exceeds the byte budget"
        );
        let txn = txns.take(&id).expect("take");
        assert!(txn.rollback_only, "byte refusal must poison the txn");

        // The discharge released the budget: a fresh txn can stage again.
        let id2 = txns.declare().expect("declare 2");
        assert!(
            matches!(txns.stage_publish(&id2, mk(&big)), PublishStage::Staged),
            "budget freed by the previous discharge"
        );
        // An unknown transaction is distinguished from a cap refusal.
        assert!(matches!(
            txns.stage_publish(&Bytes::from_static(b"nope"), mk(&big)),
            PublishStage::UnknownTxn
        ));
    }

    /// MED-14 (issue #19): a coordinator link detach drains every open
    /// transaction so its slots and staged bytes are reclaimed at once (not
    /// held until connection close).
    #[test]
    fn take_all_drains_every_txn_and_frees_the_budget() {
        let (queue_tx, _rx) = mpsc::channel(1);
        let mut txns = TxnManager::default();
        let a = txns.declare().expect("a");
        let b = txns.declare().expect("b");
        txns.stage_publish(
            &a,
            StagedPublish {
                queue: queue_tx.clone(),
                queue_name: "q".into(),
                body: Bytes::from_static(b"x"),
            },
        );
        txns.stage_publish(
            &b,
            StagedPublish {
                queue: queue_tx,
                queue_name: "q".into(),
                body: Bytes::from_static(b"y"),
            },
        );
        let drained = txns.take_all();
        assert_eq!(drained.len(), 2, "both open transactions drained");
        assert_eq!(txns.staged_bytes, 0, "staging budget reset");
        // Slots reclaimed: the table is empty again.
        assert!(txns.take(&a).is_none() && txns.take(&b).is_none());
    }

    /// The happy path: all enqueues land, in staging order per queue.
    #[tokio::test]
    async fn commit_applies_everything_when_all_queues_accept() {
        let a = queue::spawn("a".into(), EffectivePolicy::depth_only(10));
        let b = queue::spawn("b".into(), EffectivePolicy::depth_only(10));
        let txn = Txn {
            publishes: vec![staged(&a, b"1"), staged(&b, b"2"), staged(&a, b"3")],
            settles: Vec::new(),
            rollback_only: false,
        };
        let outcome = execute_commit(txn).await;
        assert_eq!(outcome, DischargeOutcome::Complete);
        assert_eq!(stats(&a).await.ready, 2);
        assert_eq!(stats(&b).await.ready, 1);
    }
}
