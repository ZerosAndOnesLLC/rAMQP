//! The in-memory transient queue: one owning task per queue, no locks.
//!
//! A queue is an actor — connections publish, subscribe, grant demand, and
//! settle through a bounded channel; the actor owns the message store, the
//! unacked map, and the round-robin subscriber ring. This is deliberately the
//! same shape a replicated (Raft) queue takes later: `Publish`/`Settle` are
//! exactly the state-machine commands a quorum queue commits to its log
//! (broker.md §7–8), so the Phase 6 lift replaces the storage, not the model.
//!
//! Message bodies stay `bytes::Bytes` from ingest to dispatch — refcount
//! clones only, never a copy (broker.md §3.2).
//!
//! # Channel orientation (deadlock freedom)
//! Connection→queue traffic (the actor mailbox) is **bounded**: a full queue
//! back-pressures the publishing connection, which back-pressures the
//! producer via TCP — the ingest path always has a real bound. Queue→
//! connection traffic ([`ConnCmd`]) is **unbounded at the channel but bounded
//! by protocol**: `Deliver`s are capped by granted demand (≤ consumer link
//! credit) and publish acks by producer credit. A queue actor therefore
//! never awaits a connection, so the wait-for graph has only
//! connection→queue edges and the bounded-channel cycle (driver waiting on a
//! full mailbox while the queue waits on a full command channel) cannot
//! form.

use std::collections::{HashMap, VecDeque};

use bytes::Bytes;
use tokio::sync::mpsc;

/// A queue's identity + mailbox, cheap to clone.
#[derive(Debug, Clone)]
pub(crate) struct QueueHandle {
    /// The queue name (post address-normalization).
    pub name: String,
    /// The actor mailbox.
    pub tx: mpsc::Sender<QueueMsg>,
}

/// A subscriber id within one queue (slab index + generation-free: u64 counter).
pub(crate) type SubId = u64;

/// How a consumer settled a dispatched message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SettleOutcome {
    /// `accepted` — the message is done; drop it.
    Ack,
    /// `released` — return to the queue, no redelivery penalty (spec: the
    /// consumer did not process it).
    Requeue,
    /// `modified{delivery-failed}` — return to the queue and count the failure.
    RequeueFailed,
    /// `rejected` — drop (dead-lettering arrives in Phase 7).
    Drop,
}

/// Commands into a queue actor.
#[derive(Debug)]
pub(crate) enum QueueMsg {
    /// Store a message (from a producer link).
    Publish {
        /// The raw message bytes as received (all sections).
        body: Bytes,
        /// Where to confirm (unsettled publishes); `None` for pre-settled.
        ack: Option<PublishAck>,
    },
    /// Add a consumer. Deliveries flow to `conn` as [`ConnCmd::Deliver`].
    Subscribe {
        /// The subscriber's connection command mailbox.
        conn: mpsc::UnboundedSender<ConnCmd>,
        /// The subscriber's session channel (ours) + link handle, echoed into
        /// every `Deliver` so the connection can route it.
        channel: u16,
        /// Link handle on that channel.
        handle: u32,
        /// The connection's binding generation for this link, echoed into every
        /// `Deliver` so a stale delivery to a reused handle is dropped.
        binding_gen: u64,
        /// Replies with the assigned subscriber id.
        reply: tokio::sync::oneshot::Sender<SubId>,
    },
    /// Grant a subscriber additional demand (an *increment*, not an absolute
    /// set-point). The connection computes this delta from the peer's flow
    /// reconciled against dispatches still in flight, so a restated flow never
    /// re-arms demand that in-flight `Deliver`s already account for.
    Demand {
        /// Which subscriber.
        sub: SubId,
        /// Additional messages it may be sent (added to current demand).
        credit: u32,
        /// The peer set `drain`: after dispatching what is available now, drop
        /// any unmet demand rather than holding it (a poll-to-empty).
        drain: bool,
    },
    /// A consumer settled a dispatched message.
    Settle {
        /// Which subscriber.
        sub: SubId,
        /// The queue-assigned message id.
        msg_id: u64,
        /// What happened.
        outcome: SettleOutcome,
    },
    /// Remove a consumer; its unacked messages requeue.
    Unsubscribe {
        /// Which subscriber.
        sub: SubId,
    },
}

/// Where to confirm an unsettled publish once stored (or refuse it).
#[derive(Debug)]
pub(crate) struct PublishAck {
    /// The producer's connection command mailbox.
    pub conn: mpsc::UnboundedSender<ConnCmd>,
    /// The producer's session channel (ours).
    pub channel: u16,
    /// The producer link handle.
    pub handle: u32,
    /// The connection's binding generation for this producer link, echoed back
    /// so a stale settlement to a reused handle is dropped.
    pub binding_gen: u64,
    /// The peer's delivery id to settle.
    pub delivery_id: u32,
}

/// Commands into a connection driver (from queue actors).
#[derive(Debug)]
pub(crate) enum ConnCmd {
    /// Dispatch a message to a consumer link.
    Deliver {
        /// The session channel (ours) the link lives on.
        channel: u16,
        /// The consumer link handle.
        handle: u32,
        /// The binding generation this command was issued for. `(channel,
        /// handle)` are reused after detach/end, so the connection drops a
        /// command whose generation no longer matches the live binding (else a
        /// stale delivery would be misrouted to a reused link — cross-queue
        /// delivery and wrong-message settlement).
        binding_gen: u64,
        /// The queue-assigned message id (echo back in `Settle`).
        msg_id: u64,
        /// The message bytes.
        body: Bytes,
    },
    /// Settle an inbound (producer) delivery: accept, or reject on overflow.
    SettleIncoming {
        /// The session channel (ours).
        channel: u16,
        /// The producer link handle.
        handle: u32,
        /// The binding generation this settlement was issued for (see
        /// [`ConnCmd::Deliver::binding_gen`]).
        binding_gen: u64,
        /// The peer's delivery id.
        delivery_id: u32,
        /// `true` → accepted; `false` → rejected (e.g. queue full).
        accepted: bool,
    },
}

/// One stored message.
#[derive(Debug)]
struct Stored {
    id: u64,
    body: Bytes,
    /// Delivery attempts that failed (modified{delivery-failed}).
    failures: u32,
}

#[derive(Debug)]
struct Subscriber {
    id: SubId,
    conn: mpsc::UnboundedSender<ConnCmd>,
    channel: u16,
    handle: u32,
    binding_gen: u64,
    demand: u32,
    /// A drain is in progress: zero any leftover demand after the next dispatch.
    drain_pending: bool,
}

/// Spawn a queue actor; the returned handle is its only address.
pub(crate) fn spawn(name: String, max_depth: usize) -> QueueHandle {
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, rx, max_depth));
    handle
}

async fn run(name: String, mut rx: mpsc::Receiver<QueueMsg>, max_depth: usize) {
    let mut ready: VecDeque<Stored> = VecDeque::new();
    let mut unacked: HashMap<u64, (Stored, SubId)> = HashMap::new();
    let mut subs: Vec<Subscriber> = Vec::new();
    let mut next_msg_id: u64 = 0;
    let mut next_sub_id: SubId = 0;
    let mut rr: usize = 0;

    while let Some(msg) = rx.recv().await {
        match msg {
            QueueMsg::Publish { body, ack } => {
                if ready.len() + unacked.len() >= max_depth {
                    // Refuse rather than grow without bound (broker.md §3.2).
                    if let Some(ack) = ack {
                        let _ = ack.conn.send(ConnCmd::SettleIncoming {
                            channel: ack.channel,
                            handle: ack.handle,
                            binding_gen: ack.binding_gen,
                            delivery_id: ack.delivery_id,
                            accepted: false,
                        });
                    } else {
                        tracing::warn!(queue = %name, "pre-settled publish dropped: queue full");
                    }
                    continue;
                }
                next_msg_id += 1;
                ready.push_back(Stored {
                    id: next_msg_id,
                    body,
                    failures: 0,
                });
                // In-memory transient queue: stored == settled. (A durable /
                // quorum queue confirms here only after fsync / Raft commit.)
                if let Some(ack) = ack {
                    let _ = ack.conn.send(ConnCmd::SettleIncoming {
                        channel: ack.channel,
                        handle: ack.handle,
                        binding_gen: ack.binding_gen,
                        delivery_id: ack.delivery_id,
                        accepted: true,
                    });
                }
            }
            QueueMsg::Subscribe {
                conn,
                channel,
                handle,
                binding_gen,
                reply,
            } => {
                next_sub_id += 1;
                subs.push(Subscriber {
                    id: next_sub_id,
                    conn,
                    channel,
                    handle,
                    binding_gen,
                    demand: 0,
                    drain_pending: false,
                });
                let _ = reply.send(next_sub_id);
            }
            QueueMsg::Demand { sub, credit, drain } => {
                if let Some(s) = subs.iter_mut().find(|s| s.id == sub) {
                    s.demand = s.demand.saturating_add(credit);
                    // A drain zeros whatever is left *after* this cycle's
                    // dispatch, so the consumer's "then stop" is honored without
                    // stranding messages that are available right now.
                    s.drain_pending = drain;
                }
            }
            QueueMsg::Settle {
                sub,
                msg_id,
                outcome,
            } => {
                // Only the current owner may settle: a message requeued (by
                // unsubscribe) and redispatched belongs to someone else now,
                // and a late settle from the former owner must not touch it.
                if unacked.get(&msg_id).is_none_or(|(_, owner)| *owner != sub) {
                    continue;
                }
                if let Some((mut stored, _)) = unacked.remove(&msg_id) {
                    match outcome {
                        SettleOutcome::Ack | SettleOutcome::Drop => {}
                        SettleOutcome::Requeue => ready.push_front(stored),
                        SettleOutcome::RequeueFailed => {
                            stored.failures += 1;
                            ready.push_front(stored);
                        }
                    }
                }
            }
            QueueMsg::Unsubscribe { sub } => {
                subs.retain(|s| s.id != sub);
                // Requeue everything that subscriber held, oldest first.
                let mut orphaned: Vec<u64> = unacked
                    .iter()
                    .filter(|(_, (_, owner))| *owner == sub)
                    .map(|(id, _)| *id)
                    .collect();
                orphaned.sort_unstable();
                for id in orphaned.into_iter().rev() {
                    if let Some((stored, _)) = unacked.remove(&id) {
                        ready.push_front(stored);
                    }
                }
            }
        }

        // Dispatch: round-robin over subscribers with demand.
        dispatch(&name, &mut ready, &mut unacked, &mut subs, &mut rr).await;
    }
    tracing::debug!(queue = %name, "queue actor stopped");
}

async fn dispatch(
    name: &str,
    ready: &mut VecDeque<Stored>,
    unacked: &mut HashMap<u64, (Stored, SubId)>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
) {
    while !ready.is_empty() {
        // Find the next subscriber with demand, round-robin.
        let n = subs.len();
        if n == 0 {
            return;
        }
        let mut picked = None;
        for i in 0..n {
            let idx = (*rr + i) % n;
            if subs[idx].demand > 0 {
                picked = Some(idx);
                break;
            }
        }
        let Some(idx) = picked else { return };
        *rr = (idx + 1) % n;

        let msg = ready.pop_front().expect("non-empty");
        let sub = &mut subs[idx];
        sub.demand -= 1;
        let cmd = ConnCmd::Deliver {
            channel: sub.channel,
            handle: sub.handle,
            binding_gen: sub.binding_gen,
            msg_id: msg.id,
            body: msg.body.clone(),
        };
        let sub_id = sub.id;
        if sub.conn.send(cmd).is_ok() {
            unacked.insert(msg.id, (msg, sub_id));
        } else {
            // Connection gone: drop the subscriber, put the message back.
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            ready.push_front(msg);
            subs.retain(|s| s.id != sub_id);
        }
    }

    // Drain completion: a subscriber that asked to drain keeps only the demand
    // it could satisfy from `ready` this cycle; the rest is dropped so it isn't
    // re-armed against messages that arrive later (the consumer said "stop").
    for s in subs.iter_mut() {
        if s.drain_pending {
            s.demand = 0;
            s.drain_pending = false;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn subscribe(
        q: &QueueHandle,
        conn: &mpsc::UnboundedSender<ConnCmd>,
        handle: u32,
    ) -> SubId {
        let (tx, rx) = tokio::sync::oneshot::channel();
        q.tx.send(QueueMsg::Subscribe {
            conn: conn.clone(),
            channel: 0,
            handle,
            binding_gen: 0,
            reply: tx,
        })
        .await
        .unwrap();
        rx.await.unwrap()
    }

    #[tokio::test]
    async fn publish_dispatch_settle_round_trip() {
        let q = spawn("t".into(), 100);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

        let sub = subscribe(&q, &conn_tx, 7).await;
        q.tx.send(QueueMsg::Demand {
            sub,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"m1"),
            ack: None,
        })
        .await
        .unwrap();

        match conn_rx.recv().await.unwrap() {
            ConnCmd::Deliver {
                handle,
                msg_id,
                body,
                ..
            } => {
                assert_eq!(handle, 7);
                assert_eq!(&body[..], b"m1");
                q.tx.send(QueueMsg::Settle {
                    sub,
                    msg_id,
                    outcome: SettleOutcome::Ack,
                })
                .await
                .unwrap();
            }
            other => panic!("expected deliver, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn requeue_redelivers() {
        let q = spawn("t".into(), 100);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        let sub = subscribe(&q, &conn_tx, 1).await;
        q.tx.send(QueueMsg::Demand {
            sub,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"m"),
            ack: None,
        })
        .await
        .unwrap();

        let ConnCmd::Deliver { msg_id, .. } = conn_rx.recv().await.unwrap() else {
            panic!("expected deliver");
        };
        q.tx.send(QueueMsg::Settle {
            sub,
            msg_id,
            outcome: SettleOutcome::Requeue,
        })
        .await
        .unwrap();
        // Redelivered to the same (only) subscriber.
        let ConnCmd::Deliver { msg_id: again, .. } = conn_rx.recv().await.unwrap() else {
            panic!("expected redelivery");
        };
        assert_eq!(again, msg_id);
    }

    /// Demand is an increment, not an absolute set-point: two grants of 1 issued
    /// before any dispatch must let two messages through (an absolute overwrite
    /// would cap at 1 — the over/under-dispatch reconciliation the connection
    /// relies on).
    #[tokio::test]
    async fn demand_grants_are_additive() {
        let q = spawn("t".into(), 100);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        let sub = subscribe(&q, &conn_tx, 1).await;
        q.tx.send(QueueMsg::Demand {
            sub,
            credit: 1,
            drain: false,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Demand {
            sub,
            credit: 1,
            drain: false,
        })
        .await
        .unwrap();
        for i in 0..2u8 {
            q.tx.send(QueueMsg::Publish {
                body: Bytes::copy_from_slice(&[i]),
                ack: None,
            })
            .await
            .unwrap();
        }
        // Both messages come through on the accumulated demand of 2.
        for _ in 0..2 {
            assert!(matches!(
                conn_rx.recv().await.unwrap(),
                ConnCmd::Deliver { .. }
            ));
        }
    }

    /// A drain grant delivers what is available *now*, then drops the unmet
    /// remainder so a later publish is not dispatched against stale credit.
    #[tokio::test]
    async fn drain_drops_unmet_demand() {
        let q = spawn("t".into(), 100);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        let sub = subscribe(&q, &conn_tx, 1).await;
        // One message is ready; drain-grant credit for five.
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"now"),
            ack: None,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Demand {
            sub,
            credit: 5,
            drain: true,
        })
        .await
        .unwrap();
        // The available message is delivered...
        assert!(matches!(
            conn_rx.recv().await.unwrap(),
            ConnCmd::Deliver { .. }
        ));
        // ...but the 4 units of unmet demand were dropped: a later publish is
        // NOT dispatched (the consumer said "then stop").
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"later"),
            ack: None,
        })
        .await
        .unwrap();
        let idle =
            tokio::time::timeout(std::time::Duration::from_millis(100), conn_rx.recv()).await;
        assert!(
            idle.is_err(),
            "drain must not leave demand armed for later messages"
        );
    }

    #[tokio::test]
    async fn competing_consumers_round_robin() {
        let q = spawn("t".into(), 100);
        let (c1_tx, mut c1_rx) = mpsc::unbounded_channel();
        let (c2_tx, mut c2_rx) = mpsc::unbounded_channel();
        let s1 = subscribe(&q, &c1_tx, 1).await;
        let s2 = subscribe(&q, &c2_tx, 2).await;
        q.tx.send(QueueMsg::Demand {
            sub: s1,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Demand {
            sub: s2,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();

        for i in 0..4u8 {
            q.tx.send(QueueMsg::Publish {
                body: Bytes::copy_from_slice(&[i]),
                ack: None,
            })
            .await
            .unwrap();
        }
        // Each consumer gets two of the four.
        let mut got1 = 0;
        let mut got2 = 0;
        for _ in 0..2 {
            assert!(matches!(
                c1_rx.recv().await.unwrap(),
                ConnCmd::Deliver { .. }
            ));
            got1 += 1;
            assert!(matches!(
                c2_rx.recv().await.unwrap(),
                ConnCmd::Deliver { .. }
            ));
            got2 += 1;
        }
        assert_eq!((got1, got2), (2, 2));
    }

    #[tokio::test]
    async fn unsubscribe_requeues_unacked() {
        let q = spawn("t".into(), 100);
        let (c1_tx, mut c1_rx) = mpsc::unbounded_channel();
        let s1 = subscribe(&q, &c1_tx, 1).await;
        q.tx.send(QueueMsg::Demand {
            sub: s1,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"m"),
            ack: None,
        })
        .await
        .unwrap();
        let ConnCmd::Deliver { .. } = c1_rx.recv().await.unwrap() else {
            panic!("expected deliver");
        };

        // Consumer dies without settling; a new consumer gets the message.
        q.tx.send(QueueMsg::Unsubscribe { sub: s1 }).await.unwrap();
        let (c2_tx, mut c2_rx) = mpsc::unbounded_channel();
        let s2 = subscribe(&q, &c2_tx, 2).await;
        q.tx.send(QueueMsg::Demand {
            sub: s2,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        let ConnCmd::Deliver { body, .. } = c2_rx.recv().await.unwrap() else {
            panic!("expected redelivery");
        };
        assert_eq!(&body[..], b"m");
    }

    #[tokio::test]
    async fn overflow_rejects_unsettled_publishes() {
        let q = spawn("t".into(), 1);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();
        // Fill the single slot.
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"a"),
            ack: None,
        })
        .await
        .unwrap();
        // Second publish overflows and is refused.
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"b"),
            ack: Some(PublishAck {
                conn: conn_tx.clone(),
                channel: 3,
                handle: 9,
                binding_gen: 0,
                delivery_id: 42,
            }),
        })
        .await
        .unwrap();
        match conn_rx.recv().await.unwrap() {
            ConnCmd::SettleIncoming {
                delivery_id,
                accepted,
                ..
            } => {
                assert_eq!(delivery_id, 42);
                assert!(!accepted);
            }
            other => panic!("expected settle, got {other:?}"),
        }
    }
}
