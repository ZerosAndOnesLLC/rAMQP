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

use crate::dispatch::{Subscriber, complete_drains, confirm_publish, pick_ready, refuse_publish};
use crate::policy::{EffectivePolicy, now_ms};

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
    /// Store a message into a slot previously held by [`QueueMsg::Reserve`]
    /// (the transaction-commit path): consumes one reserved slot and is never
    /// refused for capacity — admission was decided at reserve time.
    PublishReserved {
        /// The raw message bytes as received (all sections).
        body: Bytes,
        /// Where to confirm; `None` for pre-settled.
        ack: Option<PublishAck>,
    },
    /// Atomically reserve `count` capacity slots (transaction commit phase 1).
    /// Reserved slots count against the depth bound for ordinary publishes and
    /// are consumed by [`QueueMsg::PublishReserved`] or released by
    /// [`QueueMsg::Unreserve`]. Replies `false` when the queue cannot hold
    /// `count` more messages (drop-head queues always accept — they make room).
    Reserve {
        /// Slots requested.
        count: u32,
        /// `true` — reserved; `false` — refused (commit must roll back).
        reply: tokio::sync::oneshot::Sender<bool>,
    },
    /// Release reserved slots that will not be published (commit abort).
    Unreserve {
        /// Slots to release.
        count: u32,
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
    /// Report queue statistics (management surface; never on the hot path).
    Stats {
        /// Where to reply.
        reply: tokio::sync::oneshot::Sender<QueueStats>,
    },
}

/// A point-in-time queue statistics snapshot.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct QueueStats {
    /// Messages ready to dispatch.
    pub ready: usize,
    /// Messages dispatched, awaiting settlement.
    pub unacked: usize,
    /// Attached consumers.
    pub consumers: usize,
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
    /// Enqueue time (ms since epoch), for lazy TTL expiry.
    enqueued_ms: u64,
}

/// Spawn a queue actor; the returned handle is its only address.
pub(crate) fn spawn(name: String, policy: EffectivePolicy) -> QueueHandle {
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, rx, policy));
    handle
}

async fn run(name: String, mut rx: mpsc::Receiver<QueueMsg>, policy: EffectivePolicy) {
    let mut ready: VecDeque<Stored> = VecDeque::new();
    let mut unacked: HashMap<u64, (Stored, SubId)> = HashMap::new();
    let mut subs: Vec<Subscriber> = Vec::new();
    // Capacity slots held for transaction commits (counted against the depth
    // bound; consumed by PublishReserved, released by Unreserve).
    let mut reserved: usize = 0;
    // Bytes of message bodies currently held (ready + unacked): the RAM
    // bound — the depth bound alone admits depth × max-message-size.
    let mut bytes: usize = 0;
    let mut next_msg_id: u64 = 0;
    let mut next_sub_id: SubId = 0;
    let mut rr: usize = 0;

    while let Some(msg) = rx.recv().await {
        let from_reserved = matches!(msg, QueueMsg::PublishReserved { .. });
        match msg {
            QueueMsg::Publish { body, ack } | QueueMsg::PublishReserved { body, ack } => {
                if from_reserved {
                    reserved = reserved.saturating_sub(1);
                }
                // At a bound (depth or bytes): drop-head makes room
                // (dead-lettering displaced messages); otherwise refuse —
                // never grow without bound (broker.md §3.2).
                let over = |ready_len: usize, unacked_len: usize, bytes_now: usize| {
                    ready_len + unacked_len + reserved >= policy.max_len
                        || bytes_now.saturating_add(body.len()) > policy.max_bytes
                };
                let mut needs_room = over(ready.len(), unacked.len(), bytes);
                if needs_room && policy.drop_head {
                    while needs_room {
                        let Some(old) = ready.pop_front() else { break };
                        bytes = bytes.saturating_sub(old.body.len());
                        policy.dead_letter(&name, "maxlen", old.body);
                        needs_room = over(ready.len(), unacked.len(), bytes);
                    }
                }
                // A reserved publish is never refused for depth: its slot was
                // admitted at Reserve time (`needs_room` can only persist
                // here if the reservation was lost to a failover, or unacked
                // messages pin the byte budget).
                if needs_room && !from_reserved {
                    refuse_publish(&name, ack);
                    continue;
                }
                next_msg_id += 1;
                bytes = bytes.saturating_add(body.len());
                ready.push_back(Stored {
                    id: next_msg_id,
                    body,
                    failures: 0,
                    enqueued_ms: now_ms(),
                });
                // In-memory transient queue: stored == settled. (A durable /
                // quorum queue confirms here only after fsync / Raft commit.)
                confirm_publish(ack);
            }
            QueueMsg::Reserve { count, reply } => {
                // Drop-head queues always accept (a publish makes its own
                // room), so their slots are never actually held.
                let ok = policy.drop_head
                    || ready.len() + unacked.len() + reserved + count as usize <= policy.max_len;
                if ok && !policy.drop_head {
                    reserved += count as usize;
                }
                let _ = reply.send(ok);
            }
            QueueMsg::Unreserve { count } => {
                reserved = reserved.saturating_sub(count as usize);
            }
            QueueMsg::Subscribe {
                conn,
                channel,
                handle,
                binding_gen,
                reply,
            } => {
                next_sub_id += 1;
                subs.push(Subscriber::new(
                    next_sub_id,
                    conn,
                    channel,
                    handle,
                    binding_gen,
                ));
                let _ = reply.send(next_sub_id);
            }
            QueueMsg::Demand { sub, credit, drain } => {
                if let Some(s) = subs.iter_mut().find(|s| s.id == sub) {
                    // A drain zeros whatever is left *after* this cycle's
                    // dispatch, so the consumer's "then stop" is honored without
                    // stranding messages that are available right now.
                    s.grant(credit, drain);
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
                        SettleOutcome::Ack | SettleOutcome::Drop => {
                            bytes = bytes.saturating_sub(stored.body.len());
                        }
                        SettleOutcome::Requeue => ready.push_front(stored),
                        SettleOutcome::RequeueFailed => {
                            stored.failures += 1;
                            if policy.attempts_exhausted(stored.failures) {
                                bytes = bytes.saturating_sub(stored.body.len());
                                policy.dead_letter(&name, "delivery-limit", stored.body);
                            } else {
                                ready.push_front(stored);
                            }
                        }
                    }
                }
            }
            QueueMsg::Stats { reply } => {
                let _ = reply.send(QueueStats {
                    ready: ready.len(),
                    unacked: unacked.len(),
                    consumers: subs.len(),
                });
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
        dispatch(
            &name,
            &policy,
            &mut ready,
            &mut unacked,
            &mut subs,
            &mut rr,
            &mut bytes,
        )
        .await;
    }
    tracing::debug!(queue = %name, "queue actor stopped");
}

#[allow(clippy::too_many_arguments)]
async fn dispatch(
    name: &str,
    policy: &EffectivePolicy,
    ready: &mut VecDeque<Stored>,
    unacked: &mut HashMap<u64, (Stored, SubId)>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
    bytes: &mut usize,
) {
    // Lazy TTL: expire from the head before dispatching (RabbitMQ-classic
    // semantics — an expired message leaves when it reaches the front).
    if policy.ttl_ms.is_some() {
        let now = now_ms();
        while ready
            .front()
            .is_some_and(|m| policy.expired(m.enqueued_ms, now))
        {
            let expired = ready.pop_front().expect("non-empty");
            *bytes = bytes.saturating_sub(expired.body.len());
            policy.dead_letter(name, "expired", expired.body);
        }
    }
    while !ready.is_empty() {
        // Next subscriber with demand (round-robin); stop if none want work.
        let Some(idx) = pick_ready(subs, *rr) else {
            return;
        };
        *rr = (idx + 1) % subs.len();

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
            // Connection gone: drop the subscriber, put the message back —
            // and requeue everything ELSE it held (without a clean
            // Unsubscribe those in-flights would strand forever, MED-9).
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            ready.push_front(msg);
            let mut orphaned: Vec<u64> = unacked
                .iter()
                .filter(|(_, (_, owner))| *owner == sub_id)
                .map(|(id, _)| *id)
                .collect();
            orphaned.sort_unstable();
            for id in orphaned.into_iter().rev() {
                if let Some((stored, _)) = unacked.remove(&id) {
                    ready.push_front(stored);
                }
            }
            subs.retain(|s| s.id != sub_id);
        }
    }

    complete_drains(subs);
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
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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

    /// MED-9 (issue #19): a subscriber whose connection channel drops
    /// WITHOUT a clean Unsubscribe must have ALL its in-flight messages
    /// requeued when the dead channel is detected — not just the one whose
    /// send failed.
    #[tokio::test]
    async fn dead_subscriber_requeues_all_its_inflights() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
        let (c1_tx, mut c1_rx) = mpsc::unbounded_channel();
        let s1 = subscribe(&q, &c1_tx, 1).await;
        q.tx.send(QueueMsg::Demand {
            sub: s1,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        // Two messages go in flight to s1.
        for b in [b"m1".as_slice(), b"m2"] {
            q.tx.send(QueueMsg::Publish {
                body: Bytes::copy_from_slice(b),
                ack: None,
            })
            .await
            .unwrap();
        }
        for _ in 0..2 {
            assert!(matches!(
                c1_rx.recv().await.unwrap(),
                ConnCmd::Deliver { .. }
            ));
        }
        // The consumer's channel dies with NO Unsubscribe; a third publish
        // makes the actor detect it (the send fails).
        drop(c1_rx);
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"m3"),
            ack: None,
        })
        .await
        .unwrap();

        // A fresh consumer must receive ALL THREE messages.
        let (c2_tx, mut c2_rx) = mpsc::unbounded_channel();
        let s2 = subscribe(&q, &c2_tx, 2).await;
        q.tx.send(QueueMsg::Demand {
            sub: s2,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        let mut got = Vec::new();
        for _ in 0..3 {
            let cmd = tokio::time::timeout(std::time::Duration::from_secs(5), c2_rx.recv())
                .await
                .expect("all in-flights requeued (m1/m2 were stranded before the fix)")
                .expect("delivery");
            if let ConnCmd::Deliver { body, .. } = cmd {
                got.push(body);
            }
        }
        got.sort();
        assert_eq!(got, vec!["m1", "m2", "m3"]);
    }

    #[tokio::test]
    async fn unsubscribe_requeues_unacked() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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

    /// The settle-owner rule: once a message is requeued and redispatched to a
    /// new subscriber, a late settle from the *former* owner must be ignored —
    /// otherwise a stale ack would drop a message the new owner still holds.
    #[tokio::test]
    async fn stale_settle_from_former_owner_is_ignored() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(100));
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
        let ConnCmd::Deliver { msg_id, .. } = c1_rx.recv().await.unwrap() else {
            panic!("expected deliver");
        };

        // s1 drops; the message requeues and is redispatched to s2 (new owner).
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
        let ConnCmd::Deliver { msg_id: m2, .. } = c2_rx.recv().await.unwrap() else {
            panic!("expected redelivery to s2");
        };
        assert_eq!(m2, msg_id, "same message redelivered");

        // A stale Ack from s1 (the former owner) must NOT remove it from s2.
        q.tx.send(QueueMsg::Settle {
            sub: s1,
            msg_id,
            outcome: SettleOutcome::Ack,
        })
        .await
        .unwrap();

        // Proof it still belongs to s2: dropping s2 requeues it, and a third
        // consumer receives it again. (A stale-ack drop would lose it here.)
        q.tx.send(QueueMsg::Unsubscribe { sub: s2 }).await.unwrap();
        let (c3_tx, mut c3_rx) = mpsc::unbounded_channel();
        let s3 = subscribe(&q, &c3_tx, 3).await;
        q.tx.send(QueueMsg::Demand {
            sub: s3,
            credit: 10,
            drain: false,
        })
        .await
        .unwrap();
        let ConnCmd::Deliver { msg_id: m3, .. } = c3_rx.recv().await.unwrap() else {
            panic!("message was lost to the stale ack");
        };
        assert_eq!(m3, msg_id, "message survived the stale former-owner ack");
    }

    async fn reserve(q: &QueueHandle, count: u32) -> bool {
        let (tx, rx) = tokio::sync::oneshot::channel();
        q.tx.send(QueueMsg::Reserve { count, reply: tx })
            .await
            .unwrap();
        rx.await.unwrap()
    }

    /// Reserved slots are held against ordinary publishes and consumed by
    /// `PublishReserved` (the transaction-commit two-phase protocol).
    #[tokio::test]
    async fn reserve_holds_capacity_and_publish_reserved_consumes_it() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(2));
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

        assert!(reserve(&q, 1).await, "one of two slots reserved");
        // First ordinary publish takes the remaining free slot...
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"a"),
            ack: None,
        })
        .await
        .unwrap();
        // ...the second must be refused: the reserved slot is not available.
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"b"),
            ack: Some(PublishAck {
                conn: conn_tx.clone(),
                channel: 0,
                handle: 0,
                binding_gen: 0,
                delivery_id: 1,
            }),
        })
        .await
        .unwrap();
        match conn_rx.recv().await.unwrap() {
            ConnCmd::SettleIncoming { accepted, .. } => assert!(!accepted, "slot was reserved"),
            other => panic!("expected refusal, got {other:?}"),
        }
        // The reserved publish lands in its held slot.
        q.tx.send(QueueMsg::PublishReserved {
            body: Bytes::from_static(b"c"),
            ack: Some(PublishAck {
                conn: conn_tx.clone(),
                channel: 0,
                handle: 0,
                binding_gen: 0,
                delivery_id: 2,
            }),
        })
        .await
        .unwrap();
        match conn_rx.recv().await.unwrap() {
            ConnCmd::SettleIncoming { accepted, .. } => assert!(accepted, "reserved slot consumed"),
            other => panic!("expected confirm, got {other:?}"),
        }
    }

    /// A reservation beyond capacity is refused; releasing one restores the
    /// slot to ordinary publishes.
    #[tokio::test]
    async fn reserve_refuses_beyond_capacity_and_unreserve_releases() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(1));
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

        assert!(reserve(&q, 1).await, "empty queue reserves one");
        assert!(!reserve(&q, 1).await, "second reservation exceeds capacity");
        q.tx.send(QueueMsg::Unreserve { count: 1 }).await.unwrap();
        // The released slot admits an ordinary publish again.
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(b"a"),
            ack: Some(PublishAck {
                conn: conn_tx.clone(),
                channel: 0,
                handle: 0,
                binding_gen: 0,
                delivery_id: 7,
            }),
        })
        .await
        .unwrap();
        match conn_rx.recv().await.unwrap() {
            ConnCmd::SettleIncoming { accepted, .. } => assert!(accepted),
            other => panic!("expected confirm, got {other:?}"),
        }
    }

    async fn try_publish(
        q: &QueueHandle,
        conn: &mpsc::UnboundedSender<ConnCmd>,
        rx: &mut mpsc::UnboundedReceiver<ConnCmd>,
        body: &'static [u8],
    ) -> bool {
        q.tx.send(QueueMsg::Publish {
            body: Bytes::from_static(body),
            ack: Some(PublishAck {
                conn: conn.clone(),
                channel: 0,
                handle: 0,
                binding_gen: 0,
                delivery_id: 0,
            }),
        })
        .await
        .unwrap();
        match rx.recv().await.unwrap() {
            ConnCmd::SettleIncoming { accepted, .. } => accepted,
            other => panic!("expected settle, got {other:?}"),
        }
    }

    /// MED-6 (issue #19): the byte bound refuses publishes the depth bound
    /// alone would admit — the depth cap × max-message-size admitted
    /// terabytes.
    #[tokio::test]
    async fn byte_bound_refuses_oversized_backlog() {
        let mut policy = EffectivePolicy::depth_only(100);
        policy.max_bytes = 10;
        let q = spawn("t".into(), policy);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

        assert!(try_publish(&q, &conn_tx, &mut conn_rx, b"12345678").await);
        assert!(
            !try_publish(&q, &conn_tx, &mut conn_rx, b"1234").await,
            "4 more bytes exceed the 10-byte bound despite depth room"
        );
        assert!(
            try_publish(&q, &conn_tx, &mut conn_rx, b"12").await,
            "2 bytes still fit"
        );
    }

    /// Drop-head honors the byte bound too: it evicts enough of the oldest
    /// ready messages to admit the new one.
    #[tokio::test]
    async fn byte_bound_drop_head_evicts_to_fit() {
        let mut policy = EffectivePolicy::depth_only(100);
        policy.max_bytes = 10;
        policy.drop_head = true;
        let q = spawn("t".into(), policy);
        let (conn_tx, mut conn_rx) = mpsc::unbounded_channel();

        assert!(try_publish(&q, &conn_tx, &mut conn_rx, b"aaaa").await);
        assert!(try_publish(&q, &conn_tx, &mut conn_rx, b"bbbb").await);
        // 8 bytes held; 8 more need BOTH evicted.
        assert!(try_publish(&q, &conn_tx, &mut conn_rx, b"cccccccc").await);
        let (tx, rx) = tokio::sync::oneshot::channel();
        q.tx.send(QueueMsg::Stats { reply: tx }).await.unwrap();
        assert_eq!(rx.await.unwrap().ready, 1, "both older messages evicted");
    }

    #[tokio::test]
    async fn overflow_rejects_unsettled_publishes() {
        let q = spawn("t".into(), EffectivePolicy::depth_only(1));
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
