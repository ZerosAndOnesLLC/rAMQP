//! The durable-local queue actor: the [`crate::queue`] mailbox protocol
//! backed by the on-disk [`crate::store`] instead of a `VecDeque`.
//!
//! Same shape as the quorum actor — a publish is confirmed only once its
//! batch **commits (fsyncs) to disk**, dispatch reads from an in-memory
//! ready-set of ids seeded by a recovery scan, and bodies are fetched from
//! the store at dispatch time. Restart recovery is exactly that seed: every
//! message on disk that is not settled becomes dispatchable again
//! (at-least-once, like a consumer crash).
//!
//! Hot-path shape (broker.md §3): publishes pipeline through the store's
//! group-commit writer (one fsync per burst, across all durable queues);
//! the actor never blocks on a commit round trip.

use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{mpsc, oneshot};

use crate::dispatch::{Subscriber, complete_drains, confirm_publish, pick_ready, refuse_publish};
use crate::policy::{EffectivePolicy, now_ms};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};
use crate::store::{Store, StoreOp};

/// A pipelined commit resolution: the publish's ack + whether it fsynced.
struct Committed {
    ack: Option<PublishAck>,
    msg_id: u64,
    enqueued_ms: u64,
    len: usize,
    ok: bool,
}

type CommitFuture = Pin<Box<dyn Future<Output = Committed> + Send>>;

/// Spawn a durable queue actor over the node store. Recovery (the scan)
/// happens before the first message is served.
pub(crate) fn spawn(
    name: String,
    store: Store,
    queue_id: u64,
    policy: EffectivePolicy,
) -> Result<QueueHandle, String> {
    let recovered = store.scan(queue_id)?;
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, store, queue_id, recovered, rx, policy));
    Ok(handle)
}

async fn run(
    name: String,
    store: Store,
    queue_id: u64,
    recovered: Vec<(u64, u32, u64, usize)>,
    mut rx: mpsc::Receiver<QueueMsg>,
    policy: EffectivePolicy,
) {
    let mut subs: Vec<Subscriber> = Vec::new();
    // In-flight → (owner, enqueue time, body length) so a requeue re-arms
    // lazy TTL and settles release the byte budget.
    let mut inflight: HashMap<u64, (SubId, u64, usize)> = HashMap::new();
    // Dispatchable ids in FIFO (id) order → (enqueue time, body length),
    // seeded by the recovery scan.
    let mut ready: BTreeMap<u64, (u64, usize)> = recovered
        .iter()
        .map(|(id, _, ms, len)| (*id, (*ms, *len)))
        .collect();
    // Failed-attempt counters (only entries with failures).
    let mut failures: HashMap<u64, u32> = recovered
        .iter()
        .filter(|(_, f, _, _)| *f > 0)
        .map(|(id, f, _, _)| (*id, *f))
        .collect();
    let mut next_msg_id: u64 = recovered.iter().map(|(id, _, _, _)| *id).max().unwrap_or(0);
    // Bytes of message bodies held (ready + inflight + pending publishes).
    let mut bytes: usize = recovered.iter().map(|(_, _, _, len)| *len).sum();
    let mut commits: FuturesUnordered<CommitFuture> = FuturesUnordered::new();
    let mut publishes_pending: usize = 0;
    // Capacity slots held for transaction commits.
    let mut reserved: usize = 0;
    let mut next_sub_id: SubId = 0;
    let mut rr: usize = 0;
    let mut mailbox_open = true;

    if !ready.is_empty() {
        tracing::info!(queue = %name, recovered = ready.len(), "durable queue recovered");
    }

    while mailbox_open || !commits.is_empty() {
        tokio::select! {
            msg = rx.recv(), if mailbox_open => {
                let Some(msg) = msg else {
                    mailbox_open = false;
                    continue;
                };
                let from_reserved = matches!(msg, QueueMsg::PublishReserved { .. });
                match msg {
                    QueueMsg::Publish { body, ack } | QueueMsg::PublishReserved { body, ack } => {
                        if from_reserved {
                            reserved = reserved.saturating_sub(1);
                        }
                        // At a bound (depth or bytes): drop-head makes room
                        // (dead-lettering displaced messages); otherwise
                        // refuse.
                        let over = |ready_len: usize, bytes_now: usize| {
                            ready_len + inflight.len() + publishes_pending + reserved
                                >= policy.max_len
                                || bytes_now.saturating_add(body.len()) > policy.max_bytes
                        };
                        let mut needs_room = over(ready.len(), bytes);
                        if needs_room && policy.drop_head {
                            while needs_room {
                                let Some((old_id, (_, old_len))) = ready.pop_first() else {
                                    break;
                                };
                                bytes = bytes.saturating_sub(old_len);
                                failures.remove(&old_id);
                                dead_letter_then_remove(
                                    &name, &policy, &store, queue_id, old_id, "maxlen",
                                )
                                .await;
                                needs_room = over(ready.len(), bytes);
                            }
                        }
                        // A reserved publish is never refused for depth: its
                        // slot was admitted at Reserve time.
                        if needs_room && !from_reserved {
                            refuse_publish(&name, ack);
                            continue;
                        }
                        next_msg_id += 1;
                        let msg_id = next_msg_id;
                        publishes_pending += 1;
                        let len = body.len();
                        bytes = bytes.saturating_add(len);
                        let enqueued_ms = now_ms();
                        let (done_tx, done_rx) = oneshot::channel();
                        // Bounded writer channel: a saturated disk
                        // backpressures the producer through this await.
                        let submitted = store
                            .submit(StoreOp::Insert {
                                queue: queue_id,
                                msg_id,
                                enqueued_ms,
                                body,
                                done: done_tx,
                            })
                            .await
                            .is_ok();
                        commits.push(Box::pin(async move {
                            let ok = submitted && done_rx.await.unwrap_or(false);
                            Committed { ack, msg_id, enqueued_ms, len, ok }
                        }));
                    }
                    QueueMsg::Reserve { count, reply } => {
                        let ok = policy.drop_head
                            || ready.len()
                                + inflight.len()
                                + publishes_pending
                                + reserved
                                + count as usize
                                <= policy.max_len;
                        if ok && !policy.drop_head {
                            reserved += count as usize;
                        }
                        let _ = reply.send(ok);
                    }
                    QueueMsg::Unreserve { count } => {
                        reserved = reserved.saturating_sub(count as usize);
                    }
                    QueueMsg::Subscribe { conn, channel, handle, binding_gen, reply } => {
                        next_sub_id += 1;
                        subs.push(Subscriber::new(next_sub_id, conn, channel, handle, binding_gen));
                        let _ = reply.send(next_sub_id);
                    }
                    QueueMsg::Demand { sub, credit, drain } => {
                        if let Some(s) = subs.iter_mut().find(|s| s.id == sub) {
                            s.grant(credit, drain);
                        }
                    }
                    QueueMsg::Settle { sub, msg_id, outcome } => {
                        if inflight.get(&msg_id).map(|(owner, _, _)| owner) != Some(&sub) {
                            continue;
                        }
                        let Some((_, enqueue_time, len)) = inflight.remove(&msg_id) else {
                            continue;
                        };
                        match outcome {
                            SettleOutcome::Ack | SettleOutcome::Drop => {
                                failures.remove(&msg_id);
                                bytes = bytes.saturating_sub(len);
                                let _ = store
                                    .submit(StoreOp::Remove { queue: queue_id, msg_id })
                                    .await;
                            }
                            SettleOutcome::Requeue => {
                                ready.insert(msg_id, (enqueue_time, len));
                            }
                            SettleOutcome::RequeueFailed => {
                                let count = failures.entry(msg_id).or_insert(0);
                                *count += 1;
                                if policy.attempts_exhausted(*count) {
                                    failures.remove(&msg_id);
                                    bytes = bytes.saturating_sub(len);
                                    dead_letter_then_remove(
                                        &name,
                                        &policy,
                                        &store,
                                        queue_id,
                                        msg_id,
                                        "delivery-limit",
                                    )
                                    .await;
                                } else {
                                    ready.insert(msg_id, (enqueue_time, len));
                                    let _ = store
                                        .submit(StoreOp::Fail { queue: queue_id, msg_id })
                                        .await;
                                }
                            }
                        }
                    }
                    QueueMsg::Stats { reply } => {
                        let _ = reply.send(crate::queue::QueueStats {
                            ready: ready.len(),
                            unacked: inflight.len(),
                            consumers: subs.len(),
                        });
                    }
                    QueueMsg::Unsubscribe { sub } => {
                        subs.retain(|s| s.id != sub);
                        let mut requeued: Vec<(u64, u64, usize)> = Vec::new();
                        inflight.retain(|msg_id, (owner, ms, len)| {
                            if *owner == sub {
                                requeued.push((*msg_id, *ms, *len));
                                false
                            } else {
                                true
                            }
                        });
                        for (id, ms, len) in requeued {
                            ready.insert(id, (ms, len));
                        }
                    }
                }
            }
            Some(done) = commits.next() => {
                publishes_pending -= 1;
                if done.ok {
                    ready.insert(done.msg_id, (done.enqueued_ms, done.len));
                    confirm_publish(done.ack);
                } else {
                    bytes = bytes.saturating_sub(done.len);
                    tracing::warn!(queue = %name, "durable publish not committed");
                    refuse_publish(&name, done.ack);
                }
            }
        }

        dispatch(
            &name,
            &policy,
            &store,
            queue_id,
            &mut inflight,
            &mut ready,
            &mut failures,
            &mut subs,
            &mut rr,
            &mut bytes,
        )
        .await;
    }
    tracing::debug!(queue = %name, "durable queue actor stopped");
}

/// Dead-letter a stored message, then durably remove the source copy only
/// after the dead-letter router resolves the copy's fate (MED-12): removing
/// first opens a crash window in which a previously-confirmed durable
/// message vanishes from both the source and the DLQ (the Remove batch
/// fsyncs while the copy still rides an in-memory channel). Deferring the
/// remove means a crash instead redelivers — and re-dead-letters — the
/// message: at-least-once, never silent loss.
async fn dead_letter_then_remove(
    name: &str,
    policy: &EffectivePolicy,
    store: &Store,
    queue_id: u64,
    msg_id: u64,
    reason: &str,
) {
    let confirm = store
        .body(queue_id, msg_id)
        .and_then(|body| policy.dead_letter_ordered(name, reason, body));
    match confirm {
        Some(resolved) => {
            let store = store.clone();
            tokio::spawn(async move {
                let _ = resolved.await;
                let _ = store
                    .submit(StoreOp::Remove { queue: queue_id, msg_id })
                    .await;
            });
        }
        // Nothing rode to a DLQ: no ordering to keep.
        None => {
            let _ = store
                .submit(StoreOp::Remove { queue: queue_id, msg_id })
                .await;
        }
    }
}

/// Round-robin dispatch; bodies come off the store's read path.
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    name: &str,
    policy: &EffectivePolicy,
    store: &Store,
    queue_id: u64,
    inflight: &mut HashMap<u64, (SubId, u64, usize)>,
    ready: &mut BTreeMap<u64, (u64, usize)>,
    failures: &mut HashMap<u64, u32>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
    bytes: &mut usize,
) {
    // Lazy TTL: expire from the head before dispatching.
    if policy.ttl_ms.is_some() {
        let now = now_ms();
        while let Some((&id, &(ms, len))) = ready.first_key_value() {
            if !policy.expired(ms, now) {
                break;
            }
            ready.remove(&id);
            failures.remove(&id);
            *bytes = bytes.saturating_sub(len);
            dead_letter_then_remove(name, policy, store, queue_id, id, "expired").await;
        }
    }
    while !ready.is_empty() {
        let Some(idx) = pick_ready(subs, *rr) else {
            return;
        };

        let (&msg_id, &(enqueued_ms, len)) = ready.first_key_value().expect("non-empty");
        let Some(body) = store.body(queue_id, msg_id) else {
            // Removed under us (settled remove racing a reinsert): drop the
            // stale id and continue.
            ready.remove(&msg_id);
            continue;
        };
        ready.remove(&msg_id);
        *rr = (idx + 1) % subs.len();

        let sub = &mut subs[idx];
        sub.demand -= 1;
        let sub_id = sub.id;
        let cmd = ConnCmd::Deliver {
            channel: sub.channel,
            handle: sub.handle,
            binding_gen: sub.binding_gen,
            msg_id,
            body,
        };
        if sub.conn.send(cmd).is_ok() {
            inflight.insert(msg_id, (sub_id, enqueued_ms, len));
        } else {
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            ready.insert(msg_id, (enqueued_ms, len));
            // Requeue everything ELSE the dead subscriber held (MED-9).
            let mut orphaned: Vec<(u64, u64, usize)> = Vec::new();
            inflight.retain(|id, (owner, ms, l)| {
                if *owner == sub_id {
                    orphaned.push((*id, *ms, *l));
                    false
                } else {
                    true
                }
            });
            for (id, ms, l) in orphaned {
                ready.insert(id, (ms, l));
            }
            subs.retain(|s| s.id != sub_id);
        }
    }

    complete_drains(subs);
}
