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
    recovered: Vec<(u64, u32, u64)>,
    mut rx: mpsc::Receiver<QueueMsg>,
    policy: EffectivePolicy,
) {
    let mut subs: Vec<Subscriber> = Vec::new();
    // In-flight → (owner, enqueue time) so a requeue re-arms lazy TTL.
    let mut inflight: HashMap<u64, (SubId, u64)> = HashMap::new();
    // Dispatchable ids in FIFO (id) order → enqueue time (for lazy TTL),
    // seeded by the recovery scan.
    let mut ready: BTreeMap<u64, u64> = recovered.iter().map(|(id, _, ms)| (*id, *ms)).collect();
    // Failed-attempt counters (only entries with failures).
    let mut failures: HashMap<u64, u32> = recovered
        .iter()
        .filter(|(_, f, _)| *f > 0)
        .map(|(id, f, _)| (*id, *f))
        .collect();
    let mut next_msg_id: u64 = recovered.iter().map(|(id, _, _)| *id).max().unwrap_or(0);
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
                        if ready.len() + inflight.len() + publishes_pending + reserved
                            >= policy.max_len
                        {
                            // Drop-head makes room (dead-lettering the
                            // displaced message); otherwise refuse.
                            let dropped = policy
                                .drop_head
                                .then(|| ready.pop_first())
                                .flatten();
                            match dropped {
                                Some((old_id, _)) => {
                                    if let Some(old) = store.body(queue_id, old_id) {
                                        policy.dead_letter(&name, "maxlen", old);
                                    }
                                    failures.remove(&old_id);
                                    let _ = store
                                        .submit(StoreOp::Remove { queue: queue_id, msg_id: old_id })
                                        .await;
                                }
                                // A reserved publish is never refused: its
                                // slot was admitted at Reserve time.
                                None if from_reserved => {}
                                None => {
                                    refuse_publish(&name, ack);
                                    continue;
                                }
                            }
                        }
                        next_msg_id += 1;
                        let msg_id = next_msg_id;
                        publishes_pending += 1;
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
                            Committed { ack, msg_id, enqueued_ms, ok }
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
                        if inflight.get(&msg_id).map(|(owner, _)| owner) != Some(&sub) {
                            continue;
                        }
                        let enqueue_time = inflight.remove(&msg_id).map(|(_, ms)| ms);
                        match outcome {
                            SettleOutcome::Ack | SettleOutcome::Drop => {
                                failures.remove(&msg_id);
                                let _ = store
                                    .submit(StoreOp::Remove { queue: queue_id, msg_id })
                                    .await;
                            }
                            SettleOutcome::Requeue => {
                                ready.insert(msg_id, enqueue_time.unwrap_or_else(now_ms));
                            }
                            SettleOutcome::RequeueFailed => {
                                let count = failures.entry(msg_id).or_insert(0);
                                *count += 1;
                                if policy.attempts_exhausted(*count) {
                                    failures.remove(&msg_id);
                                    if let Some(body) = store.body(queue_id, msg_id) {
                                        policy.dead_letter(&name, "delivery-limit", body);
                                    }
                                    let _ = store
                                        .submit(StoreOp::Remove { queue: queue_id, msg_id })
                                        .await;
                                } else {
                                    ready.insert(msg_id, enqueue_time.unwrap_or_else(now_ms));
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
                        let mut requeued: Vec<(u64, u64)> = Vec::new();
                        inflight.retain(|msg_id, (owner, ms)| {
                            if *owner == sub {
                                requeued.push((*msg_id, *ms));
                                false
                            } else {
                                true
                            }
                        });
                        for (id, ms) in requeued {
                            ready.insert(id, ms);
                        }
                    }
                }
            }
            Some(done) = commits.next() => {
                publishes_pending -= 1;
                if done.ok {
                    ready.insert(done.msg_id, done.enqueued_ms);
                    confirm_publish(done.ack);
                } else {
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
        )
        .await;
    }
    tracing::debug!(queue = %name, "durable queue actor stopped");
}

/// Round-robin dispatch; bodies come off the store's read path.
#[allow(clippy::too_many_arguments)]
async fn dispatch(
    name: &str,
    policy: &EffectivePolicy,
    store: &Store,
    queue_id: u64,
    inflight: &mut HashMap<u64, (SubId, u64)>,
    ready: &mut BTreeMap<u64, u64>,
    failures: &mut HashMap<u64, u32>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
) {
    // Lazy TTL: expire from the head before dispatching.
    if policy.ttl_ms.is_some() {
        let now = now_ms();
        while let Some((&id, &ms)) = ready.first_key_value() {
            if !policy.expired(ms, now) {
                break;
            }
            ready.remove(&id);
            failures.remove(&id);
            if let Some(body) = store.body(queue_id, id) {
                policy.dead_letter(name, "expired", body);
            }
            let _ = store
                .submit(StoreOp::Remove {
                    queue: queue_id,
                    msg_id: id,
                })
                .await;
        }
    }
    while !ready.is_empty() {
        let Some(idx) = pick_ready(subs, *rr) else {
            return;
        };

        let (&msg_id, &enqueued_ms) = ready.first_key_value().expect("non-empty");
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
            inflight.insert(msg_id, (sub_id, enqueued_ms));
        } else {
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            ready.insert(msg_id, enqueued_ms);
            subs.retain(|s| s.id != sub_id);
        }
    }

    complete_drains(subs);
}
