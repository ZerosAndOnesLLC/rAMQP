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

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{mpsc, oneshot};

use crate::dispatch::{Subscriber, complete_drains, confirm_publish, pick_ready, refuse_publish};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};
use crate::store::{Store, StoreOp};

/// A pipelined commit resolution: the publish's ack + whether it fsynced.
struct Committed {
    ack: Option<PublishAck>,
    msg_id: u64,
    ok: bool,
}

type CommitFuture = Pin<Box<dyn Future<Output = Committed> + Send>>;

/// Spawn a durable queue actor over the node store. Recovery (the scan)
/// happens before the first message is served.
pub(crate) fn spawn(
    name: String,
    store: Store,
    queue_id: u64,
    max_depth: usize,
) -> Result<QueueHandle, String> {
    let recovered = store.scan(queue_id)?;
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, store, queue_id, recovered, rx, max_depth));
    Ok(handle)
}

async fn run(
    name: String,
    store: Store,
    queue_id: u64,
    recovered: Vec<(u64, u32)>,
    mut rx: mpsc::Receiver<QueueMsg>,
    max_depth: usize,
) {
    let mut subs: Vec<Subscriber> = Vec::new();
    let mut inflight: HashMap<u64, SubId> = HashMap::new();
    // Dispatchable ids in FIFO (id) order, seeded by the recovery scan.
    let mut ready: BTreeSet<u64> = recovered.iter().map(|(id, _)| *id).collect();
    let mut next_msg_id: u64 = recovered.iter().map(|(id, _)| *id).max().unwrap_or(0);
    let mut commits: FuturesUnordered<CommitFuture> = FuturesUnordered::new();
    let mut publishes_pending: usize = 0;
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
                match msg {
                    QueueMsg::Publish { body, ack } => {
                        if ready.len() + inflight.len() + publishes_pending >= max_depth {
                            refuse_publish(&name, ack);
                            continue;
                        }
                        next_msg_id += 1;
                        let msg_id = next_msg_id;
                        publishes_pending += 1;
                        let (done_tx, done_rx) = oneshot::channel();
                        // Bounded writer channel: a saturated disk
                        // backpressures the producer through this await.
                        let submitted = store
                            .submit(StoreOp::Insert {
                                queue: queue_id,
                                msg_id,
                                body,
                                done: done_tx,
                            })
                            .await
                            .is_ok();
                        commits.push(Box::pin(async move {
                            let ok = submitted && done_rx.await.unwrap_or(false);
                            Committed { ack, msg_id, ok }
                        }));
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
                        if inflight.get(&msg_id) != Some(&sub) {
                            continue;
                        }
                        inflight.remove(&msg_id);
                        match outcome {
                            SettleOutcome::Ack | SettleOutcome::Drop => {
                                let _ = store
                                    .submit(StoreOp::Remove { queue: queue_id, msg_id })
                                    .await;
                            }
                            SettleOutcome::Requeue => {
                                ready.insert(msg_id);
                            }
                            SettleOutcome::RequeueFailed => {
                                ready.insert(msg_id);
                                let _ = store
                                    .submit(StoreOp::Fail { queue: queue_id, msg_id })
                                    .await;
                            }
                        }
                    }
                    QueueMsg::Unsubscribe { sub } => {
                        subs.retain(|s| s.id != sub);
                        inflight.retain(|msg_id, owner| {
                            if *owner == sub {
                                ready.insert(*msg_id);
                                false
                            } else {
                                true
                            }
                        });
                    }
                }
            }
            Some(done) = commits.next() => {
                publishes_pending -= 1;
                if done.ok {
                    ready.insert(done.msg_id);
                    confirm_publish(done.ack);
                } else {
                    tracing::warn!(queue = %name, "durable publish not committed");
                    refuse_publish(&name, done.ack);
                }
            }
        }

        dispatch(
            &name,
            &store,
            queue_id,
            &mut inflight,
            &mut ready,
            &mut subs,
            &mut rr,
        );
    }
    tracing::debug!(queue = %name, "durable queue actor stopped");
}

/// Round-robin dispatch; bodies come off the store's read path.
fn dispatch(
    name: &str,
    store: &Store,
    queue_id: u64,
    inflight: &mut HashMap<u64, SubId>,
    ready: &mut BTreeSet<u64>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
) {
    while !ready.is_empty() {
        let Some(idx) = pick_ready(subs, *rr) else {
            return;
        };

        let msg_id = *ready.first().expect("non-empty");
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
            inflight.insert(msg_id, sub_id);
        } else {
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            ready.insert(msg_id);
            subs.retain(|s| s.id != sub_id);
        }
    }

    complete_drains(subs);
}
