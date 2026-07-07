//! The quorum queue actor: the [`crate::queue`] mailbox protocol backed by a
//! per-queue Raft group instead of a local `VecDeque`.
//!
//! Same `QueueMsg` contract as the transient actor — the connection driver
//! cannot tell them apart — but a publish is acknowledged only once the
//! enqueue is **committed to the group's log** (that acknowledgment is the
//! replicated-durability confirm), and dispatch reads from the applied state
//! machine. Dispatch bookkeeping (which subscriber holds which message) is
//! leader-local: on failover or unsubscribe, unsettled messages simply become
//! dispatchable again (at-least-once).
//!
//! Hot-path shape (broker.md §3):
//! - **Pipelined commits** — publishes and settles are proposed concurrently
//!   (`FuturesUnordered`), never serializing the actor on a commit round
//!   trip; Raft applies them in proposal order, and the dispatch ready-set is
//!   ordered by message id, so FIFO holds.
//! - **Ready-set dispatch** — an ordered set of dispatchable ids maintained
//!   incrementally (O(log n)), rebuilt from applied state only at actor
//!   start; no per-dispatch scan of the store.
//! - **`Bytes` bodies** — refcount clones from ingest through the replicated
//!   state machine to dispatch; no copies on the single-replica path.
//!
//! Slice status (broker.md Phase 6): groups are single-replica here — the
//! multi-node placement + forwarding fabric is the next slice.

use std::collections::{BTreeSet, HashMap};
use std::pin::Pin;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::mpsc;

use crate::cluster::queue_group::{QueueCommand, QueueRaft, QueueResponse, QueueStore};
use crate::dispatch::{Subscriber, complete_drains, confirm_publish, pick_ready, refuse_publish};
use crate::policy::{EffectivePolicy, now_ms};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};

/// The resolution of a pipelined commit.
enum Committed {
    /// An enqueue proposal finished (successfully or not).
    Publish {
        ack: Option<PublishAck>,
        result: Result<u64, String>,
    },
    /// An ack/drop removal proposal finished.
    Remove {
        msg_id: u64,
        result: Result<(), String>,
    },
    /// A failure-count proposal finished (best-effort).
    CountFailure { result: Result<(), String> },
}

type CommitFuture = Pin<Box<dyn Future<Output = Committed> + Send>>;

/// Spawn a quorum queue actor over an already-running queue group.
///
/// With `exit_on_demotion`, the actor exits the moment this node stops
/// leading the group — a follower must never dispatch (its reads would be
/// stale and its settles could not commit). The proxy layer detects the
/// closed mailbox and rebinds to the new leader, which redelivers every
/// unsettled message from applied state (at-least-once). Single-replica
/// unclustered groups pass `false`: they can never be demoted.
pub(crate) fn spawn(
    name: String,
    raft: QueueRaft,
    store: QueueStore,
    policy: EffectivePolicy,
    exit_on_demotion: bool,
) -> QueueHandle {
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, raft, store, rx, policy, exit_on_demotion));
    handle
}

async fn run(
    name: String,
    raft: QueueRaft,
    store: QueueStore,
    mut rx: mpsc::Receiver<QueueMsg>,
    policy: EffectivePolicy,
    exit_on_demotion: bool,
) {
    let mut subs: Vec<Subscriber> = Vec::new();
    // Which subscriber holds each in-flight (dispatched, unsettled) message.
    let mut inflight: HashMap<u64, SubId> = HashMap::new();
    // Dispatchable message ids, in FIFO (id) order. Maintained incrementally;
    // seeded from applied state so a takeover/restart redelivers everything
    // not currently held.
    let mut ready: BTreeSet<u64> = store.with_state(|s| s.messages.keys().copied().collect());
    // In-flight commit proposals (publishes + settles), pipelined.
    let mut commits: FuturesUnordered<CommitFuture> = FuturesUnordered::new();
    let mut publishes_pending: usize = 0;
    let mut next_sub_id: SubId = 0;
    let mut rr: usize = 0;
    let mut mailbox_open = true;
    let mut metrics = raft.metrics();
    let self_id = metrics.borrow().id;

    while mailbox_open || !commits.is_empty() {
        tokio::select! {
            changed = metrics.changed(), if exit_on_demotion => {
                let leader = metrics.borrow().current_leader;
                if changed.is_err() || leader != Some(self_id) {
                    // Demoted (or the Raft core stopped): stop immediately.
                    // Dropped commit futures leave their publishes
                    // unconfirmed — the proxy retries them against the new
                    // leader; unsettled dispatches redeliver from state.
                    tracing::info!(queue = %name, ?leader, "leadership lost; quorum actor exiting");
                    return;
                }
            }

            msg = rx.recv(), if mailbox_open => {
                let Some(msg) = msg else {
                    // Mailbox closed: stop accepting, drain outstanding commits.
                    mailbox_open = false;
                    continue;
                };
                handle_msg(
                    msg,
                    &name,
                    &policy,
                    &raft,
                    &store,
                    &mut subs,
                    &mut inflight,
                    &mut ready,
                    &mut commits,
                    &mut publishes_pending,
                    &mut next_sub_id,
                );
            }
            Some(done) = commits.next() => {
                match done {
                    Committed::Publish { ack, result } => {
                        publishes_pending -= 1;
                        match result {
                            Ok(msg_id) => {
                                // Committed to the Raft log: confirm to the producer.
                                ready.insert(msg_id);
                                confirm_publish(ack);
                            }
                            Err(e) => {
                                tracing::warn!(queue = %name, error = %e, "enqueue not committed");
                                refuse_publish(&name, ack);
                            }
                        }
                    }
                    Committed::Remove { msg_id, result } => {
                        if let Err(e) = result {
                            // The message is still replicated: make it
                            // dispatchable again (at-least-once).
                            tracing::warn!(queue = %name, error = %e, "settle not committed");
                            ready.insert(msg_id);
                        }
                    }
                    Committed::CountFailure { result } => {
                        if let Err(e) = result {
                            tracing::warn!(queue = %name, error = %e, "requeue count not committed");
                        }
                    }
                }
            }
        }

        expire_head(&name, &policy, &raft, &store, &mut ready, &mut commits);
        dispatch(&name, &store, &mut inflight, &mut ready, &mut subs, &mut rr);
    }
    tracing::debug!(queue = %name, "quorum queue actor stopped");
}

#[allow(clippy::too_many_arguments)]
fn handle_msg(
    msg: QueueMsg,
    name: &str,
    policy: &EffectivePolicy,
    raft: &QueueRaft,
    store: &QueueStore,
    subs: &mut Vec<Subscriber>,
    inflight: &mut HashMap<u64, SubId>,
    ready: &mut BTreeSet<u64>,
    commits: &mut FuturesUnordered<CommitFuture>,
    publishes_pending: &mut usize,
    next_sub_id: &mut SubId,
) {
    match msg {
        QueueMsg::Publish { body, ack } => {
            // Depth bound counts everything stored or about to be.
            if ready.len() + inflight.len() + *publishes_pending >= policy.max_len {
                // Drop-head makes room (dead-lettering the displaced
                // message); otherwise refuse.
                let dropped = policy.drop_head.then(|| ready.pop_first()).flatten();
                match dropped {
                    Some(old_id) => {
                        dead_letter_stored(name, policy, store, old_id, "maxlen");
                        push_remove(commits, raft, old_id);
                    }
                    None => {
                        refuse_publish(name, ack);
                        return;
                    }
                }
            }
            *publishes_pending += 1;
            let raft = raft.clone();
            let enqueued_ms = now_ms();
            commits.push(Box::pin(async move {
                let result = match raft
                    .client_write(QueueCommand::Enqueue { body, enqueued_ms })
                    .await
                {
                    Ok(resp) => match resp.data {
                        QueueResponse::Enqueued { msg_id } => Ok(msg_id),
                        other => Err(format!("unexpected enqueue response: {other:?}")),
                    },
                    Err(e) => Err(e.to_string()),
                };
                Committed::Publish { ack, result }
            }));
        }
        QueueMsg::Subscribe {
            conn,
            channel,
            handle,
            binding_gen,
            reply,
        } => {
            *next_sub_id += 1;
            subs.push(Subscriber::new(
                *next_sub_id,
                conn,
                channel,
                handle,
                binding_gen,
            ));
            let _ = reply.send(*next_sub_id);
        }
        QueueMsg::Demand { sub, credit, drain } => {
            if let Some(s) = subs.iter_mut().find(|s| s.id == sub) {
                s.grant(credit, drain);
            }
        }
        QueueMsg::Settle {
            sub,
            msg_id,
            outcome,
        } => {
            // Only the current owner may settle (same rule as transient).
            if inflight.get(&msg_id) != Some(&sub) {
                return;
            }
            inflight.remove(&msg_id);
            match outcome {
                SettleOutcome::Ack | SettleOutcome::Drop => {
                    let raft = raft.clone();
                    commits.push(Box::pin(async move {
                        let result = raft
                            .client_write(QueueCommand::Settle {
                                msg_id,
                                requeue: false,
                            })
                            .await
                            .map(|_| ())
                            .map_err(|e| e.to_string());
                        Committed::Remove { msg_id, result }
                    }));
                }
                SettleOutcome::Requeue => {
                    // Released: dispatchable again, no penalty, no log write
                    // (the message never left the state machine).
                    ready.insert(msg_id);
                }
                SettleOutcome::RequeueFailed => {
                    // Out of retries → dead-letter (or drop) instead of
                    // requeueing. Failures come from applied state; this
                    // attempt is the +1.
                    let failed = store
                        .with_state(|s| s.meta_of(msg_id).map(|(_, f)| f))
                        .unwrap_or(0)
                        + 1;
                    if policy.attempts_exhausted(failed) {
                        dead_letter_stored(name, policy, store, msg_id, "delivery-limit");
                        push_remove(commits, raft, msg_id);
                    } else {
                        ready.insert(msg_id);
                        let raft = raft.clone();
                        commits.push(Box::pin(async move {
                            let result = raft
                                .client_write(QueueCommand::Settle {
                                    msg_id,
                                    requeue: true,
                                })
                                .await
                                .map(|_| ())
                                .map_err(|e| e.to_string());
                            Committed::CountFailure { result }
                        }));
                    }
                }
            }
        }
        QueueMsg::Unsubscribe { sub } => {
            subs.retain(|s| s.id != sub);
            // Everything that subscriber held becomes dispatchable again.
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

/// Propose an ack-style removal (dead-lettered / expired / dropped-head).
fn push_remove(commits: &mut FuturesUnordered<CommitFuture>, raft: &QueueRaft, msg_id: u64) {
    let raft = raft.clone();
    commits.push(Box::pin(async move {
        let result = raft
            .client_write(QueueCommand::Settle {
                msg_id,
                requeue: false,
            })
            .await
            .map(|_| ())
            .map_err(|e| e.to_string());
        Committed::Remove { msg_id, result }
    }));
}

/// Read a message's body out of applied state and hand it to the
/// dead-letter router.
fn dead_letter_stored(
    name: &str,
    policy: &EffectivePolicy,
    store: &QueueStore,
    msg_id: u64,
    reason: &str,
) {
    let body = match store.with_state(|s| s.body_of(msg_id)) {
        Some(crate::cluster::queue_group::BodyFetch::Ready(bytes)) => Some(bytes),
        Some(crate::cluster::queue_group::BodyFetch::Spilled(spill, r)) => spill.read(&r).ok(),
        None => None,
    };
    if let Some(body) = body {
        policy.dead_letter(name, reason, body);
    }
}

/// Lazy TTL: expire ready messages from the head (dead-letter + propose
/// removal) before dispatching.
fn expire_head(
    name: &str,
    policy: &EffectivePolicy,
    raft: &QueueRaft,
    store: &QueueStore,
    ready: &mut BTreeSet<u64>,
    commits: &mut FuturesUnordered<CommitFuture>,
) {
    if policy.ttl_ms.is_none() {
        return;
    }
    let now = now_ms();
    while let Some(&head) = ready.first() {
        let expired = store
            .with_state(|s| s.meta_of(head).map(|(ms, _)| policy.expired(ms, now)))
            .unwrap_or(true);
        if !expired {
            break;
        }
        ready.remove(&head);
        dead_letter_stored(name, policy, store, head, "expired");
        push_remove(commits, raft, head);
    }
}

/// Round-robin dispatch: the oldest ready message to the next subscriber with
/// demand. Bodies come out of applied state as refcount clones.
fn dispatch(
    name: &str,
    store: &QueueStore,
    inflight: &mut HashMap<u64, SubId>,
    ready: &mut BTreeSet<u64>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
) {
    while !ready.is_empty() {
        // Next subscriber with demand (round-robin); stop if none want work.
        let Some(idx) = pick_ready(subs, *rr) else {
            return;
        };

        let msg_id = *ready.first().expect("non-empty");
        let Some(fetch) = store.with_state(|s| s.body_of(msg_id)) else {
            // Removed from the state machine under us (settled remove that
            // raced a reinsert): drop the stale id and continue.
            ready.remove(&msg_id);
            continue;
        };
        // Spilled bodies are read outside the store lock (paged deep queues).
        let body = match fetch {
            crate::cluster::queue_group::BodyFetch::Ready(bytes) => bytes,
            crate::cluster::queue_group::BodyFetch::Spilled(spill, r) => match spill.read(&r) {
                Ok(bytes) => bytes,
                Err(e) => {
                    tracing::warn!(queue = %name, msg_id, error = %e, "spilled body unreadable; skipping");
                    ready.remove(&msg_id);
                    continue;
                }
            },
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
