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
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};

#[derive(Debug)]
struct Subscriber {
    id: SubId,
    conn: mpsc::UnboundedSender<ConnCmd>,
    channel: u16,
    handle: u32,
    binding_gen: u64,
    demand: u32,
}

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
pub(crate) fn spawn(
    name: String,
    raft: QueueRaft,
    store: QueueStore,
    max_depth: usize,
) -> QueueHandle {
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(run(name, raft, store, rx, max_depth));
    handle
}

async fn run(
    name: String,
    raft: QueueRaft,
    store: QueueStore,
    mut rx: mpsc::Receiver<QueueMsg>,
    max_depth: usize,
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

    while mailbox_open || !commits.is_empty() {
        tokio::select! {
            msg = rx.recv(), if mailbox_open => {
                let Some(msg) = msg else {
                    // Mailbox closed: stop accepting, drain outstanding commits.
                    mailbox_open = false;
                    continue;
                };
                handle_msg(
                    msg,
                    &name,
                    &raft,
                    &mut subs,
                    &mut inflight,
                    &mut ready,
                    &mut commits,
                    &mut publishes_pending,
                    &mut next_sub_id,
                    max_depth,
                );
            }
            Some(done) = commits.next() => {
                match done {
                    Committed::Publish { ack, result } => {
                        publishes_pending -= 1;
                        match result {
                            Ok(msg_id) => {
                                ready.insert(msg_id);
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
                            Err(e) => {
                                tracing::warn!(queue = %name, error = %e, "enqueue not committed");
                                refuse(&name, ack);
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

        dispatch(&name, &store, &mut inflight, &mut ready, &mut subs, &mut rr);
    }
    tracing::debug!(queue = %name, "quorum queue actor stopped");
}

#[allow(clippy::too_many_arguments)]
fn handle_msg(
    msg: QueueMsg,
    name: &str,
    raft: &QueueRaft,
    subs: &mut Vec<Subscriber>,
    inflight: &mut HashMap<u64, SubId>,
    ready: &mut BTreeSet<u64>,
    commits: &mut FuturesUnordered<CommitFuture>,
    publishes_pending: &mut usize,
    next_sub_id: &mut SubId,
    max_depth: usize,
) {
    match msg {
        QueueMsg::Publish { body, ack } => {
            // Depth bound counts everything stored or about to be.
            if ready.len() + inflight.len() + *publishes_pending >= max_depth {
                refuse(name, ack);
                return;
            }
            *publishes_pending += 1;
            let raft = raft.clone();
            commits.push(Box::pin(async move {
                let result = match raft.client_write(QueueCommand::Enqueue { body }).await {
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
            subs.push(Subscriber {
                id: *next_sub_id,
                conn,
                channel,
                handle,
                binding_gen,
                demand: 0,
            });
            let _ = reply.send(*next_sub_id);
        }
        QueueMsg::Demand { sub, credit } => {
            if let Some(s) = subs.iter_mut().find(|s| s.id == sub) {
                s.demand = credit;
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

fn refuse(name: &str, ack: Option<PublishAck>) {
    if let Some(ack) = ack {
        let _ = ack.conn.send(ConnCmd::SettleIncoming {
            channel: ack.channel,
            handle: ack.handle,
            binding_gen: ack.binding_gen,
            delivery_id: ack.delivery_id,
            accepted: false,
        });
    } else {
        tracing::warn!(queue = %name, "pre-settled publish dropped: not committed/full");
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

        let msg_id = *ready.first().expect("non-empty");
        let Some(body) = store.with_state(|s| s.messages.get(&msg_id).map(|m| m.body.clone()))
        else {
            // Removed from the state machine under us (settled remove that
            // raced a reinsert): drop the stale id and continue.
            ready.remove(&msg_id);
            continue;
        };
        ready.remove(&msg_id);
        *rr = (idx + 1) % n;

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
}
