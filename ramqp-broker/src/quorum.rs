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
//! Slice status (broker.md Phase 6): groups are single-replica here (the
//! multi-node placement + forwarding fabric is the next slice), and the
//! JSON/copy encoding of the queue log is control-plane-grade — the binary
//! codec arrives with the multi-raft manager before this path is benchmarked.

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::cluster::queue_group::{QueueCommand, QueueRaft, QueueResponse, QueueStore};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};

#[derive(Debug)]
struct Subscriber {
    id: SubId,
    conn: mpsc::UnboundedSender<ConnCmd>,
    channel: u16,
    handle: u32,
    demand: u32,
}

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
    let mut next_sub_id: SubId = 0;
    let mut rr: usize = 0;

    while let Some(msg) = rx.recv().await {
        match msg {
            QueueMsg::Publish { body, ack } => {
                // Depth bound checked against applied state (approximate
                // under concurrency, exact enough for a resource cap).
                let depth = store.with_state(|s| s.messages.len());
                if depth >= max_depth {
                    refuse(&name, ack);
                    continue;
                }
                // Commit through the log; the disposition IS the replicated
                // confirm. client_write resolves only once applied.
                let committed = raft
                    .client_write(QueueCommand::Enqueue {
                        body: body.to_vec(),
                    })
                    .await;
                match committed {
                    Ok(resp) if matches!(resp.data, QueueResponse::Enqueued { .. }) => {
                        if let Some(ack) = ack {
                            let _ = ack.conn.send(ConnCmd::SettleIncoming {
                                channel: ack.channel,
                                handle: ack.handle,
                                delivery_id: ack.delivery_id,
                                accepted: true,
                            });
                        }
                    }
                    Ok(other) => {
                        tracing::warn!(queue = %name, ?other.data, "unexpected enqueue response");
                        refuse(&name, ack);
                    }
                    Err(e) => {
                        tracing::warn!(queue = %name, error = %e, "enqueue not committed");
                        refuse(&name, ack);
                    }
                }
            }
            QueueMsg::Subscribe {
                conn,
                channel,
                handle,
                reply,
            } => {
                next_sub_id += 1;
                subs.push(Subscriber {
                    id: next_sub_id,
                    conn,
                    channel,
                    handle,
                    demand: 0,
                });
                let _ = reply.send(next_sub_id);
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
                    continue;
                }
                inflight.remove(&msg_id);
                match outcome {
                    SettleOutcome::Ack | SettleOutcome::Drop => {
                        // Remove from the replicated store.
                        if let Err(e) = raft
                            .client_write(QueueCommand::Settle {
                                msg_id,
                                requeue: false,
                            })
                            .await
                        {
                            // Not committed: the message stays replicated and
                            // becomes dispatchable again — at-least-once.
                            tracing::warn!(queue = %name, error = %e, "settle not committed");
                        }
                    }
                    SettleOutcome::Requeue => {
                        // Released: back to dispatchable, no failure penalty,
                        // nothing to commit (the message never left the SM).
                    }
                    SettleOutcome::RequeueFailed => {
                        if let Err(e) = raft
                            .client_write(QueueCommand::Settle {
                                msg_id,
                                requeue: true,
                            })
                            .await
                        {
                            tracing::warn!(queue = %name, error = %e, "requeue not committed");
                        }
                    }
                }
            }
            QueueMsg::Unsubscribe { sub } => {
                subs.retain(|s| s.id != sub);
                // Everything that subscriber held becomes dispatchable again.
                inflight.retain(|_, owner| *owner != sub);
            }
        }

        dispatch(&name, &store, &mut inflight, &mut subs, &mut rr);
    }
    tracing::debug!(queue = %name, "quorum queue actor stopped");
}

fn refuse(name: &str, ack: Option<PublishAck>) {
    if let Some(ack) = ack {
        let _ = ack.conn.send(ConnCmd::SettleIncoming {
            channel: ack.channel,
            handle: ack.handle,
            delivery_id: ack.delivery_id,
            accepted: false,
        });
    } else {
        tracing::warn!(queue = %name, "pre-settled publish dropped: not committed/full");
    }
}

/// Round-robin dispatch from applied state: the oldest message not in flight
/// goes to the next subscriber with demand.
fn dispatch(
    name: &str,
    store: &QueueStore,
    inflight: &mut HashMap<u64, SubId>,
    subs: &mut Vec<Subscriber>,
    rr: &mut usize,
) {
    loop {
        if subs.is_empty() {
            return;
        }
        // Next dispatchable message (oldest id not in flight). Linear scan of
        // applied state per dispatch — correctness-first; a dispatch cursor
        // comes with the perf pass on this path.
        let next = store.with_state(|s| {
            s.messages
                .iter()
                .find(|(id, _)| !inflight.contains_key(id))
                .map(|(id, m)| (*id, Bytes::from(m.body.clone())))
        });
        let Some((msg_id, body)) = next else { return };

        let n = subs.len();
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

        let sub = &mut subs[idx];
        sub.demand -= 1;
        let sub_id = sub.id;
        let cmd = ConnCmd::Deliver {
            channel: sub.channel,
            handle: sub.handle,
            msg_id,
            body,
        };
        if sub.conn.send(cmd).is_ok() {
            inflight.insert(msg_id, sub_id);
        } else {
            tracing::debug!(queue = %name, sub = sub_id, "subscriber connection closed");
            subs.retain(|s| s.id != sub_id);
        }
    }
}
