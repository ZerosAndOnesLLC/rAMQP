//! The remote-queue proxy: a local actor speaking the [`crate::queue`]
//! mailbox protocol for a quorum queue whose leader may live anywhere.
//!
//! This is the origin half of the forwarding fabric (broker.md §8). The
//! connection driver binds links to this proxy exactly as it would to a
//! local queue actor; the proxy resolves the queue group's current leader
//! and forwards — to the leader-local actor when this node leads, or over
//! the fabric otherwise. On failover the proxy is the stable indirection:
//! it re-resolves, re-subscribes its consumers (re-arming their outstanding
//! demand), and retries in-flight publishes, so client links survive a
//! leader death without reattaching.
//!
//! Delivery guarantee: at-least-once. A publish confirmed `accepted` is
//! committed to the group's log; a publish caught mid-failover is retried
//! (and may deliver twice if the first commit's ack was lost — the AMQP
//! at-least-once contract). Message order is FIFO per producer in steady
//! state; retried publishes may reorder across a failover.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use bytes::Bytes;
use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{mpsc, oneshot};

use crate::cluster::fabric::{ConnState, OpenSubError, PublishStatus, RequestKind, SubEvent};
use crate::cluster::node::ClusterNode;
use crate::dispatch::{confirm_publish, refuse_publish};
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};

/// How many times a publish is retried across leader changes before the
/// producer sees `rejected`.
const PUBLISH_ATTEMPTS: u32 = 3;

/// How long the proxy keeps trying to find a leader before giving up and
/// closing (the registry evicts a closed handle and re-declares on the next
/// attach).
const REBIND_DEADLINE: std::time::Duration = std::time::Duration::from_secs(60);

/// Spawn the proxy actor for one quorum queue.
pub(crate) fn spawn(name: String, node: Arc<ClusterNode>) -> QueueHandle {
    let (tx, rx) = mpsc::channel(1024);
    let handle = QueueHandle {
        name: name.clone(),
        tx,
    };
    tokio::spawn(async move {
        Proxy::new(name, node).run(rx).await;
    });
    handle
}

/// Where the queue's traffic currently goes.
enum Downstream {
    /// This node leads the group: the leader-local actor.
    Local { queue: QueueHandle },
    /// Another node leads: the shared fabric connection to it.
    Remote { conn: Arc<ConnState> },
}

/// One consumer bound through this proxy.
struct ProxSub {
    /// The subscriber's real connection command mailbox.
    conn: mpsc::UnboundedSender<ConnCmd>,
    channel: u16,
    handle: u32,
    binding_gen: u64,
    /// Demand granted downstream that has not yet produced a delivery —
    /// re-armed on the new leader after a failover.
    outstanding: u32,
    /// The downstream identity under the current binding.
    down: Option<DownSub>,
}

enum DownSub {
    Local { sub: SubId },
    Remote { sub_chan: u64 },
}

/// The terminal fate of one forwarded publish attempt.
struct PubDone {
    ack: Option<PublishAck>,
    body: Bytes,
    attempt: u32,
    /// The binding epoch this attempt was sent under: a retry rebinds only
    /// when the failed binding is still the current one (so a burst of
    /// failures triggers one rebind, not one per publish).
    epoch: u64,
    /// A transaction-commit publish consuming a reserved slot (retries keep
    /// the flag; a post-failover leader simply admits it — bounded overshoot).
    reserved: bool,
    outcome: PubOutcome,
}

enum PubOutcome {
    Accepted,
    Rejected,
    /// Leadership moved / transport died: worth a retry after a rebind.
    Retry,
}

type PubFuture = Pin<Box<dyn Future<Output = PubDone> + Send>>;

struct Proxy {
    name: String,
    node: Arc<ClusterNode>,
    downstream: Option<Downstream>,
    subs: HashMap<u32, ProxSub>,
    next_sub_key: u32,
    /// Local-mode: the shared channel leader-local deliveries arrive on
    /// (the downstream `handle` field carries our sub key). The sender half
    /// is kept for binding later subscribers; actor death is detected via
    /// `queue.tx.closed()`, not channel closure.
    local_events: Option<mpsc::UnboundedReceiver<ConnCmd>>,
    local_events_tx: Option<mpsc::UnboundedSender<ConnCmd>>,
    /// Remote-mode: fabric subscription events, tagged with `sub_chan`.
    remote_events: Option<mpsc::UnboundedReceiver<(u64, SubEvent)>>,
    remote_events_tx: Option<mpsc::UnboundedSender<(u64, SubEvent)>>,
    /// Remote-mode: `sub_chan` → our sub key.
    remote_chans: HashMap<u64, u32>,
    /// In-flight publish confirmations.
    pubs: FuturesUnordered<PubFuture>,
    /// Local-mode publish acks are tagged with this token so a stale ack
    /// from a previous binding is never misread.
    binding_epoch: u64,
}

impl Proxy {
    fn new(name: String, node: Arc<ClusterNode>) -> Self {
        Proxy {
            name,
            node,
            downstream: None,
            subs: HashMap::new(),
            next_sub_key: 0,
            local_events: None,
            local_events_tx: None,
            remote_events: None,
            remote_events_tx: None,
            remote_chans: HashMap::new(),
            pubs: FuturesUnordered::new(),
            binding_epoch: 0,
        }
    }

    async fn run(mut self, mut rx: mpsc::Receiver<QueueMsg>) {
        // Bind eagerly: the registry declared the queue before spawning us.
        if !self.rebind().await {
            tracing::warn!(queue = %self.name, "proxy could not bind to a leader; closing");
            return;
        }
        loop {
            tokio::select! {
                biased;

                msg = rx.recv() => {
                    let Some(msg) = msg else { break };
                    self.handle_msg(msg).await;
                }

                Some(done) = self.pubs.next() => {
                    self.handle_pub_done(done).await;
                }

                cmd = recv_opt(&mut self.local_events) => {
                    if let Some(cmd) = cmd {
                        self.handle_local_event(cmd);
                    }
                }

                // Leader-local actor exited (leadership lost): rebind.
                _ = local_actor_closed(&self.downstream) => {
                    if !self.rebind().await { break }
                }

                ev = recv_opt(&mut self.remote_events) => {
                    match ev {
                        Some((chan, SubEvent::Deliver { msg_id, body })) => {
                            self.deliver(chan, msg_id, body);
                        }
                        Some((chan, SubEvent::Closed)) => {
                            // One subscription closed (leadership moved): the
                            // whole binding is stale — rebind everything.
                            tracing::debug!(queue = %self.name, chan, "remote subscription closed");
                            if !self.rebind().await { break }
                        }
                        None => {
                            self.remote_events = None;
                            self.remote_events_tx = None;
                            if !self.rebind().await { break }
                        }
                    }
                }
            }
        }
        tracing::debug!(queue = %self.name, "queue proxy stopped");
    }

    /// (Re)resolve the leader and rebuild the downstream binding, migrating
    /// every subscriber and re-arming its outstanding demand.
    async fn rebind(&mut self) -> bool {
        self.downstream = None;
        self.local_events = None;
        self.local_events_tx = None;
        self.remote_events = None;
        self.remote_events_tx = None;
        self.remote_chans.clear();
        self.binding_epoch += 1;
        for sub in self.subs.values_mut() {
            sub.down = None;
        }

        let deadline = tokio::time::Instant::now() + REBIND_DEADLINE;
        let mut backoff = std::time::Duration::from_millis(50);
        loop {
            // A stopping node will never produce a leader again — exit
            // instead of pinning the node (and its store) for the deadline.
            if self.node.is_stopping() {
                return false;
            }
            match self.try_bind().await {
                Ok(()) => return true,
                Err(e) => {
                    tracing::debug!(queue = %self.name, error = %e, "leader bind retry");
                }
            }
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(std::time::Duration::from_secs(1));
        }
    }

    async fn try_bind(&mut self) -> Result<(), String> {
        let leader = self
            .node
            .resolve_queue_leader(&self.name)
            .await
            .ok_or("no leader")?;
        if leader == self.node.node_id {
            let queue = self
                .node
                .leader_actor(&self.name)
                .await
                .map_err(|hint| format!("local leadership raced away (hint {hint:?})"))?;
            let (events_tx, events_rx) = mpsc::unbounded_channel();
            // Subscribe every consumer to the local actor; our sub key rides
            // in the downstream `handle` so one shared channel serves all.
            for (key, sub) in &mut self.subs {
                let (reply_tx, reply_rx) = oneshot::channel();
                queue
                    .tx
                    .send(QueueMsg::Subscribe {
                        conn: events_tx.clone(),
                        channel: 0,
                        handle: *key,
                        binding_gen: self.binding_epoch,
                        reply: reply_tx,
                    })
                    .await
                    .map_err(|_| "actor died mid-bind")?;
                let down = reply_rx.await.map_err(|_| "actor died mid-bind")?;
                sub.down = Some(DownSub::Local { sub: down });
                if sub.outstanding > 0 {
                    let _ = queue
                        .tx
                        .send(QueueMsg::Demand {
                            sub: down,
                            credit: sub.outstanding,
                            drain: false,
                        })
                        .await;
                }
            }
            self.local_events = Some(events_rx);
            self.local_events_tx = Some(events_tx);
            self.downstream = Some(Downstream::Local { queue });
        } else {
            let conn = self.node.peer_conn(leader).await?;
            let (events_tx, events_rx) = mpsc::unbounded_channel();
            for (key, sub) in &mut self.subs {
                let sub_chan = match conn.open_sub(&self.name, events_tx.clone()).await {
                    Ok(chan) => chan,
                    Err(OpenSubError::NotLeader(hint)) => {
                        return Err(format!("stale leader {leader} (hint {hint:?})"));
                    }
                    Err(OpenSubError::Transport(e)) => return Err(e),
                };
                self.remote_chans.insert(sub_chan, *key);
                sub.down = Some(DownSub::Remote { sub_chan });
                if sub.outstanding > 0 {
                    conn.send_demand(sub_chan, sub.outstanding, false);
                }
            }
            self.remote_events = Some(events_rx);
            self.remote_events_tx = Some(events_tx);
            self.downstream = Some(Downstream::Remote { conn });
        }
        tracing::debug!(queue = %self.name, leader, "proxy bound to leader");
        Ok(())
    }

    async fn handle_msg(&mut self, msg: QueueMsg) {
        match msg {
            QueueMsg::Publish { body, ack } => {
                self.publish(body, ack, false, 0).await;
            }
            QueueMsg::PublishReserved { body, ack } => {
                self.publish(body, ack, true, 0).await;
            }
            QueueMsg::Reserve { count, reply } => match &self.downstream {
                Some(Downstream::Local { queue }) => {
                    // Pass through; the leader-local actor replies directly.
                    if let Err(mpsc::error::SendError(QueueMsg::Reserve { reply, .. })) = queue
                        .tx
                        .send(QueueMsg::Reserve { count, reply })
                        .await
                    {
                        let _ = reply.send(false);
                    }
                }
                Some(Downstream::Remote { conn }) => {
                    // Spawned: a fabric round trip must not stall deliveries.
                    let conn = conn.clone();
                    let queue = self.name.clone();
                    tokio::spawn(async move {
                        let ok = match conn
                            .call(RequestKind::Reserve { queue, count }, Bytes::new())
                            .await
                        {
                            Ok(body) => bincode::deserialize::<bool>(&body).unwrap_or(false),
                            Err(_) => false,
                        };
                        let _ = reply.send(ok);
                    });
                }
                None => {
                    let _ = reply.send(false);
                }
            },
            QueueMsg::Unreserve { count } => match &self.downstream {
                Some(Downstream::Local { queue }) => {
                    let _ = queue.tx.send(QueueMsg::Unreserve { count }).await;
                }
                Some(Downstream::Remote { conn }) => {
                    let conn = conn.clone();
                    let queue = self.name.clone();
                    tokio::spawn(async move {
                        let _ = conn
                            .call(RequestKind::Unreserve { queue, count }, Bytes::new())
                            .await;
                    });
                }
                None => {}
            },
            QueueMsg::Subscribe {
                conn,
                channel,
                handle,
                binding_gen,
                reply,
            } => {
                self.next_sub_key += 1;
                let key = self.next_sub_key;
                let mut sub = ProxSub {
                    conn,
                    channel,
                    handle,
                    binding_gen,
                    outstanding: 0,
                    down: None,
                };
                if let Err(e) = self.bind_sub(key, &mut sub).await {
                    tracing::debug!(queue = %self.name, error = %e, "subscribe bind failed; rebinding");
                    self.subs.insert(key, sub);
                    let _ = reply.send(u64::from(key));
                    let _ = self.rebind().await;
                    return;
                }
                self.subs.insert(key, sub);
                let _ = reply.send(u64::from(key));
            }
            QueueMsg::Demand { sub, credit, drain } => {
                let Ok(key) = u32::try_from(sub) else { return };
                let Some(record) = self.subs.get_mut(&key) else {
                    return;
                };
                record.outstanding = record.outstanding.saturating_add(credit);
                match (&record.down, &self.downstream) {
                    (Some(DownSub::Local { sub }), Some(Downstream::Local { queue })) => {
                        let _ = queue
                            .tx
                            .send(QueueMsg::Demand {
                                sub: *sub,
                                credit,
                                drain,
                            })
                            .await;
                    }
                    (Some(DownSub::Remote { sub_chan }), Some(Downstream::Remote { conn, .. })) => {
                        conn.send_demand(*sub_chan, credit, drain);
                    }
                    _ => {} // unbound: outstanding re-arms at the next bind
                }
                if drain {
                    // A drain zeroes unmet demand downstream after dispatch;
                    // mirror that so a failover doesn't resurrect it.
                    record.outstanding = 0;
                }
            }
            QueueMsg::Settle {
                sub,
                msg_id,
                outcome,
            } => {
                let Ok(key) = u32::try_from(sub) else { return };
                let Some(record) = self.subs.get(&key) else {
                    return;
                };
                match (&record.down, &self.downstream) {
                    (Some(DownSub::Local { sub }), Some(Downstream::Local { queue })) => {
                        let _ = queue
                            .tx
                            .send(QueueMsg::Settle {
                                sub: *sub,
                                msg_id,
                                outcome,
                            })
                            .await;
                    }
                    (Some(DownSub::Remote { sub_chan }), Some(Downstream::Remote { conn, .. })) => {
                        conn.send_settle(*sub_chan, msg_id, outcome.into());
                    }
                    // Unbound (mid-failover): drop the settle; the new leader
                    // redelivers the message (at-least-once).
                    _ => {}
                }
            }
            QueueMsg::Stats { reply } => {
                // Local leader: real stats from the actor; remote: only the
                // proxy-side consumer count is known.
                if let Some(Downstream::Local { queue }) = &self.downstream {
                    let _ = queue.tx.send(QueueMsg::Stats { reply }).await;
                } else {
                    let _ = reply.send(crate::queue::QueueStats {
                        ready: 0,
                        unacked: 0,
                        consumers: self.subs.len(),
                    });
                }
            }
            QueueMsg::Unsubscribe { sub } => {
                let Ok(key) = u32::try_from(sub) else { return };
                let Some(record) = self.subs.remove(&key) else {
                    return;
                };
                match (&record.down, &self.downstream) {
                    (Some(DownSub::Local { sub }), Some(Downstream::Local { queue })) => {
                        let _ = queue.tx.send(QueueMsg::Unsubscribe { sub: *sub }).await;
                    }
                    (Some(DownSub::Remote { sub_chan }), Some(Downstream::Remote { conn, .. })) => {
                        self.remote_chans.remove(sub_chan);
                        conn.close_sub(*sub_chan);
                    }
                    _ => {}
                }
            }
        }
    }

    /// Bind one (new) subscriber to the current downstream.
    async fn bind_sub(&mut self, key: u32, sub: &mut ProxSub) -> Result<(), String> {
        match &self.downstream {
            Some(Downstream::Local { queue }) => {
                let events_tx = self
                    .local_events_tx
                    .clone()
                    .ok_or("no local event channel")?;
                let (reply_tx, reply_rx) = oneshot::channel();
                queue
                    .tx
                    .send(QueueMsg::Subscribe {
                        conn: events_tx,
                        channel: 0,
                        handle: key,
                        binding_gen: self.binding_epoch,
                        reply: reply_tx,
                    })
                    .await
                    .map_err(|_| "actor died mid-subscribe")?;
                let down = reply_rx.await.map_err(|_| "actor died mid-subscribe")?;
                sub.down = Some(DownSub::Local { sub: down });
                Ok(())
            }
            Some(Downstream::Remote { conn, .. }) => {
                let events_tx = self
                    .remote_events_tx
                    .clone()
                    .ok_or("no remote event channel")?;
                let sub_chan = match conn.open_sub(&self.name, events_tx).await {
                    Ok(chan) => chan,
                    Err(OpenSubError::NotLeader(_)) => return Err("stale leader".to_owned()),
                    Err(OpenSubError::Transport(e)) => return Err(e),
                };
                self.remote_chans.insert(sub_chan, key);
                sub.down = Some(DownSub::Remote { sub_chan });
                Ok(())
            }
            None => Err("unbound".to_owned()),
        }
    }

    /// Forward one publish downstream (attempt `attempt`). `reserved` marks a
    /// transaction-commit publish consuming a pre-reserved slot.
    async fn publish(&mut self, body: Bytes, ack: Option<PublishAck>, reserved: bool, attempt: u32) {
        match &self.downstream {
            Some(Downstream::Local { queue }) => {
                // Wrap the ack so we observe the outcome (for retries) before
                // forwarding it to the real producer.
                let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<ConnCmd>();
                let wrapped = Some(PublishAck {
                    conn: ack_tx,
                    channel: 0,
                    handle: 0,
                    binding_gen: self.binding_epoch,
                    delivery_id: 0,
                });
                let msg = if reserved {
                    QueueMsg::PublishReserved {
                        body: body.clone(),
                        ack: wrapped,
                    }
                } else {
                    QueueMsg::Publish {
                        body: body.clone(),
                        ack: wrapped,
                    }
                };
                let sent = queue.tx.send(msg).await.is_ok();
                let epoch = self.binding_epoch;
                if !sent {
                    self.pubs.push(Box::pin(async move {
                        PubDone {
                            ack,
                            body,
                            attempt,
                            epoch,
                            reserved,
                            outcome: PubOutcome::Retry,
                        }
                    }));
                    return;
                }
                self.pubs.push(Box::pin(async move {
                    let outcome = match ack_rx.recv().await {
                        Some(ConnCmd::SettleIncoming { accepted: true, .. }) => {
                            PubOutcome::Accepted
                        }
                        Some(ConnCmd::SettleIncoming {
                            accepted: false, ..
                        }) => PubOutcome::Rejected,
                        // Actor died before confirming: retry after rebind.
                        _ => PubOutcome::Retry,
                    };
                    PubDone {
                        ack,
                        body,
                        attempt,
                        epoch,
                        reserved,
                        outcome,
                    }
                }));
            }
            Some(Downstream::Remote { conn, .. }) => {
                let conn = conn.clone();
                let queue = self.name.clone();
                let epoch = self.binding_epoch;
                self.pubs.push(Box::pin(async move {
                    let req = if reserved {
                        RequestKind::PublishReserved { queue }
                    } else {
                        RequestKind::Publish { queue }
                    };
                    let reply = conn.call(req, body.clone()).await;
                    let outcome = match reply.and_then(|b| {
                        bincode::deserialize::<PublishStatus>(&b).map_err(|e| e.to_string())
                    }) {
                        Ok(PublishStatus::Accepted) => PubOutcome::Accepted,
                        Ok(PublishStatus::Rejected) => PubOutcome::Rejected,
                        Ok(PublishStatus::NotLeader(_)) | Err(_) => PubOutcome::Retry,
                    };
                    PubDone {
                        ack,
                        body,
                        attempt,
                        epoch,
                        reserved,
                        outcome,
                    }
                }));
            }
            None => {
                // Unbound and rebind failed earlier: refuse.
                refuse_publish(&self.name, ack);
            }
        }
    }

    async fn handle_pub_done(&mut self, done: PubDone) {
        match done.outcome {
            PubOutcome::Accepted => confirm_publish(done.ack),
            PubOutcome::Rejected => refuse_publish(&self.name, done.ack),
            PubOutcome::Retry => {
                if done.attempt + 1 >= PUBLISH_ATTEMPTS {
                    refuse_publish(&self.name, done.ack);
                    return;
                }
                // Rebind only if the binding this publish failed under is
                // still the current one (covers graceful demotion, where the
                // connection stays healthy but the leadership moved).
                if done.epoch == self.binding_epoch && !self.rebind().await {
                    refuse_publish(&self.name, done.ack);
                    return;
                }
                Box::pin(self.publish(done.body, done.ack, done.reserved, done.attempt + 1)).await;
            }
        }
    }

    /// A delivery from the leader-local actor (local mode): the downstream
    /// `handle` carries our sub key.
    fn handle_local_event(&mut self, cmd: ConnCmd) {
        match cmd {
            ConnCmd::Deliver {
                handle: key,
                msg_id,
                body,
                ..
            } => self.forward_delivery(key, msg_id, body),
            // Publish acks ride per-publish channels, not this one.
            ConnCmd::SettleIncoming { .. } => {}
        }
    }

    /// A delivery from the remote leader (remote mode).
    fn deliver(&mut self, sub_chan: u64, msg_id: u64, body: Bytes) {
        let Some(&key) = self.remote_chans.get(&sub_chan) else {
            return; // stale channel from a previous binding
        };
        self.forward_delivery(key, msg_id, body);
    }

    /// Hand a delivery to the real subscriber connection.
    fn forward_delivery(&mut self, key: u32, msg_id: u64, body: Bytes) {
        let Some(sub) = self.subs.get_mut(&key) else {
            return;
        };
        sub.outstanding = sub.outstanding.saturating_sub(1);
        let cmd = ConnCmd::Deliver {
            channel: sub.channel,
            handle: sub.handle,
            binding_gen: sub.binding_gen,
            msg_id,
            body,
        };
        if sub.conn.send(cmd).is_err() {
            // Subscriber connection gone: requeue downstream + drop the sub.
            tracing::debug!(queue = %self.name, key, "proxied subscriber connection closed");
            let down = self.subs.remove(&key).and_then(|s| s.down);
            match (down, &self.downstream) {
                (Some(DownSub::Local { sub }), Some(Downstream::Local { queue })) => {
                    let tx = queue.tx.clone();
                    tokio::spawn(async move {
                        let _ = tx
                            .send(QueueMsg::Settle {
                                sub,
                                msg_id,
                                outcome: SettleOutcome::Requeue,
                            })
                            .await;
                        let _ = tx.send(QueueMsg::Unsubscribe { sub }).await;
                    });
                }
                (Some(DownSub::Remote { sub_chan }), Some(Downstream::Remote { conn, .. })) => {
                    conn.send_settle(sub_chan, msg_id, SettleOutcome::Requeue.into());
                    self.remote_chans.remove(&sub_chan);
                    conn.close_sub(sub_chan);
                }
                _ => {}
            }
        }
    }
}

/// `recv` on an optional receiver; pends forever when absent (so a `select!`
/// arm stays quiet in the other mode).
async fn recv_opt<T>(rx: &mut Option<mpsc::UnboundedReceiver<T>>) -> Option<T> {
    match rx {
        Some(rx) => rx.recv().await,
        None => std::future::pending().await,
    }
}

/// Resolves when the leader-local actor exits (its mailbox receiver drops);
/// pends forever in remote/unbound mode.
async fn local_actor_closed(down: &Option<Downstream>) {
    match down {
        Some(Downstream::Local { queue }) => queue.tx.closed().await,
        _ => std::future::pending().await,
    }
}
