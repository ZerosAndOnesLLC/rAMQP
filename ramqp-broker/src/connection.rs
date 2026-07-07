//! The per-connection driver: server-order handshake (header → SASL → `open`)
//! and the frame-routing event loop, wired to the queue layer.
//!
//! One owning task per connection (the same lock-free actor model as the
//! client): all protocol state lives here, nothing is shared, and writes are
//! coalesced into one flush per loop iteration. Queues are actors too; the
//! only cross-task traffic is bounded channels — deliveries stay `Bytes`
//! (refcount clones) end to end.

use std::collections::HashMap;
use std::sync::Arc;

use futures_util::StreamExt;
use futures_util::stream::FuturesUnordered;
use tokio::sync::{mpsc, oneshot, watch};

use ramqp_core::codec::Symbol;
use ramqp_core::config::CreditMode;
use ramqp_core::connection::heartbeat::{Heartbeat, HeartbeatAction};
use ramqp_core::connection::mux::{ChannelAllocator, RemoteChannelMap};
use ramqp_core::connection::negotiate::{MIN_MAX_FRAME_SIZE, build_open, reconcile};
use ramqp_core::error::{ConnectError, ErrorKind};
use ramqp_core::ids::{DeliveryId, SessionId};
use ramqp_core::observe::{SharedMetrics, noop_metrics};
use ramqp_core::proto::{LinkEvent, SessionEvent};
use ramqp_core::sasl::server::parse_plain_response;
use ramqp_core::session::state::Session;
use ramqp_core::transport::IoStream;
use ramqp_core::transport::frame::{Frame, FrameBody, FramedTransport};
use ramqp_core::transport::header::{ProtocolHeader, accept as accept_header};
use ramqp_core::types::definitions::{
    AmqpError as AmqpCondition, ConnectionError, Error as AmqpError, ErrorCondition, Role,
};
use ramqp_core::types::messaging::{Accepted, DeliveryState, Rejected, TargetArchetype};
use ramqp_core::types::performatives::{Attach, Begin, Close, End, Performative};
use ramqp_core::types::sasl::{SaslCode, SaslFrame, SaslMechanisms, SaslOutcome};

use ramqp_core::txn::{declared_state, transactional_state, txn_state};

use crate::auth::{Authenticator, Credentials};
use crate::config::BrokerConfig;
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};
use crate::registry::QueueRegistry;
use crate::txn::{StagedPublish, StagedSettle, TxnControl, TxnManager, decode_control};

/// How many queue commands to coalesce under one flush (mirrors the client
/// driver's batching; bounds per-wakeup work so reads aren't starved).
const CMD_BATCH_MAX: usize = 64;

/// Serve one accepted byte stream to completion (handshake + event loop).
pub(crate) async fn serve<S: IoStream>(
    stream: S,
    config: Arc<BrokerConfig>,
    auth: Arc<dyn Authenticator>,
    registry: Arc<QueueRegistry>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectError> {
    // Bound the whole inbound handshake (header + SASL + open) so a client
    // that connects then stalls cannot pin this task (slow-loris guard).
    let handshake = handshake(stream, &config, auth.as_ref(), registry);
    let mut conn = match config.connection.connect_timeout {
        Some(t) => tokio::time::timeout(t, handshake)
            .await
            .map_err(|_| ConnectError::msg(ErrorKind::Timeout, "inbound handshake timed out"))??,
        None => handshake.await?,
    };
    conn.shutdown = Some(shutdown);
    let result = conn.run().await;
    // On a fatal error, tell the peer why with a close{error} before the socket
    // drops (AMQP requires the condition; a bare TCP reset leaves the peer to
    // guess). Clean completion (peer close / shutdown) returns Ok and skips this.
    if let Err(err) = &result {
        conn.close_with_error(err).await;
    }
    conn.cleanup().await;
    result
}

/// Run the server-order handshake, returning the established connection.
async fn handshake<S: IoStream>(
    mut stream: S,
    config: &Arc<BrokerConfig>,
    auth: &dyn Authenticator,
    registry: Arc<QueueRegistry>,
) -> Result<BrokerConnection<S>, ConnectError> {
    // 1. Protocol header, read-first. Offer SASL and (if permitted) bare AMQP.
    let supported: &[ProtocolHeader] = if auth.allow_unauthenticated() {
        &[ProtocolHeader::SASL, ProtocolHeader::AMQP]
    } else {
        &[ProtocolHeader::SASL]
    };
    let chosen = accept_header(&mut stream, supported).await?;

    let mut transport = FramedTransport::new(stream, config.connection.max_frame_size);

    // 2. SASL layer (when chosen): mechanisms → init → outcome → AMQP header.
    if chosen == ProtocolHeader::SASL {
        server_sasl(&mut transport, auth).await?;
        let after = accept_header(transport.stream_mut(), &[ProtocolHeader::AMQP]).await?;
        debug_assert_eq!(after, ProtocolHeader::AMQP);
    }

    // 3. `open` exchange, read-first, mirroring the client's validation.
    let peer_open = loop {
        match transport.read_frame().await?.body {
            FrameBody::Amqp(Performative::Open(o), _) => break o,
            FrameBody::Empty => continue,
            other => {
                return Err(ConnectError::msg(
                    ErrorKind::ProtocolViolation,
                    format!("expected open, got {other:?}"),
                ));
            }
        }
    };
    if peer_open.max_frame_size < MIN_MAX_FRAME_SIZE {
        return Err(ConnectError::msg(
            ErrorKind::ProtocolViolation,
            format!(
                "peer advertised max-frame-size {} below the {MIN_MAX_FRAME_SIZE}-octet minimum",
                peer_open.max_frame_size
            ),
        ));
    }
    let local_open = build_open(&config.connection);
    let negotiated = reconcile(&local_open, &peer_open);
    // max-frame-size is DIRECTIONAL (spec §2.7.1): our advertised value bounds
    // the frames the peer sends US, independent of its own (possibly smaller)
    // receive limit. So inbound decode stays at OUR advertised max; only the
    // OUTBOUND framing below uses the negotiated min (peer's advertised size).
    // A memory-constrained client that advertises 4 KiB may still legally send
    // us a 32 KiB transfer — rejecting it (via the min) would kill a valid link.
    let our_max_frame_size = local_open.max_frame_size;
    transport
        .send_amqp(0, &Performative::Open(local_open), None)
        .await?;
    transport.set_max_frame_size(our_max_frame_size);

    let heartbeat = Heartbeat::new(negotiated.send_interval, negotiated.recv_timeout);
    let (link_events_tx, link_events_rx) = mpsc::channel(1024);
    let (session_events_tx, session_events_rx) = mpsc::unbounded_channel();
    let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

    tracing::debug!(container = %peer_open.container_id, "connection open");
    Ok(BrokerConnection {
        transport,
        config: config.clone(),
        registry,
        max_frame_size: negotiated.max_frame_size as usize,
        heartbeat,
        channels: ChannelAllocator::new(negotiated.channel_max),
        remote_channels: RemoteChannelMap::default(),
        sessions: HashMap::new(),
        discarding: std::collections::HashSet::new(),
        bindings: HashMap::new(),
        next_gen: 0,
        settlements: FuturesUnordered::new(),
        txns: TxnManager::default(),
        txn_results: FuturesUnordered::new(),
        next_session_id: 0,
        metrics: noop_metrics(),
        link_events_tx,
        link_events_rx,
        session_events_tx,
        session_events_rx,
        cmd_tx,
        cmd_rx,
        shutdown: None,
    })
}

/// Server side of SASL: advertise, read `init`, verify, send the outcome.
async fn server_sasl<S: IoStream>(
    transport: &mut FramedTransport<S>,
    auth: &dyn Authenticator,
) -> Result<(), ConnectError> {
    transport
        .send_sasl(&SaslFrame::Mechanisms(SaslMechanisms {
            sasl_server_mechanisms: auth.mechanisms().iter().map(|m| Symbol::new(*m)).collect(),
        }))
        .await?;

    let init = match transport.read_frame().await?.body {
        FrameBody::Sasl(SaslFrame::Init(init)) => init,
        other => {
            return Err(ConnectError::msg(
                ErrorKind::Sasl,
                format!("expected sasl-init, got {other:?}"),
            ));
        }
    };

    let mechanism = init.mechanism.as_str().to_ascii_uppercase();
    let verified = match mechanism.as_str() {
        "ANONYMOUS" if auth.mechanisms().contains(&"ANONYMOUS") => {
            auth.verify(Credentials::Anonymous)
        }
        "PLAIN" if auth.mechanisms().contains(&"PLAIN") => init
            .initial_response
            .as_deref()
            .and_then(parse_plain_response)
            .is_some_and(|(_authzid, authcid, passwd)| {
                auth.verify(Credentials::Plain { authcid, passwd })
            }),
        _ => false,
    };

    let code = if verified {
        SaslCode::Ok
    } else {
        SaslCode::Auth
    };
    transport
        .send_sasl(&SaslFrame::Outcome(SaslOutcome {
            code,
            additional_data: None,
        }))
        .await?;
    if !verified {
        return Err(ConnectError::msg(
            ErrorKind::Sasl,
            format!("authentication failed (mechanism {mechanism})"),
        ));
    }
    Ok(())
}

/// A link's queue binding.
enum Binding {
    /// Peer sender → our receiver → publishes into `queue`.
    Producer {
        queue: QueueHandle,
        /// This binding's generation (see [`Binding::epoch`]).
        epoch: u64,
        /// Publish acks received since the last credit top-up. Producer credit
        /// is replenished from the *ack* path (not the publish path), so the
        /// in-flight publish window — and thus the unbounded queue→connection
        /// command backlog — is bounded by the granted credit: a producer whose
        /// acks are not draining runs out of credit and stops (backpressure).
        acked_since_grant: u32,
    },
    /// Peer sender → the transaction coordinator (declare/discharge).
    Coordinator,
    /// Peer receiver → our sender → subscribed to `queue` as `sub`.
    Consumer {
        queue: QueueHandle,
        sub: SubId,
        /// This binding's generation.
        epoch: u64,
        /// Demand already handed to the queue that has not yet produced a
        /// `Deliver` back (queue-side demand plus dispatches still in `cmd_rx`).
        /// Credit grants forward only the delta above this, so a restated flow
        /// cannot re-arm demand that in-flight deliveries already cover.
        granted: u32,
    },
}

/// How a consumer's terminal state resolves a dispatched message: applied
/// immediately, or staged under a transaction until discharge.
enum SettleAction {
    /// Apply now.
    Now(SettleOutcome),
    /// Stage under this transaction (`transactional-state` disposition).
    Txn(ramqp_core::txn::TxnId, SettleOutcome),
}

/// The resolution of one dispatched delivery: which queue/sub/message it was
/// and how the consumer settled it.
type SettlementResult = (mpsc::Sender<QueueMsg>, SubId, u64, SettleAction);

/// The async half of a transaction discharge (commit): where to report the
/// outcome once every staged operation lands.
struct TxnDone {
    channel: u16,
    handle: u32,
    delivery_id: u32,
    ok: bool,
}

/// An established broker-side connection (post-handshake).
struct BrokerConnection<S: IoStream> {
    transport: FramedTransport<S>,
    config: Arc<BrokerConfig>,
    registry: Arc<QueueRegistry>,
    max_frame_size: usize,
    heartbeat: Heartbeat,
    channels: ChannelAllocator,
    remote_channels: RemoteChannelMap,
    /// Sessions keyed by OUR channel.
    sessions: HashMap<u16, Session>,
    /// Remote channels whose session we ended locally (e.g. a rejected attach)
    /// but whose peer `End` we have not yet seen. Frames pipelined behind our
    /// `End` land here and are silently discarded rather than treated as
    /// frames on an unknown channel (which would kill the whole connection).
    discarding: std::collections::HashSet<u16>,
    /// Link → queue bindings, keyed by (our channel, our link handle).
    bindings: HashMap<(u16, u32), Binding>,
    /// Monotonic binding-generation counter (never reused within a connection),
    /// stamped onto each binding and echoed by queue commands so a command for a
    /// since-replaced `(channel, handle)` is dropped instead of misrouted.
    next_gen: u64,
    /// In-flight consumer dispatches awaiting a terminal outcome.
    settlements: FuturesUnordered<std::pin::Pin<Box<dyn Future<Output = SettlementResult> + Send>>>,
    /// Open transactions (staged work; dropped = rolled back).
    txns: TxnManager,
    /// In-flight transaction commits awaiting their staged work.
    txn_results: FuturesUnordered<std::pin::Pin<Box<dyn Future<Output = TxnDone> + Send>>>,
    next_session_id: u64,
    metrics: SharedMetrics,
    /// Shared event channel for all accepted links; drained synchronously
    /// after every routed frame (emissions only happen inside our own calls,
    /// so the channel never accumulates across frames).
    link_events_tx: mpsc::Sender<LinkEvent>,
    link_events_rx: mpsc::Receiver<LinkEvent>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    session_events_rx: mpsc::UnboundedReceiver<SessionEvent>,
    /// Commands from queue actors (deliveries to dispatch, publish acks).
    /// Unbounded at the channel, bounded by protocol (see `queue.rs` docs on
    /// channel orientation): queues must never await this connection.
    cmd_tx: mpsc::UnboundedSender<ConnCmd>,
    cmd_rx: mpsc::UnboundedReceiver<ConnCmd>,
    shutdown: Option<watch::Receiver<bool>>,
}

impl<S: IoStream> BrokerConnection<S> {
    async fn run(&mut self) -> Result<(), ConnectError> {
        // A shutdown receiver that is inert (pending forever) when absent.
        let mut shutdown = self.shutdown.take();
        loop {
            let shutdown_changed = async {
                match shutdown.as_mut() {
                    Some(rx) => {
                        let _ = rx.changed().await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                biased;

                frame = self.transport.read_frame() => {
                    let frame = frame?;
                    self.heartbeat.record_recv();
                    let done = self.handle_frame(frame).await?;
                    self.flush().await?;
                    if done {
                        return Ok(());
                    }
                }

                Some(cmd) = self.cmd_rx.recv() => {
                    self.handle_cmd(cmd);
                    // Coalesce a burst of queue commands under one flush.
                    let mut drained = 0;
                    while drained < CMD_BATCH_MAX {
                        match self.cmd_rx.try_recv() {
                            Ok(next) => { self.handle_cmd(next); drained += 1; }
                            Err(_) => break,
                        }
                    }
                    self.flush().await?;
                }

                Some(done) = self.txn_results.next() => {
                    if let Some(session) = self.sessions.get_mut(&done.channel) {
                        let state = if done.ok {
                            DeliveryState::Accepted(Accepted::default())
                        } else {
                            DeliveryState::Rejected(Rejected {
                                error: Some(AmqpError::new(
                                    AmqpCondition::InternalError,
                                    Some("transaction failed; rolled back".to_owned()),
                                )),
                            })
                        };
                        session.send_disposition(
                            done.handle,
                            DeliveryId(done.delivery_id),
                            None,
                            state,
                            true,
                            &mut self.transport,
                        );
                    }
                    self.flush().await?;
                }

                Some(result) = self.settlements.next() => {
                    self.forward_settlement(result).await;
                    // Drain everything else already resolved: a burst of
                    // ranged dispositions resolves thousands of settlement
                    // futures at once, and forwarding one per wakeup lets the
                    // (biased, read-preferring) loop starve them — the queue
                    // then thinks acked messages are still in flight, and a
                    // close would requeue them all as duplicates.
                    use futures_util::FutureExt;
                    while let Some(Some(next)) = self.settlements.next().now_or_never() {
                        self.forward_settlement(next).await;
                    }
                }

                Some(event) = self.session_events_rx.recv() => {
                    tracing::trace!(?event, "session event");
                }

                action = self.heartbeat.tick() => {
                    match action {
                        HeartbeatAction::SendEmpty => {
                            self.transport.queue_empty(0);
                            self.flush().await?;
                        }
                        HeartbeatAction::PeerTimedOut => {
                            return Err(ConnectError::msg(
                                ErrorKind::Timeout,
                                "peer exceeded idle-timeout",
                            ));
                        }
                        HeartbeatAction::Idle => {}
                    }
                }

                _ = shutdown_changed => {
                    // Graceful server shutdown: close the connection.
                    self.transport
                        .send_amqp(0, &Performative::Close(Close { error: None }), None)
                        .await?;
                    self.await_peer_close().await;
                    return Ok(());
                }
            }
        }
    }

    /// Best-effort teardown: unsubscribe every consumer binding so its
    /// unacked messages requeue immediately (rather than on the queue's next
    /// failed dispatch).
    async fn cleanup(&mut self) {
        // First forward every settlement outcome that already resolved (the
        // dispositions a client pipelines right before `close`) — dropping
        // them would requeue acked messages and redeliver them to the next
        // consumer as duplicates. Two traps here:
        // - `now_or_never` is unusable: the settlement futures await tokio
        //   oneshots, whose polls consult the task's cooperative budget.
        //   Right after a busy close the budget is exhausted, every poll
        //   reports `Pending` regardless of actual readiness, and a
        //   ready-only drain silently forwards nothing (hundreds of acked
        //   messages requeued per close). `unconstrained` bypasses the
        //   budget.
        // - Some futures may be genuinely unresolved (the peer closed
        //   without settling): the per-item timeout stops the drain there;
        //   whatever remains requeues via the unsubscribes below
        //   (at-least-once, as before).
        self.drain_ready_settlements().await;
        for ((_, _), binding) in self.bindings.drain() {
            if let Binding::Consumer { queue, sub, .. } = binding {
                let _ = queue.tx.send(QueueMsg::Unsubscribe { sub }).await;
            }
        }
    }

    /// Forward every settlement whose outcome has already resolved (see the
    /// cleanup notes on the tokio-coop trap; `unconstrained` + a per-item
    /// timeout so genuinely pending futures stop the drain).
    async fn drain_ready_settlements(&mut self) {
        loop {
            let next = tokio::time::timeout(
                std::time::Duration::from_millis(20),
                tokio::task::unconstrained(self.settlements.next()),
            );
            match next.await {
                Ok(Some(result)) => self.forward_settlement(result).await,
                Ok(None) | Err(_) => break,
            }
        }
    }

    /// Hand one resolved dispatch outcome to its queue.
    async fn forward_settlement(&mut self, result: SettlementResult) {
        let (queue, sub, msg_id, action) = result;
        match action {
            SettleAction::Now(outcome) => {
                let _ = queue
                    .send(QueueMsg::Settle {
                        sub,
                        msg_id,
                        outcome,
                    })
                    .await;
            }
            SettleAction::Txn(txn_id, outcome) => {
                let staged = self.txns.stage_settle(
                    &txn_id,
                    StagedSettle {
                        queue,
                        sub,
                        msg_id,
                        outcome,
                    },
                );
                if !staged {
                    // Unknown txn or cap: leave the message in flight; the
                    // teardown/unsubscribe path requeues it (at-least-once).
                    tracing::warn!(
                        msg_id,
                        "transactional settle not staged (unknown txn or cap)"
                    );
                }
            }
        }
    }

    async fn flush(&mut self) -> Result<(), ConnectError> {
        if self.transport.pending_bytes() > 0 {
            self.transport.flush().await?;
            self.heartbeat.record_send();
        }
        Ok(())
    }

    /// Handle one command from a queue actor. Both commands carry the
    /// generation of the binding they were issued for; a command whose
    /// generation no longer matches the live binding at `(channel, handle)` is
    /// dropped (the `(channel, handle)` was reused by a different link — routing
    /// the stale command would deliver/settle against the wrong queue).
    fn handle_cmd(&mut self, cmd: ConnCmd) {
        match cmd {
            ConnCmd::Deliver {
                channel,
                handle,
                binding_gen,
                msg_id,
                body,
            } => {
                // Validate against the live binding; capture its queue+sub and
                // account this delivery against the demand we handed the queue.
                let (queue_tx, sub) = match self.bindings.get_mut(&(channel, handle)) {
                    Some(Binding::Consumer {
                        queue,
                        sub,
                        epoch,
                        granted,
                    }) if *epoch == binding_gen => {
                        *granted = granted.saturating_sub(1);
                        (queue.tx.clone(), *sub)
                    }
                    // Link detached / reused / gone: the old subscriber's
                    // Unsubscribe already requeued this message on its queue.
                    _ => return,
                };
                let Some(session) = self.sessions.get_mut(&channel) else {
                    // Session raced away: requeue on the validated queue.
                    self.settlements.push(Box::pin(async move {
                        (
                            queue_tx,
                            sub,
                            msg_id,
                            SettleAction::Now(SettleOutcome::Requeue),
                        )
                    }));
                    return;
                };
                let (reply_tx, reply_rx) = oneshot::channel();
                session.send_transfer(
                    handle,
                    body,
                    false,
                    0,
                    None,
                    Some(reply_tx),
                    &mut self.transport,
                    self.max_frame_size,
                );
                self.settlements.push(Box::pin(async move {
                    let action = match reply_rx.await {
                        Ok(Ok(state)) => match txn_state(&state) {
                            // A transactional settlement: stage it under the
                            // transaction (applied/undone at discharge).
                            Some(ts) => SettleAction::Txn(
                                ts.txn_id,
                                ts.outcome
                                    .as_ref()
                                    .map(outcome_to_settle)
                                    .unwrap_or(SettleOutcome::Ack),
                            ),
                            None => SettleAction::Now(state_to_outcome(&state)),
                        },
                        // Link/connection died before a terminal outcome:
                        // requeue (no-op if an unsubscribe already did).
                        Ok(Err(_)) | Err(_) => SettleAction::Now(SettleOutcome::Requeue),
                    };
                    (queue_tx, sub, msg_id, action)
                }));
            }
            ConnCmd::SettleIncoming {
                channel,
                handle,
                binding_gen,
                delivery_id,
                accepted,
            } => {
                // Drop a stale ack for a since-reused producer handle (else it
                // would settle an unrelated delivery-id on the new link).
                let live = matches!(
                    self.bindings.get(&(channel, handle)),
                    Some(Binding::Producer { epoch, .. }) if *epoch == binding_gen
                );
                if !live {
                    return;
                }
                if let Some(session) = self.sessions.get_mut(&channel) {
                    let state = if accepted {
                        DeliveryState::Accepted(Accepted::default())
                    } else {
                        DeliveryState::Rejected(Rejected {
                            error: Some(AmqpError::new(
                                AmqpCondition::ResourceLimitExceeded,
                                Some("queue is full".to_owned()),
                            )),
                        })
                    };
                    session.send_disposition(
                        handle,
                        DeliveryId(delivery_id),
                        None,
                        state,
                        true,
                        &mut self.transport,
                    );
                }
                // Replenish producer credit from the ack path — this bounds the
                // in-flight publish window and the queue→connection backlog.
                self.replenish_producer_credit(channel, handle);
            }
        }
    }

    /// Batched producer-credit replenishment: count one consumed credit against
    /// the producer link and, once half the window is consumed, grant that much
    /// back via a `flow`. Called once per settled/acked publish so credit tracks
    /// throughput without a flow per message.
    fn replenish_producer_credit(&mut self, channel: u16, handle: u32) {
        let threshold = (self.config.initial_credit / 2).max(1);
        let grant = match self.bindings.get_mut(&(channel, handle)) {
            Some(Binding::Producer {
                acked_since_grant, ..
            }) => {
                *acked_since_grant += 1;
                if *acked_since_grant >= threshold {
                    std::mem::take(acked_since_grant)
                } else {
                    0
                }
            }
            _ => 0,
        };
        if grant > 0
            && let Some(session) = self.sessions.get_mut(&channel)
        {
            session.grant_credit(handle, grant, &mut self.transport);
        }
    }

    /// Handle one coordinator control message (`declare` / `discharge`).
    ///
    /// Declares and rollbacks answer synchronously. A commit resolves
    /// asynchronously through `txn_results`: every staged enqueue must be
    /// accepted by its queue (Raft-committed / fsynced for replicated and
    /// durable queues — the coordinator inherits cluster-awareness from the
    /// queue layer) before the discharge is answered `accepted`.
    fn handle_txn_control(
        &mut self,
        channel: u16,
        handle: u32,
        d: ramqp_core::proto::IncomingDelivery,
    ) {
        let delivery_id = d.delivery_id;
        let control = decode_control(&d.message);
        match control {
            Some(TxnControl::Declare) => {
                let state = match self.txns.declare() {
                    Some(txn_id) => declared_state(txn_id),
                    None => DeliveryState::Rejected(Rejected {
                        error: Some(AmqpError::new(
                            AmqpCondition::ResourceLimitExceeded,
                            Some("too many open transactions".to_owned()),
                        )),
                    }),
                };
                if let Some(session) = self.sessions.get_mut(&channel) {
                    session.send_disposition(
                        handle,
                        delivery_id,
                        None,
                        state,
                        true,
                        &mut self.transport,
                    );
                    session.grant_credit(handle, 1, &mut self.transport);
                }
            }
            Some(TxnControl::Discharge { txn_id, fail }) => {
                let Some(txn) = self.txns.take(&txn_id) else {
                    if let Some(session) = self.sessions.get_mut(&channel) {
                        session.send_disposition(
                            handle,
                            delivery_id,
                            None,
                            DeliveryState::Rejected(Rejected {
                                error: Some(AmqpError::new(
                                    AmqpCondition::NotFound,
                                    Some("unknown transaction".to_owned()),
                                )),
                            }),
                            true,
                            &mut self.transport,
                        );
                        session.grant_credit(handle, 1, &mut self.transport);
                    }
                    return;
                };
                let done = TxnDone {
                    channel,
                    handle,
                    delivery_id: delivery_id.value(),
                    ok: true,
                };
                if fail {
                    // Roll back: staged enqueues drop; staged settlements
                    // requeue their (still in-flight) messages.
                    self.txn_results.push(Box::pin(async move {
                        for settle in txn.settles {
                            let _ = settle
                                .queue
                                .send(QueueMsg::Settle {
                                    sub: settle.sub,
                                    msg_id: settle.msg_id,
                                    outcome: SettleOutcome::Requeue,
                                })
                                .await;
                        }
                        done
                    }));
                } else {
                    // Commit: every staged enqueue must land (its queue's own
                    // durability confirm) before the settlements apply and
                    // the discharge is accepted.
                    self.txn_results.push(Box::pin(async move {
                        let mut ok = true;
                        for publish in &txn.publishes {
                            let (ack_tx, mut ack_rx) = mpsc::unbounded_channel::<ConnCmd>();
                            let sent = publish
                                .queue
                                .send(QueueMsg::Publish {
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
                            let accepted = sent
                                && matches!(
                                    ack_rx.recv().await,
                                    Some(ConnCmd::SettleIncoming { accepted: true, .. })
                                );
                            if !accepted {
                                tracing::warn!(
                                    queue = %publish.queue_name,
                                    "transactional publish refused at commit; failing the transaction"
                                );
                                ok = false;
                                break;
                            }
                        }
                        // Settlements: apply on success; requeue on failure
                        // (the consumer's work is undone with the txn).
                        for settle in txn.settles {
                            let outcome = if ok {
                                settle.outcome
                            } else {
                                SettleOutcome::Requeue
                            };
                            let _ = settle
                                .queue
                                .send(QueueMsg::Settle {
                                    sub: settle.sub,
                                    msg_id: settle.msg_id,
                                    outcome,
                                })
                                .await;
                        }
                        TxnDone { ok, ..done }
                    }));
                }
                if let Some(session) = self.sessions.get_mut(&channel) {
                    session.grant_credit(handle, 1, &mut self.transport);
                }
            }
            None => {
                tracing::warn!("undecodable coordinator control message");
                if let Some(session) = self.sessions.get_mut(&channel) {
                    session.send_disposition(
                        handle,
                        delivery_id,
                        None,
                        DeliveryState::Rejected(Rejected {
                            error: Some(AmqpError::new(
                                AmqpCondition::DecodeError,
                                Some("expected declare or discharge".to_owned()),
                            )),
                        }),
                        true,
                        &mut self.transport,
                    );
                    session.grant_credit(handle, 1, &mut self.transport);
                }
            }
        }
    }

    /// Resolve a peer channel to our local channel for an inbound link frame.
    /// `Ok(Some(local))` — process it; `Ok(None)` — silently ignore (a frame
    /// pipelined behind an `End` we already sent for a session we ended);
    /// `Err` — fail the connection (a frame on a channel we never mapped).
    fn resolve_active(&self, remote_channel: u16) -> Result<Option<u16>, ConnectError> {
        match self.remote_channels.resolve(remote_channel) {
            Some(local) => Ok(Some(local)),
            None if self.discarding.contains(&remote_channel) => Ok(None),
            None => Err(unknown_channel(remote_channel)),
        }
    }

    /// Route one inbound frame. Returns `Ok(true)` when the connection is done.
    async fn handle_frame(&mut self, frame: Frame) -> Result<bool, ConnectError> {
        let channel = frame.channel;
        let (performative, payload) = match frame.body {
            FrameBody::Empty => return Ok(false),
            FrameBody::Sasl(_) => {
                return Err(ConnectError::msg(
                    ErrorKind::ProtocolViolation,
                    "SASL frame after the SASL layer completed",
                ));
            }
            FrameBody::Amqp(p, payload) => (p, payload),
        };

        match performative {
            Performative::Open(_) => Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                "duplicate open",
            )),
            Performative::Close(close) => {
                if let Some(err) = &close.error {
                    tracing::debug!(%err, "peer closed with error");
                }
                self.transport
                    .queue_amqp(0, &Performative::Close(Close { error: None }), None);
                Ok(true)
            }
            Performative::Begin(begin) => {
                self.accept_begin(channel, begin)?;
                Ok(false)
            }
            Performative::End(end) => {
                self.handle_end(channel, end).await;
                Ok(false)
            }
            Performative::Attach(attach) => {
                let Some(local) = self.resolve_active(channel)? else {
                    return Ok(false); // pipelined behind our End — ignore
                };
                if self
                    .sessions
                    .get(&local)
                    .expect("bound channel")
                    .knows_link(&attach.name)
                {
                    // A response to a link we initiated (none today).
                    let session = self.sessions.get_mut(&local).expect("bound channel");
                    session
                        .handle_link_frame(
                            Performative::Attach(attach),
                            payload,
                            &mut self.transport,
                            self.max_frame_size,
                        )
                        .await?;
                } else {
                    self.accept_attach(local, channel, attach).await;
                }
                self.drain_link_events(local).await;
                Ok(false)
            }
            p @ (Performative::Transfer(_)
            | Performative::Flow(_)
            | Performative::Disposition(_)
            | Performative::Detach(_)) => {
                let Some(local) = self.resolve_active(channel)? else {
                    return Ok(false); // pipelined behind our End — ignore
                };
                let session = self.sessions.get_mut(&local).expect("bound channel");
                session
                    .handle_link_frame(p, payload, &mut self.transport, self.max_frame_size)
                    .await?;
                self.drain_link_events(local).await;
                Ok(false)
            }
        }
    }

    /// Accept a peer-initiated attach: resolve its address to a queue, mirror
    /// the endpoint, and bind it.
    async fn accept_attach(&mut self, local: u16, remote_channel: u16, attach: Attach) {
        // A sender targeting a COORDINATOR is a transaction control link
        // (spec part 4) — no queue behind it; declare/discharge control
        // messages arrive as deliveries.
        if attach.role == Role::Sender
            && matches!(&attach.target, Some(TargetArchetype::Coordinator(_)))
        {
            let session = self.sessions.get_mut(&local).expect("bound channel");
            let accepted = session.accept_peer_attach(
                attach,
                CreditMode::Manual,
                self.config.initial_credit,
                self.config.max_message_size,
                self.link_events_tx.clone(),
                &mut self.transport,
            );
            match accepted {
                Ok(a) => {
                    self.bindings
                        .insert((local, a.handle.0), Binding::Coordinator);
                    tracing::debug!(handle = a.handle.0, "coordinator link bound");
                }
                Err(e) => {
                    tracing::warn!(error = %e, "coordinator attach rejected; ending session");
                    self.end_session_with_error(
                        local,
                        remote_channel,
                        AmqpError::new(AmqpCondition::ResourceLimitExceeded, Some(e.to_string())),
                    )
                    .await;
                }
            }
            return;
        }
        // Producer (peer sender) targets a queue; consumer (peer receiver)
        // sources from one.
        let address = match attach.role {
            Role::Sender => match &attach.target {
                Some(TargetArchetype::Target(t)) => t.address.clone(),
                _ => None,
            },
            Role::Receiver => attach.source.as_ref().and_then(|s| s.address.clone()),
        };
        let queue = match address.as_deref() {
            Some(a) => self.registry.resolve(a).await,
            None => None,
        };
        let Some(queue) = queue else {
            tracing::debug!(name = %attach.name, ?address, "attach to unresolvable address");
            // Refuse just this link (attach null-terminus + detach not-found);
            // sibling links and the session stay up.
            let session = self.sessions.get_mut(&local).expect("bound channel");
            session.refuse_peer_attach(
                &attach,
                AmqpError::new(
                    AmqpCondition::NotFound,
                    Some(format!("no queue for address {address:?}")),
                ),
                &mut self.transport,
            );
            return;
        };

        let peer_role = attach.role;
        let initial_credit = match peer_role {
            Role::Sender => self.config.initial_credit,
            Role::Receiver => 0,
        };
        let session = self.sessions.get_mut(&local).expect("bound channel");
        let accepted = session.accept_peer_attach(
            attach,
            CreditMode::Manual,
            initial_credit,
            self.config.max_message_size,
            self.link_events_tx.clone(),
            &mut self.transport,
        );
        let accepted = match accepted {
            Ok(a) => a,
            Err(e) => {
                tracing::warn!(error = %e, "attach rejected; ending session");
                self.end_session_with_error(
                    local,
                    remote_channel,
                    AmqpError::new(AmqpCondition::ResourceLimitExceeded, Some(e.to_string())),
                )
                .await;
                return;
            }
        };

        let epoch = self.next_gen;
        self.next_gen += 1;
        let binding = match peer_role {
            Role::Sender => Binding::Producer {
                queue,
                epoch,
                acked_since_grant: 0,
            },
            Role::Receiver => {
                let (reply_tx, reply_rx) = oneshot::channel();
                let subscribed = queue
                    .tx
                    .send(QueueMsg::Subscribe {
                        conn: self.cmd_tx.clone(),
                        channel: local,
                        handle: accepted.handle.0,
                        binding_gen: epoch,
                        reply: reply_tx,
                    })
                    .await
                    .is_ok();
                let Ok(sub) = reply_rx.await else {
                    debug_assert!(!subscribed, "queue died between send and reply");
                    tracing::warn!(queue = %queue.name, "queue actor unavailable");
                    return;
                };
                Binding::Consumer {
                    queue,
                    sub,
                    epoch,
                    granted: 0,
                }
            }
        };
        self.bindings.insert((local, accepted.handle.0), binding);
        tracing::debug!(handle = accepted.handle.0, role = ?accepted.role, "link bound");
    }

    /// Drain the link events emitted synchronously by the session call we just
    /// made, routing them to queues. `channel` is the session they came from.
    async fn drain_link_events(&mut self, channel: u16) {
        while let Ok(event) = self.link_events_rx.try_recv() {
            match event {
                LinkEvent::Delivery(d) => {
                    let handle = d.handle.0;
                    // Clone the sender + epoch out so the bindings borrow is
                    // released before the await / the credit replenishment.
                    let (queue_tx, queue_name, epoch) = match self.bindings.get(&(channel, handle))
                    {
                        Some(Binding::Producer { queue, epoch, .. }) => {
                            (queue.tx.clone(), queue.name.clone(), *epoch)
                        }
                        Some(Binding::Coordinator) => {
                            // The client pipelines transactional dispositions
                            // ahead of its discharge; their settlement
                            // futures are resolved but may not be staged yet.
                            // Drain them first or a discharge could take an
                            // incomplete transaction.
                            self.drain_ready_settlements().await;
                            self.handle_txn_control(channel, handle, d);
                            continue;
                        }
                        _ => continue, // link vanished mid-flight
                    };
                    // A transfer carrying `transactional-state` stages its
                    // enqueue under the transaction instead of publishing.
                    if let Some(ts) = d.state.as_ref().and_then(txn_state) {
                        let staged = self.txns.stage_publish(
                            &ts.txn_id,
                            StagedPublish {
                                queue: queue_tx,
                                queue_name,
                                body: d.message,
                            },
                        );
                        if let Some(session) = self.sessions.get_mut(&channel) {
                            let state = if staged {
                                transactional_state(
                                    ts.txn_id,
                                    Some(ramqp_core::types::messaging::Outcome::Accepted(
                                        Accepted::default(),
                                    )),
                                )
                            } else {
                                DeliveryState::Rejected(Rejected {
                                    error: Some(AmqpError::new(
                                        AmqpCondition::ResourceLimitExceeded,
                                        Some("unknown transaction or staging cap".to_owned()),
                                    )),
                                })
                            };
                            session.send_disposition(
                                handle,
                                d.delivery_id,
                                None,
                                state,
                                true,
                                &mut self.transport,
                            );
                        }
                        self.replenish_producer_credit(channel, handle);
                        continue;
                    }
                    let settled = d.settled;
                    let ack = (!settled).then(|| PublishAck {
                        conn: self.cmd_tx.clone(),
                        channel,
                        handle,
                        binding_gen: epoch,
                        delivery_id: d.delivery_id.value(),
                    });
                    // Bounded queue mailbox: a full queue back-pressures this
                    // connection (and thus the producer) — never unbounded.
                    if queue_tx
                        .send(QueueMsg::Publish {
                            body: d.message,
                            ack,
                        })
                        .await
                        .is_err()
                    {
                        tracing::warn!(channel, handle, "publish to dead queue actor");
                        continue;
                    }
                    // A pre-settled publish gets NO ack (no SettleIncoming), so
                    // its credit is replenished here. Unsettled publishes are
                    // replenished on the ack path (handle_cmd), which is what
                    // bounds their in-flight window / command backlog.
                    if settled {
                        self.replenish_producer_credit(channel, handle);
                    }
                }
                LinkEvent::Credit {
                    handle,
                    credit,
                    drain,
                } => {
                    // `credit` is the session's absolute remaining link-credit,
                    // which cannot see Delivers still in `cmd_rx` or parked in the
                    // sender outbox. Grant the queue only the delta above what we
                    // have already handed out (`granted`); a restated flow then
                    // adds nothing, so those in-flight deliveries aren't
                    // double-counted into an over-dispatch that strands messages.
                    let forward = match self.bindings.get_mut(&(channel, handle.0)) {
                        Some(Binding::Consumer {
                            queue,
                            sub,
                            granted,
                            ..
                        }) => {
                            let delta = credit.saturating_sub(*granted);
                            // On drain, core has consumed the link-credit; reset
                            // our view so the post-drain grant re-arms cleanly.
                            *granted = if drain { 0 } else { credit.max(*granted) };
                            (delta > 0 || drain).then(|| (queue.tx.clone(), *sub, delta))
                        }
                        _ => None,
                    };
                    if let Some((tx, sub, delta)) = forward {
                        let _ = tx
                            .send(QueueMsg::Demand {
                                sub,
                                credit: delta,
                                drain,
                            })
                            .await;
                    }
                }
                LinkEvent::Detached { handle, .. } => {
                    if let Some(Binding::Consumer { queue, sub, .. }) =
                        self.bindings.remove(&(channel, handle.0))
                    {
                        let _ = queue.tx.send(QueueMsg::Unsubscribe { sub }).await;
                    }
                }
                // Consumer settlements arrive via the per-dispatch replies in
                // `settlements`, not via Disposition events.
                LinkEvent::Disposition { .. } => {}
            }
        }
    }

    /// Accept a peer-initiated `begin`.
    fn accept_begin(&mut self, remote_channel: u16, begin: Begin) -> Result<(), ConnectError> {
        if begin.remote_channel.is_some() {
            // A begin *response* — but this broker never initiates sessions.
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                "begin response for a session we never initiated",
            ));
        }
        if self.remote_channels.resolve(remote_channel).is_some() {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!("duplicate begin on channel {remote_channel}"),
            ));
        }
        // The peer is reusing this channel for a fresh session; drop any stale
        // discarding marker so its later `End` tears down the *new* session
        // rather than being swallowed as an echo of the old one.
        self.discarding.remove(&remote_channel);
        let Some(local) = self.channels.allocate() else {
            return Err(ConnectError::msg(
                ErrorKind::Capacity,
                "channel-max exhausted",
            ));
        };
        self.next_session_id += 1;
        let (session, response) = Session::accept_peer_begin(
            SessionId(self.next_session_id),
            local,
            remote_channel,
            &begin,
            &self.config.session,
            self.session_events_tx.clone(),
            self.metrics.clone(),
        );
        self.remote_channels.bind(remote_channel, local);
        self.sessions.insert(local, session);
        self.transport
            .queue_amqp(local, &Performative::Begin(response), None);
        tracing::debug!(remote_channel, local_channel = local, "session begun");
        Ok(())
    }

    /// Handle a peer `end`: acknowledge and tear the session down.
    async fn handle_end(&mut self, remote_channel: u16, end: End) {
        if self.discarding.remove(&remote_channel) {
            // The peer's echo of an `End` we already sent (e.g. after we
            // refused an attach by ending the session). Teardown is complete;
            // just drop the discarding marker — do not re-send `End`.
            return;
        }
        let Some(local) = self.remote_channels.resolve(remote_channel) else {
            return; // already gone — end/end race, ignore
        };
        if let Some(mut session) = self.sessions.remove(&local) {
            session.on_peer_end(end.error);
        }
        self.transport
            .queue_amqp(local, &Performative::End(End { error: None }), None);
        self.remote_channels.unbind(remote_channel);
        self.channels.release(local);
        self.release_session_bindings(local).await;
        tracing::debug!(remote_channel, local_channel = local, "session ended");
    }

    /// End a session server-side with an error (e.g. a rejected attach). We are
    /// the initiator here, so the peer has not yet seen our `End`: mark the
    /// remote channel `discarding` so frames it already pipelined behind the
    /// rejected attach are ignored (not treated as fatal) until its `End` echo.
    async fn end_session_with_error(&mut self, local: u16, remote_channel: u16, error: AmqpError) {
        self.sessions.remove(&local);
        self.transport
            .queue_amqp(local, &Performative::End(End { error: Some(error) }), None);
        self.remote_channels.unbind(remote_channel);
        self.channels.release(local);
        self.discarding.insert(remote_channel);
        self.release_session_bindings(local).await;
    }

    /// Unbind every link on a session, unsubscribing its consumers.
    async fn release_session_bindings(&mut self, local: u16) {
        let keys: Vec<_> = self
            .bindings
            .keys()
            .filter(|(ch, _)| *ch == local)
            .copied()
            .collect();
        for key in keys {
            if let Some(Binding::Consumer { queue, sub, .. }) = self.bindings.remove(&key) {
                let _ = queue.tx.send(QueueMsg::Unsubscribe { sub }).await;
            }
        }
    }

    /// Best-effort `close{error}` sent to the peer when the connection fails,
    /// so the peer learns the condition instead of only seeing a TCP reset.
    /// Errors that mean the socket is already gone (I/O, peer-closed) or a
    /// clean local cancellation are skipped — there is no one to tell.
    async fn close_with_error(&mut self, err: &ConnectError) {
        let condition: ErrorCondition = match err.kind() {
            ErrorKind::ProtocolViolation => ConnectionError::FramingError.into(),
            ErrorKind::Timeout => ConnectionError::ConnectionForced.into(),
            ErrorKind::Sasl => AmqpCondition::UnauthorizedAccess.into(),
            ErrorKind::Capacity => AmqpCondition::ResourceLimitExceeded.into(),
            ErrorKind::Settlement | ErrorKind::Encode => AmqpCondition::InternalError.into(),
            // Socket already gone or a clean local stop: nothing to send.
            ErrorKind::Io
            | ErrorKind::Tls
            | ErrorKind::PeerClosed
            | ErrorKind::NotConnected
            | ErrorKind::Cancelled => return,
            _ => AmqpCondition::InternalError.into(),
        };
        let error = AmqpError::new(condition, Some(err.to_string()));
        let _ = self
            .transport
            .send_amqp(0, &Performative::Close(Close { error: Some(error) }), None)
            .await;
    }

    /// After we initiate `close`, wait briefly for the peer's `close`.
    async fn await_peer_close(&mut self) {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(5), async {
            loop {
                match self.transport.read_frame().await {
                    Ok(Frame {
                        body: FrameBody::Amqp(Performative::Close(_), _),
                        ..
                    }) => break,
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        })
        .await;
    }
}

/// Map a transactional provisional outcome onto a queue settlement.
fn outcome_to_settle(outcome: &ramqp_core::types::messaging::Outcome) -> SettleOutcome {
    use ramqp_core::types::messaging::Outcome;
    match outcome {
        Outcome::Accepted(_) => SettleOutcome::Ack,
        Outcome::Released(_) => SettleOutcome::Requeue,
        Outcome::Modified(m) => {
            if m.delivery_failed.unwrap_or(false) {
                SettleOutcome::RequeueFailed
            } else {
                SettleOutcome::Requeue
            }
        }
        Outcome::Rejected(_) => SettleOutcome::Drop,
    }
}

/// Map a consumer's terminal delivery state onto a queue settlement.
fn state_to_outcome(state: &DeliveryState) -> SettleOutcome {
    match state {
        DeliveryState::Accepted(_) => SettleOutcome::Ack,
        DeliveryState::Released(_) => SettleOutcome::Requeue,
        DeliveryState::Modified(m) => {
            if m.delivery_failed.unwrap_or(false) {
                SettleOutcome::RequeueFailed
            } else {
                SettleOutcome::Requeue
            }
        }
        DeliveryState::Rejected(_) => SettleOutcome::Drop,
        // Non-terminal or unknown states shouldn't complete a settlement;
        // requeue is the safe default (at-least-once).
        _ => SettleOutcome::Requeue,
    }
}

fn unknown_channel(channel: u16) -> ConnectError {
    ConnectError::msg(
        ErrorKind::ProtocolViolation,
        format!("frame on unmapped channel {channel}"),
    )
}
