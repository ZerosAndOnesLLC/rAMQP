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
use ramqp_core::types::definitions::{AmqpError as AmqpCondition, Error as AmqpError, Role};
use ramqp_core::types::messaging::{Accepted, DeliveryState, Rejected, TargetArchetype};
use ramqp_core::types::performatives::{Attach, Begin, Close, End, Performative};
use ramqp_core::types::sasl::{SaslCode, SaslFrame, SaslMechanisms, SaslOutcome};

use crate::auth::{Authenticator, Credentials};
use crate::config::BrokerConfig;
use crate::queue::{ConnCmd, PublishAck, QueueHandle, QueueMsg, SettleOutcome, SubId};
use crate::registry::QueueRegistry;

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
    transport
        .send_amqp(0, &Performative::Open(local_open), None)
        .await?;
    transport.set_max_frame_size(negotiated.max_frame_size);

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
        bindings: HashMap::new(),
        settlements: FuturesUnordered::new(),
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
        /// Deliveries since the last credit top-up (batched replenishment).
        received_since_grant: u32,
    },
    /// Peer receiver → our sender → subscribed to `queue` as `sub`.
    Consumer { queue: QueueHandle, sub: SubId },
}

/// The resolution of one dispatched delivery: which queue/sub/message it was
/// and the outcome the consumer settled it with.
type SettlementResult = (mpsc::Sender<QueueMsg>, SubId, u64, SettleOutcome);

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
    /// Link → queue bindings, keyed by (our channel, our link handle).
    bindings: HashMap<(u16, u32), Binding>,
    /// In-flight consumer dispatches awaiting a terminal outcome.
    settlements: FuturesUnordered<std::pin::Pin<Box<dyn Future<Output = SettlementResult> + Send>>>,
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

                Some((queue, sub, msg_id, outcome)) = self.settlements.next() => {
                    let _ = queue.send(QueueMsg::Settle { sub, msg_id, outcome }).await;
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
        for ((_, _), binding) in self.bindings.drain() {
            if let Binding::Consumer { queue, sub } = binding {
                let _ = queue.tx.send(QueueMsg::Unsubscribe { sub }).await;
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

    /// Handle one command from a queue actor.
    fn handle_cmd(&mut self, cmd: ConnCmd) {
        match cmd {
            ConnCmd::Deliver {
                channel,
                handle,
                msg_id,
                body,
            } => {
                let Some(session) = self.sessions.get_mut(&channel) else {
                    // Session raced away; the queue will requeue via settle.
                    self.settle_later_requeue(channel, handle, msg_id);
                    return;
                };
                let Some(Binding::Consumer { queue, sub }) = self.bindings.get(&(channel, handle))
                else {
                    return; // link gone; unsubscribe already requeued it
                };
                let (reply_tx, reply_rx) = oneshot::channel();
                let (queue_tx, sub) = (queue.tx.clone(), *sub);
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
                    let outcome = match reply_rx.await {
                        Ok(Ok(state)) => state_to_outcome(&state),
                        // Link/connection died before a terminal outcome:
                        // requeue (no-op if an unsubscribe already did).
                        Ok(Err(_)) | Err(_) => SettleOutcome::Requeue,
                    };
                    (queue_tx, sub, msg_id, outcome)
                }));
            }
            ConnCmd::SettleIncoming {
                channel,
                handle,
                delivery_id,
                accepted,
            } => {
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
            }
        }
    }

    /// Queue a requeue-settlement for a delivery we can no longer dispatch.
    fn settle_later_requeue(&mut self, channel: u16, handle: u32, msg_id: u64) {
        if let Some(Binding::Consumer { queue, sub }) = self.bindings.get(&(channel, handle)) {
            let (queue_tx, sub) = (queue.tx.clone(), *sub);
            self.settlements.push(Box::pin(async move {
                (queue_tx, sub, msg_id, SettleOutcome::Requeue)
            }));
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
                let Some(local) = self.remote_channels.resolve(channel) else {
                    return Err(unknown_channel(channel));
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
                let Some(local) = self.remote_channels.resolve(channel) else {
                    return Err(unknown_channel(channel));
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
        // Producer (peer sender) targets a queue; consumer (peer receiver)
        // sources from one.
        let address = match attach.role {
            Role::Sender => match &attach.target {
                Some(TargetArchetype::Target(t)) => t.address.clone(),
                _ => None,
            },
            Role::Receiver => attach.source.as_ref().and_then(|s| s.address.clone()),
        };
        let queue = address.as_deref().and_then(|a| self.registry.resolve(a));
        let Some(queue) = queue else {
            tracing::debug!(name = %attach.name, ?address, "attach to unresolvable address");
            self.end_session_with_error(
                local,
                remote_channel,
                AmqpError::new(
                    AmqpCondition::NotFound,
                    Some(format!("no queue for address {address:?}")),
                ),
            )
            .await;
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

        let binding = match peer_role {
            Role::Sender => Binding::Producer {
                queue,
                received_since_grant: 0,
            },
            Role::Receiver => {
                let (reply_tx, reply_rx) = oneshot::channel();
                let subscribed = queue
                    .tx
                    .send(QueueMsg::Subscribe {
                        conn: self.cmd_tx.clone(),
                        channel: local,
                        handle: accepted.handle.0,
                        reply: reply_tx,
                    })
                    .await
                    .is_ok();
                let Ok(sub) = reply_rx.await else {
                    debug_assert!(!subscribed, "queue died between send and reply");
                    tracing::warn!(queue = %queue.name, "queue actor unavailable");
                    return;
                };
                Binding::Consumer { queue, sub }
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
                    let Some(Binding::Producer {
                        queue,
                        received_since_grant,
                    }) = self.bindings.get_mut(&(channel, handle))
                    else {
                        continue; // link vanished mid-flight
                    };
                    let ack = (!d.settled).then(|| PublishAck {
                        conn: self.cmd_tx.clone(),
                        channel,
                        handle,
                        delivery_id: d.delivery_id.value(),
                    });
                    // Bounded queue mailbox: a full queue back-pressures this
                    // connection (and thus the producer) — never unbounded.
                    if queue
                        .tx
                        .send(QueueMsg::Publish {
                            body: d.message,
                            ack,
                        })
                        .await
                        .is_err()
                    {
                        tracing::warn!(queue = %queue.name, "publish to dead queue actor");
                        continue;
                    }
                    // Batched credit replenishment: top the producer back up
                    // once it has consumed half its window.
                    *received_since_grant += 1;
                    let threshold = (self.config.initial_credit / 2).max(1);
                    if *received_since_grant >= threshold {
                        let grant = *received_since_grant;
                        *received_since_grant = 0;
                        if let Some(session) = self.sessions.get_mut(&channel) {
                            session.grant_credit(handle, grant, &mut self.transport);
                        }
                    }
                }
                LinkEvent::Credit {
                    handle,
                    credit,
                    drain: _,
                } => {
                    if let Some(Binding::Consumer { queue, sub }) =
                        self.bindings.get(&(channel, handle.0))
                    {
                        let _ = queue.tx.send(QueueMsg::Demand { sub: *sub, credit }).await;
                    }
                }
                LinkEvent::Detached { handle, .. } => {
                    if let Some(Binding::Consumer { queue, sub }) =
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

    /// End a session server-side with an error (e.g. a rejected attach).
    async fn end_session_with_error(&mut self, local: u16, remote_channel: u16, error: AmqpError) {
        self.sessions.remove(&local);
        self.transport
            .queue_amqp(local, &Performative::End(End { error: Some(error) }), None);
        self.remote_channels.unbind(remote_channel);
        self.channels.release(local);
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
            if let Some(Binding::Consumer { queue, sub }) = self.bindings.remove(&key) {
                let _ = queue.tx.send(QueueMsg::Unsubscribe { sub }).await;
            }
        }
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
