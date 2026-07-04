//! The per-connection driver: server-order handshake (header → SASL → `open`)
//! and the frame-routing event loop.
//!
//! One owning task per connection (the same lock-free actor model as the
//! client): all protocol state lives here, nothing is shared, and writes are
//! coalesced into one flush per loop iteration.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};

use ramqp_core::codec::Symbol;
use ramqp_core::config::CreditMode;
use ramqp_core::connection::heartbeat::{Heartbeat, HeartbeatAction};
use ramqp_core::connection::mux::{ChannelAllocator, RemoteChannelMap};
use ramqp_core::connection::negotiate::{MIN_MAX_FRAME_SIZE, build_open, reconcile};
use ramqp_core::error::{ConnectError, ErrorKind};
use ramqp_core::ids::SessionId;
use ramqp_core::observe::{SharedMetrics, noop_metrics};
use ramqp_core::proto::{LinkEvent, SessionEvent};
use ramqp_core::sasl::server::parse_plain_response;
use ramqp_core::session::state::Session;
use ramqp_core::transport::IoStream;
use ramqp_core::transport::frame::{Frame, FrameBody, FramedTransport};
use ramqp_core::transport::header::{ProtocolHeader, accept as accept_header};
use ramqp_core::types::definitions::Error as AmqpError;
use ramqp_core::types::performatives::{Begin, Close, End, Performative};
use ramqp_core::types::sasl::{SaslCode, SaslFrame, SaslMechanisms, SaslOutcome};

use crate::auth::{Authenticator, Credentials};
use crate::config::BrokerConfig;

/// Serve one accepted byte stream to completion (handshake + event loop).
pub(crate) async fn serve<S: IoStream>(
    stream: S,
    config: Arc<BrokerConfig>,
    auth: Arc<dyn Authenticator>,
    shutdown: watch::Receiver<bool>,
) -> Result<(), ConnectError> {
    // Bound the whole inbound handshake (header + SASL + open) so a client
    // that connects then stalls cannot pin this task (slow-loris guard).
    let handshake = handshake(stream, &config, auth.as_ref());
    let mut conn = match config.connection.connect_timeout {
        Some(t) => tokio::time::timeout(t, handshake)
            .await
            .map_err(|_| ConnectError::msg(ErrorKind::Timeout, "inbound handshake timed out"))??,
        None => handshake.await?,
    };
    conn.shutdown = Some(shutdown);
    conn.run().await
}

/// Run the server-order handshake, returning the established connection.
async fn handshake<S: IoStream>(
    mut stream: S,
    config: &Arc<BrokerConfig>,
    auth: &dyn Authenticator,
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

    tracing::debug!(container = %peer_open.container_id, "connection open");
    Ok(BrokerConnection {
        transport,
        config: config.clone(),
        max_frame_size: negotiated.max_frame_size as usize,
        heartbeat,
        channels: ChannelAllocator::new(negotiated.channel_max),
        remote_channels: RemoteChannelMap::default(),
        sessions: HashMap::new(),
        next_session_id: 0,
        metrics: noop_metrics(),
        link_events_tx,
        link_events_rx,
        session_events_tx,
        session_events_rx,
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

/// An established broker-side connection (post-handshake).
struct BrokerConnection<S: IoStream> {
    transport: FramedTransport<S>,
    config: Arc<BrokerConfig>,
    max_frame_size: usize,
    heartbeat: Heartbeat,
    channels: ChannelAllocator,
    remote_channels: RemoteChannelMap,
    /// Sessions keyed by OUR channel.
    sessions: HashMap<u16, Session>,
    next_session_id: u64,
    metrics: SharedMetrics,
    /// Shared event channel for all accepted links on this connection.
    /// Phase 3 drains it; Phase 4 feeds deliveries into queues instead.
    link_events_tx: mpsc::Sender<LinkEvent>,
    link_events_rx: mpsc::Receiver<LinkEvent>,
    session_events_tx: mpsc::UnboundedSender<SessionEvent>,
    session_events_rx: mpsc::UnboundedReceiver<SessionEvent>,
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

                // Phase 3: accepted-link events have no queue to land in yet;
                // drain them so channels never back up.
                Some(event) = self.link_events_rx.recv() => {
                    tracing::trace!(?event, "link event (dropped; no queue layer yet)");
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

    async fn flush(&mut self) -> Result<(), ConnectError> {
        if self.transport.pending_bytes() > 0 {
            self.transport.flush().await?;
            self.heartbeat.record_send();
        }
        Ok(())
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
                self.handle_end(channel, end);
                Ok(false)
            }
            Performative::Attach(attach) => {
                let Some(local) = self.remote_channels.resolve(channel) else {
                    return Err(unknown_channel(channel));
                };
                let session = self.sessions.get_mut(&local).expect("bound channel");
                if session.knows_link(&attach.name) {
                    // A response to a link we initiated (none in Phase 3, but
                    // the path is uniform).
                    session
                        .handle_link_frame(
                            Performative::Attach(attach),
                            payload,
                            &mut self.transport,
                            self.max_frame_size,
                        )
                        .await?;
                } else {
                    let accepted = session.accept_peer_attach(
                        attach,
                        CreditMode::Manual,
                        self.config.initial_credit,
                        self.config.max_message_size,
                        self.link_events_tx.clone(),
                        &mut self.transport,
                    );
                    match accepted {
                        Ok(link) => {
                            tracing::debug!(handle = link.handle.0, role = ?link.role, "link accepted");
                        }
                        Err(e) => {
                            // Session-level failure: end the session rather
                            // than dropping the whole connection.
                            tracing::warn!(error = %e, "attach rejected; ending session");
                            self.end_session_with_error(
                                local,
                                channel,
                                AmqpError::new(
                                    ramqp_core::types::definitions::AmqpError::ResourceLimitExceeded,
                                    Some(e.to_string()),
                                ),
                            );
                        }
                    }
                }
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
                Ok(false)
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
    fn handle_end(&mut self, remote_channel: u16, end: End) {
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
        tracing::debug!(remote_channel, local_channel = local, "session ended");
    }

    /// End a session server-side with an error (e.g. a rejected attach).
    fn end_session_with_error(&mut self, local: u16, remote_channel: u16, error: AmqpError) {
        self.sessions.remove(&local);
        self.transport
            .queue_amqp(local, &Performative::End(End { error: Some(error) }), None);
        self.remote_channels.unbind(remote_channel);
        self.channels.release(local);
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

fn unknown_channel(channel: u16) -> ConnectError {
    ConnectError::msg(
        ErrorKind::ProtocolViolation,
        format!("frame on unmapped channel {channel}"),
    )
}
