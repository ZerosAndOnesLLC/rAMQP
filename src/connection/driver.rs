//! The connection driver task (WP-2.1) — the single owner of the transport and
//! all protocol state (decision D-2). User handles never touch this state; they
//! send [`DriverCommand`]s and await `oneshot` replies.

use std::sync::Arc;

use tokio::sync::mpsc;

use crate::config::Config;
use crate::connection::heartbeat::{Heartbeat, HeartbeatAction};
use crate::connection::mux::{ChannelAllocator, RemoteChannelMap};
use crate::connection::negotiate::{
    MIN_MAX_FRAME_SIZE, Negotiated, build_open, close_to_error, reconcile,
};
use crate::error::{ConnectError, ErrorKind, LinkError, RecvError, SendError, SessionError};
use crate::ids::{ChannelId, SessionId};
use crate::observe::{ConnectionEvent, EventBus, SharedMetrics};
use crate::proto::{DriverCommand, Reply, SessionEvent, SessionOpened};
use crate::session::state::{Session, SessionPhase};
use crate::transport::IoStream;
use crate::transport::frame::{Frame, FrameBody, FramedTransport};
use crate::types::definitions::Error as AmqpError;
use crate::types::performatives::{Begin, Close, End, Performative};

/// Connection lifecycle state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnState {
    Open,
    Closing,
    Closed,
}

/// The owning driver task for one connection.
pub struct Driver<S> {
    transport: FramedTransport<S>,
    commands: mpsc::Receiver<DriverCommand>,
    metrics: SharedMetrics,
    events: EventBus,
    negotiated: Negotiated,
    heartbeat: Heartbeat,
    channels: ChannelAllocator,
    /// Local channel → session state.
    sessions: std::collections::HashMap<u16, Session>,
    /// Peer's channel → our local channel.
    remote_channels: RemoteChannelMap,
    state: ConnState,
}

impl<S> std::fmt::Debug for Driver<S> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Driver")
            .field("state", &self.state)
            .field("negotiated", &self.negotiated)
            .field("sessions", &self.sessions.len())
            .finish_non_exhaustive()
    }
}

impl<S: IoStream> Driver<S> {
    /// Perform the connection `open` exchange and construct the driver.
    ///
    /// The protocol header (and any SASL handshake) must already have completed
    /// on `transport`.
    pub async fn open(
        mut transport: FramedTransport<S>,
        config: Arc<Config>,
        metrics: SharedMetrics,
        events: EventBus,
        commands: mpsc::Receiver<DriverCommand>,
    ) -> Result<Self, ConnectError> {
        let local_open = build_open(&config.connection);
        transport
            .send_amqp(0, &Performative::Open(local_open.clone()), None)
            .await?;

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

        // A peer advertising a max-frame-size below the 512-octet spec floor is
        // non-compliant (it could not receive our minimum frames); reject the
        // connection rather than silently clamping it.
        if peer_open.max_frame_size < MIN_MAX_FRAME_SIZE {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!(
                    "peer advertised max-frame-size {} below the {MIN_MAX_FRAME_SIZE}-octet minimum",
                    peer_open.max_frame_size
                ),
            ));
        }

        let negotiated = reconcile(&local_open, &peer_open);
        transport.set_max_frame_size(negotiated.max_frame_size);
        events.publish(ConnectionEvent::Connected);

        let heartbeat = Heartbeat::new(negotiated.send_interval, negotiated.recv_timeout);
        let channels = ChannelAllocator::new(negotiated.channel_max);

        Ok(Driver {
            transport,
            commands,
            metrics,
            events,
            negotiated,
            heartbeat,
            channels,
            sessions: std::collections::HashMap::new(),
            remote_channels: RemoteChannelMap::default(),
            state: ConnState::Open,
        })
    }

    /// The negotiated connection parameters.
    pub fn negotiated(&self) -> Negotiated {
        self.negotiated
    }

    /// Run the event loop until the connection closes or fails. Returns the
    /// terminal error, if any.
    pub async fn run(mut self) -> Result<(), ConnectError> {
        let result = self.event_loop().await;
        match &result {
            Ok(()) => self.events.publish(ConnectionEvent::Closed {
                error: false,
                reason: "closed".into(),
            }),
            Err(e) => self.events.publish(ConnectionEvent::Closed {
                error: true,
                reason: e.to_string(),
            }),
        }
        self.state = ConnState::Closed;
        result
    }

    async fn event_loop(&mut self) -> Result<(), ConnectError> {
        loop {
            tokio::select! {
                biased;

                command = self.commands.recv() => {
                    match command {
                        Some(command) => {
                            let done = self.handle_command(command).await?;
                            self.flush().await?;
                            if done {
                                return Ok(());
                            }
                        }
                        // All user handles dropped: close gracefully.
                        None => {
                            self.send_close(None).await?;
                            self.await_peer_close().await?;
                            return Ok(());
                        }
                    }
                }

                frame = self.transport.read_frame() => {
                    let frame = frame?;
                    self.heartbeat.record_recv();
                    self.metrics.on_frame_received(self.transport.last_read_size());
                    let done = self.handle_frame(frame).await?;
                    self.flush().await?;
                    if done {
                        return Ok(());
                    }
                }

                action = self.heartbeat.tick() => {
                    match action {
                        HeartbeatAction::SendEmpty => {
                            self.transport.queue_empty(0);
                            self.transport.flush().await?;
                            self.heartbeat.record_send();
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
            }
        }
    }

    /// Handle a user command. Returns `Ok(true)` to terminate the loop.
    async fn handle_command(&mut self, command: DriverCommand) -> Result<bool, ConnectError> {
        match command {
            DriverCommand::CloseConnection { error, reply } => {
                let result = self.do_close(error).await;
                let _ = reply.send(result);
                Ok(true)
            }
            DriverCommand::BeginSession {
                begin,
                events,
                reply,
            } => {
                self.begin_session(*begin, events, reply).await?;
                Ok(false)
            }
            DriverCommand::EndSession {
                channel,
                error,
                reply,
            } => {
                self.end_session(channel, error, reply).await?;
                Ok(false)
            }
            DriverCommand::AttachLink {
                channel,
                attach,
                credit_mode,
                events,
                reply,
            } => {
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.attach_link(*attach, credit_mode, events, reply, &mut self.transport);
                } else {
                    let _ = reply.send(Err(LinkError::msg(
                        ErrorKind::NotConnected,
                        "no such session",
                    )));
                }
                Ok(false)
            }
            DriverCommand::DetachLink {
                channel,
                handle,
                closed,
                error,
                reply,
            } => {
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.detach_link(handle.value(), closed, error, reply, &mut self.transport);
                } else {
                    let _ = reply.send(Err(LinkError::msg(
                        ErrorKind::NotConnected,
                        "no such session",
                    )));
                }
                Ok(false)
            }
            DriverCommand::SendTransfer {
                channel,
                handle,
                body,
                settled,
                message_format,
                state,
                reply,
            } => {
                let max = self.negotiated.max_frame_size as usize;
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.send_transfer(
                        handle.value(),
                        body,
                        settled,
                        message_format,
                        state,
                        reply,
                        &mut self.transport,
                        max,
                    );
                    self.metrics.on_transfer_sent();
                } else if let Some(reply) = reply {
                    let _ = reply.send(Err(SendError::msg(
                        ErrorKind::NotConnected,
                        "no such session",
                    )));
                }
                Ok(false)
            }
            DriverCommand::SendDisposition {
                channel,
                handle,
                first,
                last,
                state,
                settled,
                reply,
            } => {
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.send_disposition(
                        handle.value(),
                        first,
                        last,
                        state,
                        settled,
                        &mut self.transport,
                    );
                    if let Some(reply) = reply {
                        let _ = reply.send(Ok(()));
                    }
                } else if let Some(reply) = reply {
                    let _ = reply.send(Err(RecvError::msg(
                        ErrorKind::NotConnected,
                        "no such session",
                    )));
                }
                Ok(false)
            }
            DriverCommand::SendFlow { channel, flow } => {
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.send_flow(*flow, &mut self.transport);
                }
                Ok(false)
            }
            DriverCommand::GrantCredit {
                channel,
                handle,
                credit,
            } => {
                if let Some(session) = self.sessions.get_mut(&channel.value()) {
                    session.grant_credit(handle.value(), credit, &mut self.transport);
                }
                Ok(false)
            }
        }
    }

    async fn begin_session(
        &mut self,
        mut begin: Begin,
        events: tokio::sync::mpsc::UnboundedSender<SessionEvent>,
        reply: Reply<SessionOpened, SessionError>,
    ) -> Result<(), ConnectError> {
        let channel = match self.channels.allocate() {
            Some(c) => c,
            None => {
                let _ = reply.send(Err(SessionError::msg(
                    ErrorKind::Capacity,
                    "channel-max exhausted",
                )));
                return Ok(());
            }
        };
        begin.remote_channel = None;
        let session = Session::new(
            SessionId::next(),
            channel,
            &begin,
            events,
            reply,
            self.metrics.clone(),
        );
        self.transport
            .send_amqp(channel, &Performative::Begin(begin), None)
            .await?;
        self.sessions.insert(channel, session);
        Ok(())
    }

    async fn end_session(
        &mut self,
        channel: ChannelId,
        error: Option<AmqpError>,
        reply: Reply<(), SessionError>,
    ) -> Result<(), ConnectError> {
        let local = channel.value();
        if let Some(session) = self.sessions.get_mut(&local) {
            self.transport
                .send_amqp(local, &Performative::End(Session::build_end(error)), None)
                .await?;
            session.begin_end(reply);
        } else {
            let _ = reply.send(Err(SessionError::msg(
                ErrorKind::NotConnected,
                "no such session",
            )));
        }
        Ok(())
    }

    /// Handle an inbound frame. Returns `Ok(true)` to terminate the loop.
    async fn handle_frame(&mut self, frame: Frame) -> Result<bool, ConnectError> {
        match frame.body {
            FrameBody::Empty => Ok(false),
            FrameBody::Amqp(Performative::Close(close), _) => {
                // Peer-initiated or our close acknowledged.
                if let Some(err) = close_to_error(&close) {
                    if self.state != ConnState::Closing {
                        // Peer closed with an error; ack and surface it.
                        self.send_close(None).await?;
                    }
                    return Err(err);
                }
                if self.state != ConnState::Closing {
                    self.send_close(None).await?;
                }
                Ok(true)
            }
            FrameBody::Amqp(Performative::Open(_), _) => Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                "unexpected second open",
            )),
            FrameBody::Amqp(performative, payload) => {
                self.route_session_frame(frame.channel, performative, payload)
                    .await?;
                Ok(false)
            }
            FrameBody::Sasl(_) => Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                "unexpected SASL frame after handshake",
            )),
        }
    }

    /// Route an inbound performative to its session. Link performatives are
    /// dispatched to the link layer in Phase 4.
    async fn route_session_frame(
        &mut self,
        channel: u16,
        performative: Performative,
        payload: Option<bytes::Bytes>,
    ) -> Result<(), ConnectError> {
        // Inbound channel numbers must fall within the negotiated channel-max
        // (spec §2.7.1); a frame outside that range is a connection error.
        if channel > self.negotiated.channel_max {
            return Err(ConnectError::msg(
                ErrorKind::ProtocolViolation,
                format!(
                    "frame on channel {channel} exceeds negotiated channel-max {}",
                    self.negotiated.channel_max
                ),
            ));
        }
        match performative {
            Performative::Begin(begin) => {
                let local = begin.remote_channel.ok_or_else(|| {
                    ConnectError::msg(
                        ErrorKind::ProtocolViolation,
                        "peer begin missing remote-channel",
                    )
                })?;
                // The peer's begin must reference one of our sessions that is
                // still awaiting it. Binding a duplicate or unknown channel would
                // overwrite live session flow-control state or leak a routing
                // entry, so reject it as a connection error instead.
                match self.sessions.get(&local) {
                    Some(s) if s.phase() == SessionPhase::BeginSent => {}
                    Some(_) => {
                        return Err(ConnectError::msg(
                            ErrorKind::ProtocolViolation,
                            format!("peer begin for already-mapped channel {local}"),
                        ));
                    }
                    None => {
                        return Err(ConnectError::msg(
                            ErrorKind::ProtocolViolation,
                            format!("peer begin references unknown local channel {local}"),
                        ));
                    }
                }
                self.remote_channels.bind(channel, local);
                if let Some(session) = self.sessions.get_mut(&local) {
                    session.on_peer_begin(channel, &begin);
                }
            }
            Performative::End(end) => {
                if let Some(local) = self.remote_channels.resolve(channel) {
                    let initiated_by_us = self
                        .sessions
                        .get(&local)
                        .map(|s| s.is_ending())
                        .unwrap_or(false);
                    if let Some(session) = self.sessions.get_mut(&local) {
                        session.on_peer_end(end.error.clone());
                    }
                    if !initiated_by_us {
                        self.transport
                            .send_amqp(local, &Performative::End(End { error: None }), None)
                            .await?;
                    }
                    self.sessions.remove(&local);
                    self.channels.release(local);
                    self.remote_channels.unbind(channel);
                }
            }
            // Link performatives (attach/detach/transfer/disposition/flow).
            performative => {
                // A link frame on a channel with no mapped session is a protocol
                // violation (spec §2.7); surface it rather than dropping silently.
                let Some(local) = self.remote_channels.resolve(channel) else {
                    return Err(ConnectError::msg(
                        ErrorKind::ProtocolViolation,
                        format!("link frame on unmapped channel {channel}"),
                    ));
                };
                match &performative {
                    Performative::Transfer(_) => self.metrics.on_transfer_received(),
                    Performative::Disposition(_) => self.metrics.on_settlement(),
                    _ => {}
                }
                let max = self.negotiated.max_frame_size as usize;
                if let Some(session) = self.sessions.get_mut(&local) {
                    session
                        .handle_link_frame(performative, payload, &mut self.transport, max)
                        .await?;
                }
            }
        }
        Ok(())
    }

    async fn do_close(&mut self, error: Option<AmqpError>) -> Result<(), ConnectError> {
        self.send_close(error).await?;
        self.await_peer_close().await
    }

    async fn send_close(&mut self, error: Option<AmqpError>) -> Result<(), ConnectError> {
        self.transport
            .send_amqp(0, &Performative::Close(Close { error }), None)
            .await?;
        self.state = ConnState::Closing;
        Ok(())
    }

    async fn await_peer_close(&mut self) -> Result<(), ConnectError> {
        loop {
            match self.transport.read_frame().await {
                Ok(frame) => match frame.body {
                    FrameBody::Amqp(Performative::Close(c), _) => {
                        return match close_to_error(&c) {
                            Some(e) => Err(e),
                            None => Ok(()),
                        };
                    }
                    _ => continue,
                },
                // Peer dropped the socket after we closed: that's fine.
                Err(e) if e.kind() == ErrorKind::PeerClosed => return Ok(()),
                Err(e) => return Err(e),
            }
        }
    }

    /// Flush any queued outbound frames in one write, recording the keepalive.
    async fn flush(&mut self) -> Result<(), ConnectError> {
        let n = self.transport.pending_bytes();
        if n > 0 {
            self.transport.flush().await?;
            self.heartbeat.record_send();
            self.metrics.on_frame_sent(n);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::noop_metrics;
    use crate::transport::frame::FramedTransport;
    use tokio::sync::oneshot;

    /// A mock peer that answers `open` and `close`.
    async fn mock_peer(server: tokio::io::DuplexStream) {
        let mut st = FramedTransport::new(server, 1 << 16);
        // expect open, reply open
        let open = st.read_frame().await.unwrap();
        assert!(matches!(
            open.body,
            FrameBody::Amqp(Performative::Open(_), _)
        ));
        st.send_amqp(
            0,
            &Performative::Open(crate::types::performatives::Open::new("server")),
            None,
        )
        .await
        .unwrap();
        // expect close, reply close
        while let Ok(f) = st.read_frame().await {
            if matches!(f.body, FrameBody::Amqp(Performative::Close(_), _)) {
                st.send_amqp(0, &Performative::Close(Close { error: None }), None)
                    .await
                    .unwrap();
                break;
            }
        }
    }

    #[tokio::test]
    async fn open_and_graceful_close() {
        let (client, server) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(mock_peer(server));

        let transport = FramedTransport::new(client, 1 << 16);
        let config = Arc::new(Config::default());
        let (tx, rx) = mpsc::channel(16);
        let driver = Driver::open(transport, config, noop_metrics(), EventBus::default(), rx)
            .await
            .unwrap();

        let run = tokio::spawn(driver.run());

        // request a graceful close
        let (rtx, rrx) = oneshot::channel();
        tx.send(DriverCommand::CloseConnection {
            error: None,
            reply: rtx,
        })
        .await
        .unwrap();

        rrx.await.unwrap().unwrap();
        run.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn begin_and_end_session() {
        use crate::types::performatives::{Begin, End, Open};

        let (client, server) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(async move {
            let mut st = FramedTransport::new(server, 1 << 16);
            // open
            let _ = st.read_frame().await.unwrap();
            st.send_amqp(0, &Performative::Open(Open::new("server")), None)
                .await
                .unwrap();
            // begin: echo the client's channel as remote-channel
            let f = st.read_frame().await.unwrap();
            let ch = f.channel;
            assert!(matches!(f.body, FrameBody::Amqp(Performative::Begin(_), _)));
            let begin = Begin {
                remote_channel: Some(ch),
                next_outgoing_id: 0,
                incoming_window: 100,
                outgoing_window: 100,
                ..Default::default()
            };
            st.send_amqp(0, &Performative::Begin(begin), None)
                .await
                .unwrap();
            // end
            let f = st.read_frame().await.unwrap();
            assert!(matches!(f.body, FrameBody::Amqp(Performative::End(_), _)));
            st.send_amqp(0, &Performative::End(End { error: None }), None)
                .await
                .unwrap();
            // close
            loop {
                match st.read_frame().await {
                    Ok(f) if matches!(f.body, FrameBody::Amqp(Performative::Close(_), _)) => {
                        st.send_amqp(0, &Performative::Close(Close { error: None }), None)
                            .await
                            .unwrap();
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let transport = FramedTransport::new(client, 1 << 16);
        let (tx, rx) = mpsc::channel(16);
        let driver = Driver::open(
            transport,
            Arc::new(Config::default()),
            noop_metrics(),
            EventBus::default(),
            rx,
        )
        .await
        .unwrap();
        let run = tokio::spawn(driver.run());

        // begin a session
        let (btx, brx) = oneshot::channel();
        let (evt_tx, _evt_rx) = mpsc::unbounded_channel();
        let begin = Begin {
            incoming_window: 100,
            outgoing_window: 100,
            handle_max: 100,
            ..Default::default()
        };
        tx.send(DriverCommand::BeginSession {
            begin: Box::new(begin),
            events: evt_tx,
            reply: btx,
        })
        .await
        .unwrap();
        let opened = brx.await.unwrap().unwrap();
        assert_eq!(opened.channel, crate::ids::ChannelId(0));

        // end the session
        let (etx, erx) = oneshot::channel();
        tx.send(DriverCommand::EndSession {
            channel: opened.channel,
            error: None,
            reply: etx,
        })
        .await
        .unwrap();
        erx.await.unwrap().unwrap();

        // close the connection
        let (ctx, crx) = oneshot::channel();
        tx.send(DriverCommand::CloseConnection {
            error: None,
            reply: ctx,
        })
        .await
        .unwrap();
        crx.await.unwrap().unwrap();
        run.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn attach_send_and_settle() {
        use crate::proto::{LinkEvent, SessionEvent};
        use crate::types::definitions::Role;
        use crate::types::messaging::{Accepted, DeliveryState, Source, Target, TargetArchetype};
        use crate::types::performatives::{Attach, Begin, Disposition, Flow, Open};

        let (client, server) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(async move {
            let mut st = FramedTransport::new(server, 1 << 16);
            // open + begin
            let _ = st.read_frame().await.unwrap();
            st.send_amqp(0, &Performative::Open(Open::new("broker")), None)
                .await
                .unwrap();
            let f = st.read_frame().await.unwrap();
            let ch = f.channel;
            st.send_amqp(
                0,
                &Performative::Begin(Begin {
                    remote_channel: Some(ch),
                    next_outgoing_id: 0,
                    incoming_window: 100,
                    outgoing_window: 100,
                    ..Default::default()
                }),
                None,
            )
            .await
            .unwrap();
            // attach (our sender) → respond as receiver + grant credit
            let f = st.read_frame().await.unwrap();
            let attach = match f.body {
                FrameBody::Amqp(Performative::Attach(a), _) => a,
                other => panic!("expected attach, got {other:?}"),
            };
            assert_eq!(attach.role, Role::Sender);
            st.send_amqp(
                0,
                &Performative::Attach(Attach {
                    name: attach.name.clone(),
                    handle: 0,
                    role: Role::Receiver,
                    source: Some(Source::default()),
                    target: Some(TargetArchetype::from(Target::new("queue"))),
                    ..Default::default()
                }),
                None,
            )
            .await
            .unwrap();
            st.send_amqp(
                0,
                &Performative::Flow(Flow {
                    next_incoming_id: Some(0),
                    incoming_window: 100,
                    next_outgoing_id: 0,
                    outgoing_window: 100,
                    handle: Some(0),
                    delivery_count: Some(0),
                    link_credit: Some(10),
                    ..Default::default()
                }),
                None,
            )
            .await
            .unwrap();
            // read the transfer, accept + settle it
            let f = st.read_frame().await.unwrap();
            let (transfer, payload) = match f.body {
                FrameBody::Amqp(Performative::Transfer(t), p) => (t, p),
                other => panic!("expected transfer, got {other:?}"),
            };
            assert!(payload.is_some(), "transfer carried a message payload");
            let id = transfer.delivery_id.unwrap();
            st.send_amqp(
                0,
                &Performative::Disposition(Disposition {
                    role: Role::Receiver,
                    first: id,
                    last: None,
                    settled: true,
                    state: Some(DeliveryState::Accepted(Accepted::default())),
                    batchable: false,
                }),
                None,
            )
            .await
            .unwrap();
            // drain until close
            loop {
                match st.read_frame().await {
                    Ok(f) if matches!(f.body, FrameBody::Amqp(Performative::Close(_), _)) => {
                        st.send_amqp(0, &Performative::Close(Close { error: None }), None)
                            .await
                            .unwrap();
                        break;
                    }
                    Ok(_) => continue,
                    Err(_) => break,
                }
            }
        });

        let transport = FramedTransport::new(client, 1 << 16);
        let (tx, rx) = mpsc::channel(32);
        let driver = Driver::open(
            transport,
            Arc::new(Config::default()),
            noop_metrics(),
            EventBus::default(),
            rx,
        )
        .await
        .unwrap();
        let run = tokio::spawn(driver.run());

        // begin
        let (btx, brx) = oneshot::channel();
        let (sevt, _sevt_rx) = mpsc::unbounded_channel::<SessionEvent>();
        tx.send(DriverCommand::BeginSession {
            begin: Box::new(Begin {
                incoming_window: 100,
                outgoing_window: 100,
                handle_max: 100,
                ..Default::default()
            }),
            events: sevt,
            reply: btx,
        })
        .await
        .unwrap();
        let opened = brx.await.unwrap().unwrap();

        // attach sender
        let (atx, arx) = oneshot::channel();
        let (levt, _levt_rx) = mpsc::channel::<LinkEvent>(32);
        tx.send(DriverCommand::AttachLink {
            channel: opened.channel,
            attach: Box::new(Attach {
                name: "test-sender".into(),
                handle: 0,
                role: Role::Sender,
                source: Some(Source::default()),
                target: Some(TargetArchetype::from(Target::new("queue"))),
                initial_delivery_count: Some(0),
                ..Default::default()
            }),
            credit_mode: crate::config::CreditMode::Manual,
            events: levt,
            reply: atx,
        })
        .await
        .unwrap();
        let attached = arx.await.unwrap().unwrap();

        // send a message, await its outcome
        let (stx, srx) = oneshot::channel();
        let body = crate::codec::to_vec(&crate::types::messaging::Message::text("hello"));
        tx.send(DriverCommand::SendTransfer {
            channel: opened.channel,
            handle: attached.handle,
            body: bytes::Bytes::from(body),
            settled: false,
            message_format: 0,
            state: None,
            reply: Some(stx),
        })
        .await
        .unwrap();
        let outcome = srx.await.unwrap().unwrap();
        assert!(matches!(outcome, DeliveryState::Accepted(_)));

        // close
        let (ctx, crx) = oneshot::channel();
        tx.send(DriverCommand::CloseConnection {
            error: None,
            reply: ctx,
        })
        .await
        .unwrap();
        crx.await.unwrap().unwrap();
        run.await.unwrap().unwrap();
        server_task.await.unwrap();
    }

    #[tokio::test]
    async fn rejects_begin_for_unknown_channel() {
        use crate::types::performatives::{Begin, Open};

        let (client, server) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(async move {
            let mut st = FramedTransport::new(server, 1 << 16);
            let _ = st.read_frame().await.unwrap(); // open
            st.send_amqp(0, &Performative::Open(Open::new("server")), None)
                .await
                .unwrap();
            // Unsolicited begin referencing a local channel we never allocated.
            st.send_amqp(
                0,
                &Performative::Begin(Begin {
                    remote_channel: Some(99),
                    next_outgoing_id: 0,
                    incoming_window: 100,
                    outgoing_window: 100,
                    ..Default::default()
                }),
                None,
            )
            .await
            .unwrap();
            let _ = st.read_frame().await; // peer observes the drop
        });

        let transport = FramedTransport::new(client, 1 << 16);
        let (_tx, rx) = mpsc::channel(16);
        let driver = Driver::open(
            transport,
            Arc::new(Config::default()),
            noop_metrics(),
            EventBus::default(),
            rx,
        )
        .await
        .unwrap();

        let err = driver.run().await.unwrap_err();
        assert_eq!(err.kind(), ErrorKind::ProtocolViolation);
        let _ = server_task.await;
    }

    #[tokio::test]
    async fn close_on_all_handles_dropped() {
        let (client, server) = tokio::io::duplex(1 << 16);
        let server_task = tokio::spawn(mock_peer(server));

        let transport = FramedTransport::new(client, 1 << 16);
        let (tx, rx) = mpsc::channel(16);
        let driver = Driver::open(
            transport,
            Arc::new(Config::default()),
            noop_metrics(),
            EventBus::default(),
            rx,
        )
        .await
        .unwrap();
        let run = tokio::spawn(driver.run());

        drop(tx); // all handles gone → driver closes
        run.await.unwrap().unwrap();
        server_task.await.unwrap();
    }
}
