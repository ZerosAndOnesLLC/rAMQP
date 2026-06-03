//! Transparent mid-stream reconnect (WP-6.4): a "virtual driver" supervisor.
//!
//! The supervisor speaks the exact same [`DriverCommand`] / event protocol as a
//! real connection driver, so ordinary [`Session`](crate::Session) /
//! [`Producer`](crate::Producer) / [`Consumer`](crate::Consumer) handles talk to
//! it unchanged. It multiplexes those handles onto a *real* driver and, when that
//! driver dies (network drop, broker restart), reconnects with jittered backoff,
//! re-begins every session and re-attaches every link, and re-pumps their event
//! streams — all without the handles noticing.
//!
//! Stable virtual ids decouple the handles from the connection-specific channel /
//! handle numbers: the supervisor maps each command's virtual ids to the live
//! connection's real ids, and rewrites them on every reconnect. In-flight awaited
//! sends whose disposition is lost in a drop are **replayed** on the new link
//! (the [`Resend`](crate::link::resume::ResumeAction::Resend) arm of the resume
//! matrix — at-least-once, may duplicate). Commands issued while disconnected
//! block until the link is back, so `producer.send(..).await` simply waits out
//! the reconnect.
//!
//! Receiver settlement that straddles a reconnect is best-effort: a disposition
//! for a delivery from a previous epoch is dropped (the broker redelivers the
//! still-unsettled message after re-attach), preserving at-least-once.

use std::collections::HashMap;
use std::future::pending;
use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::api::connection::Connection;
use crate::config::{Config, CreditMode, ReconnectConfig};
use crate::error::{ConnectError, ErrorKind, LinkError, SendError, SessionError};
use crate::ids::{ChannelId, Handle};
use crate::observe::{ConnectionEvent, EventBus, SharedMetrics};
use crate::proto::{DriverCommand, LinkAttached, Reply, SessionEvent, SessionOpened};
use crate::resilience::supervisor::Backoff;
use crate::sasl::SaslProfile;
use crate::transport::{Address, TlsConfig};
use crate::types::messaging::DeliveryState;
use crate::types::performatives::{Attach, Begin};

/// Capacity of the supervisor→handle link-event relay. Bounded so a slow
/// consumer still back-pressures the broker (preserving the consumer-driven
/// credit invariant); the extra slack over the credit window is small.
const RELAY_CAPACITY: usize = 256;

/// A logical (handle-visible) sender/receiver link, replayed on reconnect.
struct LogicalLink {
    /// The original attach (link name is stable; the driver re-assigns `handle`).
    attach: Attach,
    credit_mode: CreditMode,
    /// The handle's link-event channel (we re-pump into it each epoch).
    handle_events: mpsc::Sender<crate::proto::LinkEvent>,
    /// The current connection's real handle (`None` while disconnected).
    real_handle: Option<u32>,
}

/// A logical (handle-visible) session, replayed on reconnect.
struct LogicalSession {
    begin: Begin,
    handle_events: mpsc::UnboundedSender<SessionEvent>,
    real_channel: Option<u16>,
    links: HashMap<u32, LogicalLink>,
    next_vhandle: u32,
}

/// A send to replay on the new connection (its disposition was lost in a drop).
struct ReplayReq {
    vchan: u16,
    vhandle: u32,
    body: Bytes,
    settled: bool,
    message_format: u32,
    reply: Reply<DeliveryState, SendError>,
}

/// What the supervisor woke for.
enum Wake {
    DriverDied,
    Command(Option<DriverCommand>),
    Replay(Option<ReplayReq>),
}

/// The reconnect supervisor: a virtual driver in front of a real one.
struct Supervisor {
    addr: Address,
    config: Arc<Config>,
    metrics: SharedMetrics,
    tls: TlsConfig,
    profile: SaslProfile,
    events: EventBus,
    reconnect: ReconnectConfig,

    commands_rx: mpsc::Receiver<DriverCommand>,
    replay_tx: mpsc::UnboundedSender<ReplayReq>,
    replay_rx: mpsc::UnboundedReceiver<ReplayReq>,

    inner: Option<mpsc::Sender<DriverCommand>>,
    driver_join: Option<JoinHandle<Result<(), ConnectError>>>,

    sessions: HashMap<u16, LogicalSession>,
    next_vchan: u16,
    stopping: bool,
}

/// Establish one real connection and decompose it into (command sink, driver).
async fn establish(
    addr: &Address,
    config: &Arc<Config>,
    metrics: &SharedMetrics,
    profile: &SaslProfile,
    tls: &TlsConfig,
) -> Result<
    (
        mpsc::Sender<DriverCommand>,
        JoinHandle<Result<(), ConnectError>>,
    ),
    ConnectError,
> {
    let conn = Connection::establish(
        addr.clone(),
        (**config).clone(),
        metrics.clone(),
        profile.clone(),
        tls.clone(),
    )
    .await?;
    Ok(conn.into_driver_parts())
}

/// Open a transparently-reconnecting connection. The initial connect is
/// fail-fast; after it succeeds, drops are healed per `config.connection.reconnect`.
pub(crate) async fn connect_supervised(
    addr: Address,
    config: Config,
    metrics: SharedMetrics,
    profile: SaslProfile,
    tls: TlsConfig,
) -> Result<Connection, ConnectError> {
    let config = Arc::new(config);
    let (inner, join) = establish(&addr, &config, &metrics, &profile, &tls).await?;

    let events = EventBus::default();
    events.publish(ConnectionEvent::Connected);

    let (cmd_tx, cmd_rx) = mpsc::channel(config.connection.command_buffer);
    let (replay_tx, replay_rx) = mpsc::unbounded_channel();
    let reconnect = config.connection.reconnect.clone();

    let sup = Supervisor {
        addr,
        config: config.clone(),
        metrics,
        tls,
        profile,
        events: events.clone(),
        reconnect,
        commands_rx: cmd_rx,
        replay_tx,
        replay_rx,
        inner: Some(inner),
        driver_join: Some(join),
        sessions: HashMap::new(),
        next_vchan: 0,
        stopping: false,
    };
    let join = tokio::spawn(sup.run());
    Ok(Connection::from_parts(cmd_tx, events, config, join))
}

impl Supervisor {
    async fn run(mut self) -> Result<(), ConnectError> {
        while !self.stopping {
            let has_driver = self.inner.is_some();
            let wake = {
                let driver_join = &mut self.driver_join;
                let commands_rx = &mut self.commands_rx;
                let replay_rx = &mut self.replay_rx;
                tokio::select! {
                    biased;
                    _ = async {
                        match driver_join {
                            Some(j) => { let _ = j.await; }
                            None => pending::<()>().await,
                        }
                    }, if has_driver => Wake::DriverDied,
                    c = commands_rx.recv() => Wake::Command(c),
                    r = replay_rx.recv() => Wake::Replay(r),
                }
            };
            match wake {
                Wake::DriverDied => self.on_death().await,
                Wake::Command(Some(cmd)) => self.handle_command(cmd).await,
                Wake::Command(None) => {
                    self.shutdown().await;
                    break;
                }
                Wake::Replay(Some(req)) => self.handle_replay(req).await,
                Wake::Replay(None) => {}
            }
        }
        Ok(())
    }

    fn real_ids(&self, vchan: u16, vhandle: u32) -> Option<(u16, u32)> {
        let s = self.sessions.get(&vchan)?;
        let rc = s.real_channel?;
        let rh = s.links.get(&vhandle)?.real_handle?;
        Some((rc, rh))
    }

    async fn handle_command(&mut self, cmd: DriverCommand) {
        match cmd {
            DriverCommand::BeginSession {
                begin,
                events,
                reply,
            } => {
                let vchan = self.next_vchan;
                self.next_vchan = self.next_vchan.wrapping_add(1);
                self.sessions.insert(
                    vchan,
                    LogicalSession {
                        begin: *begin,
                        handle_events: events,
                        real_channel: None,
                        links: HashMap::new(),
                        next_vhandle: 0,
                    },
                );
                match self.open_session(vchan).await {
                    Ok(opened) => {
                        let _ = reply.send(Ok(SessionOpened {
                            channel: ChannelId(vchan),
                            session_id: opened.session_id,
                        }));
                    }
                    Err(e) => {
                        self.sessions.remove(&vchan);
                        let _ = reply.send(Err(e));
                    }
                }
            }
            DriverCommand::AttachLink {
                channel,
                attach,
                credit_mode,
                events,
                reply,
            } => {
                let vchan = channel.value();
                let Some(session) = self.sessions.get_mut(&vchan) else {
                    let _ = reply.send(Err(LinkError::msg(
                        ErrorKind::NotConnected,
                        "no such session",
                    )));
                    return;
                };
                let vhandle = session.next_vhandle;
                session.next_vhandle += 1;
                session.links.insert(
                    vhandle,
                    LogicalLink {
                        attach: *attach,
                        credit_mode,
                        handle_events: events,
                        real_handle: None,
                    },
                );
                match self.open_link(vchan, vhandle).await {
                    Ok(attached) => {
                        let _ = reply.send(Ok(LinkAttached {
                            handle: Handle(vhandle),
                            remote: attached.remote,
                        }));
                    }
                    Err(e) => {
                        if let Some(s) = self.sessions.get_mut(&vchan) {
                            s.links.remove(&vhandle);
                        }
                        let _ = reply.send(Err(e));
                    }
                }
            }
            DriverCommand::EndSession {
                channel,
                error,
                reply,
            } => {
                let vchan = channel.value();
                let real = self.sessions.get(&vchan).and_then(|s| s.real_channel);
                self.sessions.remove(&vchan);
                match (self.inner.clone(), real) {
                    (Some(inner), Some(rc)) => {
                        let _ = inner
                            .send(DriverCommand::EndSession {
                                channel: ChannelId(rc),
                                error,
                                reply,
                            })
                            .await;
                    }
                    _ => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            DriverCommand::DetachLink {
                channel,
                handle,
                closed,
                error,
                reply,
            } => {
                let vchan = channel.value();
                let vhandle = handle.value();
                let real = self.real_ids(vchan, vhandle);
                if let Some(s) = self.sessions.get_mut(&vchan) {
                    s.links.remove(&vhandle);
                }
                match (self.inner.clone(), real) {
                    (Some(inner), Some((rc, rh))) => {
                        let _ = inner
                            .send(DriverCommand::DetachLink {
                                channel: ChannelId(rc),
                                handle: Handle(rh),
                                closed,
                                error,
                                reply,
                            })
                            .await;
                    }
                    _ => {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
            DriverCommand::SendTransfer {
                channel,
                handle,
                body,
                settled,
                message_format,
                reply,
            } => {
                self.send_transfer(
                    channel.value(),
                    handle.value(),
                    body,
                    settled,
                    message_format,
                    reply,
                )
                .await;
            }
            DriverCommand::SendDisposition {
                channel,
                handle,
                first,
                last,
                state,
                settled,
                reply,
            } => match (
                self.inner.clone(),
                self.real_ids(channel.value(), handle.value()),
            ) {
                (Some(inner), Some((rc, rh))) => {
                    let _ = inner
                        .send(DriverCommand::SendDisposition {
                            channel: ChannelId(rc),
                            handle: Handle(rh),
                            first,
                            last,
                            state,
                            settled,
                            reply,
                        })
                        .await;
                }
                // Stale epoch: the broker redelivers the unsettled message.
                _ => {
                    if let Some(reply) = reply {
                        let _ = reply.send(Ok(()));
                    }
                }
            },
            DriverCommand::SendFlow { channel, mut flow } => {
                let vchan = channel.value();
                if let (Some(inner), Some((rc, real_handle))) = (
                    self.inner.clone(),
                    self.sessions.get(&vchan).and_then(|s| {
                        let rc = s.real_channel?;
                        let rh = flow
                            .handle
                            .and_then(|vh| s.links.get(&vh))
                            .and_then(|l| l.real_handle);
                        Some((rc, rh))
                    }),
                ) {
                    flow.handle = real_handle;
                    let _ = inner
                        .send(DriverCommand::SendFlow {
                            channel: ChannelId(rc),
                            flow,
                        })
                        .await;
                }
            }
            DriverCommand::GrantCredit {
                channel,
                handle,
                credit,
            } => {
                if let (Some(inner), Some((rc, rh))) = (
                    self.inner.clone(),
                    self.real_ids(channel.value(), handle.value()),
                ) {
                    let _ = inner
                        .send(DriverCommand::GrantCredit {
                            channel: ChannelId(rc),
                            handle: Handle(rh),
                            credit,
                        })
                        .await;
                }
            }
            DriverCommand::CloseConnection { error, reply } => {
                self.stopping = true;
                match self.inner.clone() {
                    Some(inner) => {
                        let _ = inner
                            .send(DriverCommand::CloseConnection { error, reply })
                            .await;
                        if let Some(join) = self.driver_join.take() {
                            let _ = join.await;
                        }
                    }
                    None => {
                        let _ = reply.send(Ok(()));
                    }
                }
                self.events.publish(ConnectionEvent::Closed {
                    error: false,
                    reason: "closed".into(),
                });
            }
        }
    }

    async fn send_transfer(
        &mut self,
        vchan: u16,
        vhandle: u32,
        body: Bytes,
        settled: bool,
        message_format: u32,
        reply: Option<Reply<DeliveryState, SendError>>,
    ) {
        let routed = self.inner.clone().zip(self.real_ids(vchan, vhandle));
        let Some((inner, (rc, rh))) = routed else {
            if let Some(reply) = reply {
                let _ = reply.send(Err(SendError::msg(
                    ErrorKind::NotConnected,
                    "connection lost",
                )));
            }
            return;
        };
        match reply {
            // Awaited send: proxy the outcome so we can replay on a drop.
            Some(handle_reply) => {
                let (ptx, prx) = oneshot::channel();
                let body_for_replay = body.clone();
                if inner
                    .send(DriverCommand::SendTransfer {
                        channel: ChannelId(rc),
                        handle: Handle(rh),
                        body,
                        settled,
                        message_format,
                        reply: Some(ptx),
                    })
                    .await
                    .is_err()
                {
                    let _ = handle_reply.send(Err(SendError::msg(
                        ErrorKind::NotConnected,
                        "connection lost",
                    )));
                    return;
                }
                let replay_tx = self.replay_tx.clone();
                tokio::spawn(async move {
                    match prx.await {
                        Ok(result) => {
                            let _ = handle_reply.send(result);
                        }
                        // Driver died before settling: replay on the new link.
                        Err(_) => {
                            let _ = replay_tx.send(ReplayReq {
                                vchan,
                                vhandle,
                                body: body_for_replay,
                                settled,
                                message_format,
                                reply: handle_reply,
                            });
                        }
                    }
                });
            }
            // Pre-settled fire-and-forget: best-effort (settled implies no guarantee).
            None => {
                let _ = inner
                    .send(DriverCommand::SendTransfer {
                        channel: ChannelId(rc),
                        handle: Handle(rh),
                        body,
                        settled,
                        message_format,
                        reply: None,
                    })
                    .await;
            }
        }
    }

    async fn handle_replay(&mut self, req: ReplayReq) {
        self.send_transfer(
            req.vchan,
            req.vhandle,
            req.body,
            req.settled,
            req.message_format,
            Some(req.reply),
        )
        .await;
    }

    /// (Re)begin a logical session on the current driver and re-pump its events.
    async fn open_session(&mut self, vchan: u16) -> Result<SessionOpened, SessionError> {
        let inner = self
            .inner
            .clone()
            .ok_or_else(|| SessionError::msg(ErrorKind::NotConnected, "connection lost"))?;
        let (begin, handle_events) = {
            let s = self
                .sessions
                .get(&vchan)
                .ok_or_else(|| SessionError::msg(ErrorKind::NotConnected, "no such session"))?;
            (s.begin.clone(), s.handle_events.clone())
        };
        let (etx, mut erx) = mpsc::unbounded_channel();
        let (rtx, rrx) = oneshot::channel();
        inner
            .send(DriverCommand::BeginSession {
                begin: Box::new(begin),
                events: etx,
                reply: rtx,
            })
            .await
            .map_err(|_| SessionError::msg(ErrorKind::NotConnected, "connection lost"))?;
        let opened = rrx
            .await
            .map_err(|_| SessionError::msg(ErrorKind::Cancelled, "driver dropped"))??;
        if let Some(s) = self.sessions.get_mut(&vchan) {
            s.real_channel = Some(opened.channel.value());
        }
        // Relay this epoch's session events to the handle.
        tokio::spawn(async move {
            while let Some(ev) = erx.recv().await {
                if handle_events.send(ev).is_err() {
                    break;
                }
            }
        });
        Ok(opened)
    }

    /// (Re)attach a logical link on the current driver and re-pump its events.
    async fn open_link(&mut self, vchan: u16, vhandle: u32) -> Result<LinkAttached, LinkError> {
        let inner = self
            .inner
            .clone()
            .ok_or_else(|| LinkError::msg(ErrorKind::NotConnected, "connection lost"))?;
        let (rc, attach, credit_mode, handle_events) = {
            let s = self
                .sessions
                .get(&vchan)
                .ok_or_else(|| LinkError::msg(ErrorKind::NotConnected, "no such session"))?;
            let rc = s
                .real_channel
                .ok_or_else(|| LinkError::msg(ErrorKind::NotConnected, "session not open"))?;
            let l = s
                .links
                .get(&vhandle)
                .ok_or_else(|| LinkError::msg(ErrorKind::NotConnected, "no such link"))?;
            (rc, l.attach.clone(), l.credit_mode, l.handle_events.clone())
        };
        let (etx, mut erx) = mpsc::channel(RELAY_CAPACITY);
        let (rtx, rrx) = oneshot::channel();
        inner
            .send(DriverCommand::AttachLink {
                channel: ChannelId(rc),
                attach: Box::new(attach),
                credit_mode,
                events: etx,
                reply: rtx,
            })
            .await
            .map_err(|_| LinkError::msg(ErrorKind::NotConnected, "connection lost"))?;
        let attached = rrx
            .await
            .map_err(|_| LinkError::msg(ErrorKind::Cancelled, "driver dropped"))??;
        if let Some(l) = self
            .sessions
            .get_mut(&vchan)
            .and_then(|s| s.links.get_mut(&vhandle))
        {
            l.real_handle = Some(attached.handle.value());
        }
        tokio::spawn(async move {
            while let Some(ev) = erx.recv().await {
                if handle_events.send(ev).await.is_err() {
                    break;
                }
            }
        });
        Ok(attached)
    }

    async fn on_death(&mut self) {
        self.inner = None;
        self.driver_join = None;
        if self.stopping {
            return;
        }
        self.events.publish(ConnectionEvent::Closed {
            error: true,
            reason: "connection lost; reconnecting".into(),
        });
        self.reconnect().await;
    }

    async fn reconnect(&mut self) {
        let mut backoff = Backoff::new(self.reconnect.clone());
        loop {
            if self.stopping {
                return;
            }
            self.events.publish(ConnectionEvent::Reconnecting {
                attempt: backoff.attempt() + 1,
            });
            match establish(
                &self.addr,
                &self.config,
                &self.metrics,
                &self.profile,
                &self.tls,
            )
            .await
            {
                Ok((inner, join)) => {
                    self.inner = Some(inner);
                    self.driver_join = Some(join);
                    self.metrics.on_reconnect(backoff.attempt() + 1);
                    if self.replay_topology().await {
                        self.events.publish(ConnectionEvent::Connected);
                        return;
                    }
                    // The fresh driver died during replay: drop it and retry.
                    self.inner = None;
                    self.driver_join = None;
                }
                Err(e) if e.is_retryable() => match backoff.next_delay() {
                    Some(delay) => tokio::time::sleep(delay).await,
                    None => return self.give_up("reconnect budget exhausted"),
                },
                Err(_) => return self.give_up("reconnect failed (non-retryable)"),
            }
        }
    }

    /// Re-begin every session and re-attach every link on the new connection.
    /// Returns `false` if the connection died mid-replay.
    async fn replay_topology(&mut self) -> bool {
        let vchans: Vec<u16> = {
            let mut v: Vec<u16> = self.sessions.keys().copied().collect();
            v.sort_unstable();
            v
        };
        for vchan in vchans {
            if self.open_session(vchan).await.is_err() {
                return false;
            }
            let vhandles: Vec<u32> = {
                let mut v: Vec<u32> = self
                    .sessions
                    .get(&vchan)
                    .map(|s| s.links.keys().copied().collect())
                    .unwrap_or_default();
                v.sort_unstable();
                v
            };
            for vhandle in vhandles {
                if self.open_link(vchan, vhandle).await.is_err() {
                    return false;
                }
            }
        }
        true
    }

    fn give_up(&mut self, reason: &str) {
        self.stopping = true;
        self.events.publish(ConnectionEvent::Closed {
            error: true,
            reason: reason.into(),
        });
    }

    async fn shutdown(&mut self) {
        self.stopping = true;
        if let Some(inner) = self.inner.clone() {
            let (tx, rx) = oneshot::channel();
            if inner
                .send(DriverCommand::CloseConnection {
                    error: None,
                    reply: tx,
                })
                .await
                .is_ok()
            {
                let _ = rx.await;
            }
        }
        if let Some(join) = self.driver_join.take() {
            let _ = join.await;
        }
    }
}
