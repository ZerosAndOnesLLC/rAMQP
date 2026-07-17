//! Session state machine (WP-3.1) and link orchestration (Phase 4).
//!
//! A [`Session`] lives inside the connection driver (single-owner, no locks). It
//! owns the begin/end lifecycle, the flow-control windows, the link registry,
//! and all per-link state, and drives sends/receives by queueing frames onto the
//! driver's transport (the driver flushes once per event-loop iteration).

use std::collections::HashMap;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::config::{CreditMode, SessionConfig};
use crate::error::{ConnectError, ErrorKind, LinkError, RemoteError, SessionError};
use crate::ids::{ChannelId, DeliveryId, Handle, SessionId};
use crate::link::Link;
use crate::link::credit::LinkCredit;
use crate::link::delivery::PartialDelivery;
use crate::link::receiver::ReceiverLink;
use crate::link::sender::{PendingSend, SenderLink};
use crate::observe::SharedMetrics;
use crate::proto::{IncomingDelivery, LinkAttached, LinkEvent, Reply, SessionEvent, SessionOpened};
use crate::session::registry::{HandleAllocator, RemoteHandleMap};
use crate::session::window::SessionWindows;
use crate::transport::IoStream;
use crate::transport::frame::FramedTransport;
use crate::types::definitions::{Error as AmqpError, ReceiverSettleMode, Role};
use crate::types::messaging::{Accepted, DeliveryState};
use crate::types::performatives::{
    Attach, Begin, Detach, Disposition, End, Flow, Performative, Transfer,
};

/// The lifecycle phase of a session.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionPhase {
    /// `begin` sent, awaiting the peer's `begin`.
    BeginSent,
    /// Mapped (both `begin`s exchanged).
    Mapped,
    /// `end` sent, awaiting the peer's `end`.
    EndSent,
    /// Fully ended.
    Ended,
}

/// The outcome of accepting a peer-initiated attach: the local endpoint the
/// session created to mirror it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AcceptedLink {
    /// Our local handle for the link.
    pub handle: Handle,
    /// The role *we* play on the link (mirror of the peer's).
    pub role: Role,
}

/// Diagnostics snapshot of one sender link's backlog (see
/// [`Session::sender_backlog`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SenderBacklog {
    /// The link's local handle.
    pub handle: u32,
    /// Messages queued locally, not yet sent (credit/window-gated).
    pub outbox: usize,
    /// Sent deliveries the peer has not settled.
    pub unsettled: usize,
    /// Send futures awaiting a terminal outcome.
    pub pending: usize,
    /// The (min, max) unsettled delivery ids, when any.
    pub unsettled_ids: Option<(u32, u32)>,
}

/// Per-session protocol state owned by the driver.
pub struct Session {
    /// Process-local logical id (stable across reconnects).
    pub session_id: SessionId,
    /// Our outgoing channel for this session.
    pub local_channel: u16,
    /// The peer's channel (learned from its `begin`).
    pub remote_channel: Option<u16>,
    phase: SessionPhase,
    windows: SessionWindows,
    handles: HandleAllocator,
    remote_handles: RemoteHandleMap,
    links: HashMap<u32, Link>,
    /// Index of (link name, our local role) -> local handle, so a peer `attach`
    /// response binds in O(1) instead of scanning every link. Keyed by role as
    /// well as name because AMQP 1.0 §2.6.1 identifies a link by container-id +
    /// name + *role*: a sender and a receiver may legitimately share a name on
    /// one session (e.g. a Qpid Proton client opening both to the same address).
    /// Kept in lockstep with `links` via [`Session::forget_link`].
    link_handles: HashMap<(String, Role), u32>,
    events: mpsc::UnboundedSender<SessionEvent>,
    pending_begin: Option<Reply<SessionOpened, SessionError>>,
    pending_end: Option<Reply<(), SessionError>>,
    metrics: SharedMetrics,
}

impl std::fmt::Debug for Session {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Session")
            .field("session_id", &self.session_id)
            .field("local_channel", &self.local_channel)
            .field("phase", &self.phase)
            .field("links", &self.links.len())
            .finish_non_exhaustive()
    }
}

impl Session {
    /// Create a session (in [`SessionPhase::BeginSent`]) from our outgoing
    /// `begin`, holding the begin reply until the peer responds.
    pub fn new(
        session_id: SessionId,
        local_channel: u16,
        begin: &Begin,
        events: mpsc::UnboundedSender<SessionEvent>,
        pending_begin: Reply<SessionOpened, SessionError>,
        metrics: SharedMetrics,
    ) -> Self {
        let mut windows = SessionWindows::new(&SessionConfig {
            incoming_window: begin.incoming_window,
            outgoing_window: begin.outgoing_window,
            handle_max: begin.handle_max,
        });
        windows.next_outgoing_id = begin.next_outgoing_id;
        Session {
            session_id,
            local_channel,
            remote_channel: None,
            phase: SessionPhase::BeginSent,
            windows,
            handles: HandleAllocator::new(begin.handle_max),
            remote_handles: RemoteHandleMap::default(),
            links: HashMap::new(),
            link_handles: HashMap::new(),
            events,
            pending_begin: Some(pending_begin),
            pending_end: None,
            metrics,
        }
    }

    /// The current lifecycle phase.
    pub fn phase(&self) -> SessionPhase {
        self.phase
    }

    /// Whether we initiated the end (and are awaiting the peer's `end`).
    pub fn is_ending(&self) -> bool {
        self.phase == SessionPhase::EndSent
    }

    /// Apply the peer's `begin`: bind its channel, learn its windows, become
    /// mapped, and complete the begin reply.
    ///
    /// Only a session that sent `begin` and is awaiting the peer's may map; a
    /// duplicate or out-of-phase begin is ignored rather than clobbering live
    /// flow-control state (the driver also rejects it as a connection error).
    pub fn on_peer_begin(&mut self, remote_channel: u16, begin: &Begin) {
        if self.phase != SessionPhase::BeginSent {
            return;
        }
        self.remote_channel = Some(remote_channel);
        self.windows.on_peer_begin(begin);
        self.phase = SessionPhase::Mapped;
        if let Some(reply) = self.pending_begin.take() {
            let _ = reply.send(Ok(SessionOpened {
                channel: ChannelId(self.local_channel),
                session_id: self.session_id,
            }));
        }
    }

    /// Accept a peer-initiated `begin` (server polarity): the peer opened the
    /// session, so its `begin` has no `remote-channel` and there is no local
    /// begin future awaiting it. The session is created directly in
    /// [`SessionPhase::Mapped`], and the returned `begin` is our response —
    /// the caller queues it on `local_channel` (its `remote-channel` names the
    /// peer's `remote_channel` per spec §2.7.2).
    #[allow(clippy::too_many_arguments)]
    pub fn accept_peer_begin(
        session_id: SessionId,
        local_channel: u16,
        remote_channel: u16,
        peer_begin: &Begin,
        config: &SessionConfig,
        events: mpsc::UnboundedSender<SessionEvent>,
        metrics: SharedMetrics,
    ) -> (Self, Begin) {
        let mut windows = SessionWindows::new(config);
        windows.on_peer_begin(peer_begin);
        // Our outbound handles must satisfy both bounds: what the peer accepts
        // (its handle-max) and what we advertise back (ours).
        let handle_max = config.handle_max.min(peer_begin.handle_max);
        let session = Session {
            session_id,
            local_channel,
            remote_channel: Some(remote_channel),
            phase: SessionPhase::Mapped,
            windows,
            handles: HandleAllocator::new(handle_max),
            remote_handles: RemoteHandleMap::default(),
            links: HashMap::new(),
            link_handles: HashMap::new(),
            events,
            pending_begin: None,
            pending_end: None,
            metrics,
        };
        let response = Begin {
            remote_channel: Some(remote_channel),
            next_outgoing_id: session.windows.next_outgoing_id,
            incoming_window: config.incoming_window,
            outgoing_window: config.outgoing_window,
            handle_max: config.handle_max,
            ..Default::default()
        };
        (session, response)
    }

    /// Mark that we have sent `end` and are awaiting the peer's.
    pub fn begin_end(&mut self, reply: Reply<(), SessionError>) {
        self.phase = SessionPhase::EndSent;
        self.pending_end = Some(reply);
    }

    /// Apply the peer's `end`: complete a pending end reply, or surface the end
    /// as a [`SessionEvent`] when the peer initiated it.
    pub fn on_peer_end(&mut self, error: Option<AmqpError>) {
        self.phase = SessionPhase::Ended;
        let result = match &error {
            Some(e) => Err(SessionError::from_remote(
                ErrorKind::PeerClosed,
                RemoteError::new(e.clone()),
            )),
            None => Ok(()),
        };
        if let Some(reply) = self.pending_end.take() {
            let _ = reply.send(result);
        } else {
            let _ = self.events.send(SessionEvent::Ended { error });
        }
    }

    /// Build an `end` performative.
    pub fn build_end(error: Option<AmqpError>) -> End {
        End { error }
    }

    // -----------------------------------------------------------------------
    // Link orchestration (Phase 4)
    // -----------------------------------------------------------------------

    /// Attach a link: allocate a handle, build the sender/receiver state, and
    /// queue the `attach`.
    pub fn attach_link<S: IoStream>(
        &mut self,
        mut attach: Attach,
        credit_mode: CreditMode,
        events: mpsc::Sender<LinkEvent>,
        reply: Reply<LinkAttached, LinkError>,
        transport: &mut FramedTransport<S>,
    ) {
        let handle = match self.handles.allocate() {
            Some(h) => h,
            None => {
                let _ = reply.send(Err(LinkError::msg(
                    ErrorKind::Capacity,
                    "handle-max exhausted",
                )));
                return;
            }
        };
        attach.handle = handle;
        let name = attach.name.clone();
        // Our local role for a self-initiated attach is `attach.role`.
        self.link_handles
            .insert((name.clone(), attach.role), handle);
        let link = match attach.role {
            Role::Sender => {
                if attach.initial_delivery_count.is_none() {
                    attach.initial_delivery_count = Some(0);
                }
                Link::Sender(SenderLink::new(
                    handle,
                    name,
                    events,
                    reply,
                    attach.snd_settle_mode,
                    credit_mode,
                ))
            }
            Role::Receiver => Link::Receiver(ReceiverLink::new(
                handle,
                name,
                events,
                reply,
                attach.rcv_settle_mode,
                credit_mode,
                attach.max_message_size,
            )),
        };
        transport.queue_amqp(self.local_channel, &Performative::Attach(attach), None);
        self.links.insert(handle, link);
    }

    /// Handle the peer's responding `attach`: bind handles, mark attached, grant
    /// initial receiver credit, and complete the attach reply.
    pub fn on_peer_attach<S: IoStream>(
        &mut self,
        attach: Attach,
        transport: &mut FramedTransport<S>,
    ) {
        // This is the peer's *response* to a link we initiated: the frame's role
        // is the peer's, so our local endpoint plays the opposite role.
        let Some(&local) = self
            .link_handles
            .get(&(attach.name.clone(), attach.role.opposite()))
        else {
            return;
        };
        // Only bind a link still awaiting its peer attach (ignore a duplicate
        // attach for an already-bound link).
        if self
            .links
            .get(&local)
            .is_none_or(|l| l.remote_handle().is_some())
        {
            return;
        }
        self.remote_handles.bind(attach.handle, local);

        let windows = self.windows;
        let local_channel = self.local_channel;
        let mut flow_to_send = None;

        if let Some(link) = self.links.get_mut(&local) {
            link.set_remote_handle(attach.handle);
            link.mark_attached();
            if let Link::Receiver(r) = link {
                // Adopt the peer's agreed receiver settle mode (first vs second).
                r.settle_mode = attach.rcv_settle_mode;
                if let Some(idc) = attach.initial_delivery_count {
                    r.credit.delivery_count = idc;
                }
                let grant = r.initial_credit();
                if grant > 0 {
                    r.credit.set_credit(grant);
                    self.metrics.on_credit(r.handle, r.credit.link_credit);
                    flow_to_send = Some(link_flow(r.handle, &r.credit, &windows));
                }
            }
            if let Some(reply) = link.take_pending_attach() {
                let _ = reply.send(Ok(LinkAttached {
                    handle: Handle(local),
                    remote: Box::new(attach),
                }));
            }
        }

        if let Some(flow) = flow_to_send {
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
        }
    }

    /// Whether the link an inbound `attach` (carrying `remote_role`) refers to
    /// was initiated locally — i.e. this attach is the peer's *response*,
    /// handled by [`Session::on_peer_attach`]. Matched by name **and** role
    /// (our local endpoint plays `remote_role.opposite()`), so a peer opening a
    /// same-named link in the *other* direction is correctly seen as a new
    /// peer-initiated attach for [`Session::accept_peer_attach`], not a response.
    pub fn knows_link(&self, name: &str, remote_role: Role) -> bool {
        self.link_handles
            .contains_key(&(name.to_string(), remote_role.opposite()))
    }

    /// Diagnostics: per sender link, what is still queued locally, what the
    /// peer has not settled, and how many send futures await an outcome.
    pub fn sender_backlog(&self) -> Vec<SenderBacklog> {
        self.links
            .iter()
            .filter_map(|(h, link)| match link {
                Link::Sender(s) => Some(SenderBacklog {
                    handle: *h,
                    outbox: s.outbox.len(),
                    unsettled: s.unsettled.len(),
                    pending: s.pending.len(),
                    unsettled_ids: s.unsettled.id_range(),
                }),
                _ => None,
            })
            .collect()
    }

    /// Accept a peer-initiated `attach` (server polarity): create the mirror
    /// endpoint (peer sender → our receiver; peer receiver → our sender),
    /// queue our responding `attach`, and — when we are the receiver —
    /// optionally grant `initial_credit` via a `flow`.
    ///
    /// The responding `attach` echoes the peer's `source`/`target` verbatim;
    /// a broker resolves/rewrites the address on `attach` *before* calling
    /// this (mutating the passed-in value), so what it passes is what the
    /// peer sees confirmed.
    ///
    /// `max_message_size` is our per-delivery cap when we mirror as receiver.
    pub fn accept_peer_attach<S: IoStream>(
        &mut self,
        attach: Attach,
        credit_mode: CreditMode,
        initial_credit: u32,
        max_message_size: Option<u64>,
        events: mpsc::Sender<LinkEvent>,
        transport: &mut FramedTransport<S>,
    ) -> Result<AcceptedLink, LinkError> {
        // We take the mirror role of the peer's.
        let our_role = attach.role.opposite();
        // Reject only a duplicate of the *same* link (name + our role); a
        // same-named link in the other direction is a distinct link (§2.6.1).
        if self
            .link_handles
            .contains_key(&(attach.name.clone(), our_role))
        {
            return Err(LinkError::msg(
                ErrorKind::ProtocolViolation,
                format!(
                    "duplicate attach for link name {:?} (role {:?})",
                    attach.name, our_role
                ),
            ));
        }
        let local = self.handles.allocate().ok_or_else(|| {
            LinkError::msg(ErrorKind::Capacity, "handle-max exhausted accepting attach")
        })?;

        let mut response = Attach {
            name: attach.name.clone(),
            handle: local,
            role: our_role,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: attach.rcv_settle_mode,
            source: attach.source.clone(),
            target: attach.target.clone(),
            ..Default::default()
        };

        let mut flow_to_send = None;
        let link = match our_role {
            Role::Receiver => {
                let mut r = ReceiverLink::accepted(
                    local,
                    attach.name.clone(),
                    events,
                    attach.rcv_settle_mode,
                    credit_mode,
                    max_message_size,
                );
                // Adopt the peer sender's initial delivery-count so credit
                // accounting starts aligned (spec §2.6.7).
                if let Some(idc) = attach.initial_delivery_count {
                    r.credit.delivery_count = idc;
                }
                response.max_message_size = max_message_size;
                if initial_credit > 0 {
                    r.credit.set_credit(initial_credit);
                    self.metrics.on_credit(local, r.credit.link_credit);
                    flow_to_send = Some(link_flow(local, &r.credit, &self.windows));
                }
                Link::Receiver(r)
            }
            Role::Sender => {
                // As the sender we declare our initial delivery-count (spec:
                // the sender MUST set it); credit arrives via the peer's flow.
                response.initial_delivery_count = Some(0);
                Link::Sender(SenderLink::accepted(
                    local,
                    attach.name.clone(),
                    events,
                    attach.snd_settle_mode,
                    credit_mode,
                ))
            }
        };

        let mut link = link;
        link.set_remote_handle(attach.handle);
        link.mark_attached();
        self.link_handles
            .insert((attach.name.clone(), our_role), local);
        self.remote_handles.bind(attach.handle, local);
        self.links.insert(local, link);

        transport.queue_amqp(self.local_channel, &Performative::Attach(response), None);
        if let Some(flow) = flow_to_send {
            transport.queue_amqp(self.local_channel, &Performative::Flow(flow), None);
        }
        Ok(AcceptedLink {
            handle: Handle(local),
            role: our_role,
        })
    }

    /// Refuse a peer-initiated attach we cannot honor (e.g. an unresolvable
    /// address), scoped to just that link. Per AMQP 1.0 §2.6.3 the responder
    /// completes the attach with the offending terminus set to null, then
    /// immediately detaches (closed) with the error. No link state is retained,
    /// and the session and its sibling links stay up — a bad address on one
    /// link must not tear the whole session down.
    pub fn refuse_peer_attach<S: IoStream>(
        &mut self,
        attach: &Attach,
        error: AmqpError,
        transport: &mut FramedTransport<S>,
    ) {
        let our_role = match attach.role {
            Role::Sender => Role::Receiver,
            Role::Receiver => Role::Sender,
        };
        // Name the link with a throwaway handle so the attach/detach pair is
        // well-formed; release it right back since we retain no link.
        let handle = self.handles.allocate();
        let local = handle.unwrap_or(0);
        // Null source AND target: the unambiguous "terminus refused" signal
        // regardless of which side the peer asked us to resolve.
        let response = Attach {
            name: attach.name.clone(),
            handle: local,
            role: our_role,
            snd_settle_mode: attach.snd_settle_mode,
            rcv_settle_mode: attach.rcv_settle_mode,
            source: None,
            target: None,
            ..Default::default()
        };
        transport.queue_amqp(self.local_channel, &Performative::Attach(response), None);
        transport.queue_amqp(
            self.local_channel,
            &Performative::Detach(Detach {
                handle: local,
                closed: true,
                error: Some(error),
            }),
            None,
        );
        if handle.is_some() {
            self.handles.release(local);
        }
    }

    /// Enqueue an outbound message on a sender link and try to flush it.
    #[allow(clippy::too_many_arguments)]
    pub fn send_transfer<S: IoStream>(
        &mut self,
        handle: u32,
        body: Bytes,
        settled: bool,
        message_format: u32,
        state: Option<DeliveryState>,
        reply: Option<Reply<DeliveryState, crate::error::SendError>>,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) {
        match self.links.get_mut(&handle) {
            Some(Link::Sender(s)) => s.outbox.push_back(PendingSend {
                body,
                settled,
                message_format,
                state,
                reply,
            }),
            _ => {
                if let Some(reply) = reply {
                    let _ = reply.send(Err(crate::error::SendError::msg(
                        ErrorKind::Detached,
                        "no such sender link",
                    )));
                }
                return;
            }
        }
        self.flush_sender(handle, transport, max_frame_size);
    }

    fn flush_sender<S: IoStream>(
        &mut self,
        handle: u32,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) {
        let local_channel = self.local_channel;
        let windows = &mut self.windows;
        let Some(Link::Sender(sender)) = self.links.get_mut(&handle) else {
            return;
        };
        let max_payload = max_payload_per_frame(max_frame_size);

        while !sender.outbox.is_empty() && sender.attached && sender.credit.can_send() {
            // Bound outstanding unsettled deliveries: if the broker grants credit
            // but withholds dispositions, pause unsettled sends (leaving them
            // queued) rather than growing the unsettled/pending maps without
            // limit. Pre-settled sends never accumulate, so they are not gated.
            let front = sender.outbox.front().expect("non-empty");
            if !front.settled
                && sender.unsettled.len() >= crate::link::sender::MAX_UNSETTLED_PER_LINK
            {
                break;
            }
            // A message takes `frame_count` transfer frames; only begin it if the
            // session outgoing-window can cover all of them (avoids over-drawing
            // the window mid-message).
            let body_len = front.body.len();
            let frame_count = body_len.div_ceil(max_payload).max(1) as u32;
            if windows.remote_incoming_window < frame_count {
                break;
            }

            let pending = sender.outbox.pop_front().expect("non-empty");
            let delivery_id = windows.next_outgoing_id;
            let tag = sender.next_delivery_tag();
            // Clone the tag for the unsettled map only when the delivery is
            // tracked; the pre-settled fast path moves the single allocation
            // straight into the frame with no extra refcount traffic.
            let tag_for_map = (!pending.settled).then(|| tag.clone());
            send_message_frames(
                transport,
                local_channel,
                handle,
                delivery_id,
                tag,
                pending.settled,
                pending.message_format,
                pending.state,
                &pending.body,
                max_payload,
                windows,
            );
            sender.credit.record_sent();
            if pending.settled {
                if let Some(reply) = pending.reply {
                    let _ = reply.send(Ok(DeliveryState::Accepted(Accepted::default())));
                }
            } else {
                sender.unsettled.insert(
                    delivery_id,
                    tag_for_map.expect("unsettled delivery has a tag"),
                    None,
                );
                self.metrics.on_inflight(1);
                if let Some(reply) = pending.reply {
                    sender
                        .pending
                        .insert(delivery_id, (reply, std::time::Instant::now()));
                }
            }
        }

        // Drain completion: if a drain is in progress and we have nothing more to
        // send, consume the remaining credit and echo a flow reporting it.
        if sender.credit.drain && sender.outbox.is_empty() && sender.credit.link_credit > 0 {
            sender.credit.delivery_count = sender
                .credit
                .delivery_count
                .wrapping_add(sender.credit.link_credit);
            sender.credit.link_credit = 0;
            let flow = link_flow(handle, &sender.credit, windows);
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
            sender.credit.drain = false;
        }
    }

    /// Handle an inbound transfer (assemble, account, emit, refill credit).
    pub async fn on_transfer<S: IoStream>(
        &mut self,
        transfer: Transfer,
        payload: Option<Bytes>,
        transport: &mut FramedTransport<S>,
    ) -> Result<(), ConnectError> {
        let Some(local) = self.remote_handles.resolve(transfer.handle) else {
            return Ok(());
        };
        let need_session_flow = self
            .windows
            .record_incoming(transfer.delivery_id.unwrap_or(0));

        let local_channel = self.local_channel;

        // Synchronous assembly + state updates; collect what to emit/queue.
        // Credit is consumer-driven (the Consumer replenishes as it reads), so
        // the driver never grants credit here — that decoupling is what caused a
        // slow consumer to back-pressure (and deadlock) the whole connection.
        let mut emit: Option<(LinkEvent, mpsc::Sender<LinkEvent>)> = None;
        let mut size_exceeded = false;

        if let Some(Link::Receiver(r)) = self.links.get_mut(&local) {
            if transfer.aborted {
                // Abandoned delivery: drop any partial, account the credit, emit nothing.
                r.partial = None;
                r.credit.record_received();
            } else {
                // A continuation transfer must carry the same delivery-id (or none).
                if let (Some(partial), Some(did)) = (r.partial.as_ref(), transfer.delivery_id)
                    && partial.delivery_id().value() != did
                {
                    return Err(ConnectError::msg(
                        ErrorKind::ProtocolViolation,
                        "interleaved delivery-id during multi-frame transfer",
                    ));
                }
                let payload = payload.unwrap_or_default();
                let cap = r.size_cap();
                if r.partial.is_none() && !transfer.more {
                    // Fast path: a single self-contained frame (nothing is being
                    // assembled and this transfer is not continued). The frame's
                    // payload `Bytes` becomes the message body directly — no copy
                    // into an assembly buffer.
                    if payload.len() as u64 > cap {
                        size_exceeded = true;
                    } else {
                        let delivery_id = DeliveryId(transfer.delivery_id.unwrap_or(0));
                        let tag = transfer.delivery_tag.clone().unwrap_or_default();
                        let settled = transfer.settled.unwrap_or(false);
                        r.credit.record_received();
                        if !settled {
                            r.unsettled.insert(delivery_id.value(), tag.clone(), None);
                            self.metrics.on_inflight(1);
                        }
                        let event = LinkEvent::Delivery(IncomingDelivery {
                            handle: Handle(r.handle),
                            delivery_id,
                            delivery_tag: tag,
                            settled,
                            state: transfer.state.clone(),
                            message: payload,
                        });
                        emit = Some((event, r.events.clone()));
                    }
                } else {
                    // Slow path: multi-frame assembly accumulates into a buffer.
                    let complete = if let Some(partial) = r.partial.as_mut() {
                        partial.append(&payload);
                        !transfer.more
                    } else {
                        let delivery_id = DeliveryId(transfer.delivery_id.unwrap_or(0));
                        let tag = transfer.delivery_tag.clone().unwrap_or_default();
                        let settled = transfer.settled.unwrap_or(false);
                        r.partial = Some(PartialDelivery::new(
                            delivery_id,
                            tag,
                            settled,
                            transfer.state.clone(),
                            &payload,
                        ));
                        !transfer.more
                    };

                    if r.partial.as_ref().map(|p| p.len() as u64).unwrap_or(0) > cap {
                        // Refuse oversized deliveries rather than assembling unbounded.
                        r.partial = None;
                        size_exceeded = true;
                    } else if complete {
                        let delivery = r.partial.take().expect("partial present").complete();
                        r.credit.record_received();
                        if !delivery.settled {
                            r.unsettled.insert(
                                delivery.delivery_id.value(),
                                delivery.delivery_tag.clone(),
                                None,
                            );
                            self.metrics.on_inflight(1);
                        }
                        let event = LinkEvent::Delivery(IncomingDelivery {
                            handle: Handle(r.handle),
                            delivery_id: delivery.delivery_id,
                            delivery_tag: delivery.delivery_tag.clone(),
                            settled: delivery.settled,
                            state: delivery.state().cloned(),
                            message: delivery.into_raw(),
                        });
                        emit = Some((event, r.events.clone()));
                    }
                }
            }
        }

        // Replenish the session incoming-window if exhausted.
        if need_session_flow {
            self.windows.replenish_incoming();
            let flow = self.windows.build_flow();
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
        }
        if let Some((event, sender)) = emit {
            // Consumer-driven credit guarantees the channel has room, so this is
            // non-blocking on the common path; the awaited fallback is a safety
            // net that never blocks indefinitely under correct credit accounting.
            match sender.try_send(event) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(event)) => {
                    let _ = sender.send(event).await;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {}
            }
        }
        if size_exceeded {
            self.detach_with_error(
                local,
                transport,
                crate::types::definitions::LinkError::MessageSizeExceeded,
                "assembled message exceeds max-message-size",
            );
        }
        Ok(())
    }

    /// Remove a link from the links table, its name index, and free its local
    /// handle. Centralized so `link_handles` can never drift out of sync with
    /// `links` across the (three) detach paths.
    fn forget_link(&mut self, local: u32) {
        if let Some(link) = self.links.remove(&local) {
            let key = match &link {
                Link::Sender(s) => (s.name.clone(), Role::Sender),
                Link::Receiver(r) => (r.name.clone(), Role::Receiver),
            };
            self.link_handles.remove(&key);
        }
        self.handles.release(local);
    }

    /// Detach a link with a peer-facing error and notify the user handle.
    fn detach_with_error<S: IoStream>(
        &mut self,
        local: u32,
        transport: &mut FramedTransport<S>,
        condition: crate::types::definitions::LinkError,
        description: &str,
    ) {
        let error = AmqpError::new(condition, Some(description.to_owned()));
        let remote = self.links.get(&local).and_then(|l| l.remote_handle());
        let events = self.links.get(&local).map(|l| l.events().clone());
        transport.queue_amqp(
            self.local_channel,
            &Performative::Detach(Detach {
                handle: local,
                closed: true,
                error: Some(error.clone()),
            }),
            None,
        );
        if let Some(events) = events {
            let _ = events.try_send(LinkEvent::Detached {
                handle: Handle(local),
                error: Some(error),
            });
        }
        if let Some(r) = remote {
            self.remote_handles.unbind(r);
        }
        self.forget_link(local);
    }

    /// Handle an inbound disposition (resolve our sends or update our receives).
    pub fn on_disposition(&mut self, disposition: Disposition) {
        let first = disposition.first;
        let last = disposition.last.unwrap_or(first);
        let settled = disposition.settled;
        let state = disposition.state;

        match disposition.role {
            // Peer (receiver) is settling deliveries WE sent.
            Role::Receiver => {
                for link in self.links.values_mut() {
                    if let Link::Sender(s) = link {
                        if s.unsettled.is_empty() {
                            continue; // disposition can't reference this link's deliveries
                        }
                        let affected =
                            s.unsettled
                                .apply_disposition(first, last, state.as_ref(), settled);
                        for id in affected {
                            // Complete the send future only on a TERMINAL outcome
                            // (or once the delivery is settled).
                            let resolve = match &state {
                                Some(st) if st.is_terminal() => Some(st.clone()),
                                _ if settled => Some(DeliveryState::Accepted(Accepted::default())),
                                _ => None,
                            };
                            if let Some(rs) = resolve
                                && let Some((reply, sent_at)) = s.pending.remove(&id)
                            {
                                let _ = reply.send(Ok(rs));
                                self.metrics.on_send_to_settle(sent_at.elapsed());
                            }
                            if settled {
                                self.metrics.on_inflight(-1);
                            }
                            if let Some(st) = &state {
                                let _ = s.events.try_send(LinkEvent::Disposition {
                                    handle: Handle(s.handle),
                                    delivery_id: DeliveryId(id),
                                    state: st.clone(),
                                    settled,
                                });
                            }
                        }
                    }
                }
            }
            // Peer (sender) is settling deliveries we received.
            Role::Sender => {
                for link in self.links.values_mut() {
                    if let Link::Receiver(r) = link
                        && !r.unsettled.is_empty()
                    {
                        r.unsettled
                            .apply_disposition(first, last, state.as_ref(), settled);
                    }
                }
            }
        }
    }

    /// Emit a disposition for received deliveries (consumer settle), honoring the
    /// link's settle mode.
    pub fn send_disposition<S: IoStream>(
        &mut self,
        handle: u32,
        first: DeliveryId,
        last: Option<DeliveryId>,
        state: DeliveryState,
        settled: bool,
        transport: &mut FramedTransport<S>,
    ) {
        let f = first.value();
        let l = last.map(|x| x.value()).unwrap_or(f);

        // A `second` receiver proposes the outcome unsettled first; the local
        // settle happens when the sender confirms (handled in `on_disposition`).
        let second_mode = matches!(
            self.links.get(&handle),
            Some(Link::Receiver(r)) if r.settle_mode == ReceiverSettleMode::Second
        );
        let wire_settled = settled && !second_mode;

        let disposition = Disposition {
            role: Role::Receiver,
            first: f,
            last: last.map(|x| x.value()),
            settled: wire_settled,
            state: Some(state.clone()),
            batchable: false,
        };
        transport.queue_amqp(
            self.local_channel,
            &Performative::Disposition(disposition),
            None,
        );

        // Apply only to the owning link (no full-table scan).
        if let Some(Link::Receiver(r)) = self.links.get_mut(&handle)
            && !r.unsettled.is_empty()
        {
            let affected = r
                .unsettled
                .apply_disposition(f, l, Some(&state), wire_settled);
            if wire_settled {
                self.metrics.on_inflight(-(affected.len() as i64));
            }
        }
    }

    /// Grant *additional* receiver credit and advertise it (consumer-driven
    /// auto-replenish; keeps outstanding credit ≈ the channel window).
    pub fn grant_credit<S: IoStream>(
        &mut self,
        handle: u32,
        credit: u32,
        transport: &mut FramedTransport<S>,
    ) {
        let windows = self.windows;
        let local_channel = self.local_channel;
        if let Some(Link::Receiver(r)) = self.links.get_mut(&handle) {
            r.credit.grant(credit);
            self.metrics.on_credit(handle, r.credit.link_credit);
            let flow = link_flow(handle, &r.credit, &windows);
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
        }
    }

    /// Handle an inbound flow (session window + link credit).
    pub fn on_flow<S: IoStream>(
        &mut self,
        flow: Flow,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) {
        // Every inbound flow carries the peer's session-window fields; apply
        // them first — this is what reopens our remote-incoming-window.
        self.windows.on_peer_flow(&flow);
        // Apply the link-level credit for a handle-carrying flow...
        if let Some(peer_handle) = flow.handle
            && let Some(local) = self.remote_handles.resolve(peer_handle)
            && let Some(Link::Sender(s)) = self.links.get_mut(&local)
        {
            s.credit
                .apply_flow_as_sender(flow.delivery_count, flow.link_credit, flow.drain);
            self.metrics.on_credit(local, s.credit.link_credit);
            let _ = s.events.try_send(LinkEvent::Credit {
                handle: Handle(local),
                credit: s.credit.link_credit,
                drain: s.credit.drain,
            });
        }
        // ...then flush *every* sender with queued work. The reopened session
        // window is a shared resource: a flow carrying link B's handle still
        // advances the window that link A is stalled on, so flushing only the
        // named link would leave other senders' outboxes stuck indefinitely
        // (a dispatch-driven peer — a broker — never gets a second nudge).
        // Each flush is still gated by that link's own credit + the window, so
        // this neither double-sends nor sends without credit.
        let stalled: Vec<u32> = self
            .links
            .iter()
            .filter_map(|(&h, l)| match l {
                Link::Sender(s) if !s.outbox.is_empty() => Some(h),
                _ => None,
            })
            .collect();
        for handle in stalled {
            self.flush_sender(handle, transport, max_frame_size);
        }

        // Respond to an echo request with our current flow state.
        if flow.echo {
            let mut response = self.windows.build_flow();
            if let Some(peer_handle) = flow.handle
                && let Some(local) = self.remote_handles.resolve(peer_handle)
            {
                match self.links.get(&local) {
                    Some(Link::Sender(s)) => {
                        response.handle = Some(local);
                        response.delivery_count = Some(s.credit.delivery_count);
                        response.link_credit = Some(s.credit.link_credit);
                    }
                    Some(Link::Receiver(r)) => {
                        response.handle = Some(local);
                        response.delivery_count = Some(r.credit.delivery_count);
                        response.link_credit = Some(r.credit.link_credit);
                    }
                    None => {}
                }
            }
            transport.queue_amqp(self.local_channel, &Performative::Flow(response), None);
        }
    }

    /// Issue link/session flow (e.g. manual consumer credit).
    pub fn send_flow<S: IoStream>(&mut self, flow: Flow, transport: &mut FramedTransport<S>) {
        // Apply credit locally for receiver links so accounting stays consistent.
        if let Some(peer_handle) = flow.handle
            && let Some(Link::Receiver(r)) = self.links.get_mut(&peer_handle)
            && let Some(lc) = flow.link_credit
        {
            r.credit.set_credit(lc);
        }
        let mut flow = flow;
        let win = self.windows.build_flow();
        flow.next_incoming_id = win.next_incoming_id;
        flow.incoming_window = win.incoming_window;
        flow.next_outgoing_id = win.next_outgoing_id;
        flow.outgoing_window = win.outgoing_window;
        transport.queue_amqp(self.local_channel, &Performative::Flow(flow), None);
    }

    /// Detach a link.
    pub fn detach_link<S: IoStream>(
        &mut self,
        handle: u32,
        closed: bool,
        error: Option<AmqpError>,
        reply: Reply<(), LinkError>,
        transport: &mut FramedTransport<S>,
    ) {
        let remote = self.links.get(&handle).and_then(|l| l.remote_handle());
        if self.links.contains_key(&handle) {
            transport.queue_amqp(
                self.local_channel,
                &Performative::Detach(Detach {
                    handle,
                    closed,
                    error,
                }),
                None,
            );
            if let Some(r) = remote {
                self.remote_handles.unbind(r);
            }
            self.forget_link(handle);
        }
        let _ = reply.send(Ok(()));
    }

    /// Handle the peer's `detach`.
    pub fn on_peer_detach<S: IoStream>(
        &mut self,
        detach: Detach,
        transport: &mut FramedTransport<S>,
    ) {
        let Some(local) = self.remote_handles.resolve(detach.handle) else {
            return;
        };
        let events = self.links.get(&local).map(|l| l.events().clone());
        if let Some(events) = events {
            let _ = events.try_send(LinkEvent::Detached {
                handle: Handle(local),
                error: detach.error.clone(),
            });
        }
        // Acknowledge the peer's detach.
        transport.queue_amqp(
            self.local_channel,
            &Performative::Detach(Detach {
                handle: local,
                closed: detach.closed,
                error: None,
            }),
            None,
        );
        self.remote_handles.unbind(detach.handle);
        self.forget_link(local);
    }

    /// Dispatch an inbound link performative.
    pub async fn handle_link_frame<S: IoStream>(
        &mut self,
        performative: Performative,
        payload: Option<Bytes>,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) -> Result<(), ConnectError> {
        match performative {
            Performative::Attach(a) => self.on_peer_attach(a, transport),
            Performative::Transfer(t) => self.on_transfer(t, payload, transport).await?,
            Performative::Disposition(d) => self.on_disposition(d),
            Performative::Flow(f) => self.on_flow(f, transport, max_frame_size),
            Performative::Detach(d) => self.on_peer_detach(d, transport),
            _ => {}
        }
        Ok(())
    }
}

/// Build a link-level `flow` from the link credit + session windows.
fn link_flow(handle: u32, credit: &LinkCredit, windows: &SessionWindows) -> Flow {
    let mut flow = windows.build_flow();
    flow.handle = Some(handle);
    flow.delivery_count = Some(credit.delivery_count);
    flow.link_credit = Some(credit.link_credit);
    flow.available = Some(credit.available);
    flow.drain = credit.drain;
    flow
}

/// A conservative upper bound on a `transfer` performative's encoded size
/// (described header + list + every field + an 8-byte delivery-tag), so the
/// payload split can be computed without trial-serializing the performative.
const TRANSFER_PERF_BOUND: usize = 72;

/// Max payload bytes per frame given the negotiated frame size (single-pass:
/// derived from a size bound, never by re-encoding the performative).
fn max_payload_per_frame(max_frame_size: usize) -> usize {
    max_frame_size
        .saturating_sub(8 + TRANSFER_PERF_BOUND)
        .max(1)
}

/// Encode and queue the (possibly multi-frame) transfer(s) for one message,
/// advancing the session outgoing-id once per frame. The performative is
/// serialized exactly once per frame — never re-encoded to probe its size.
#[allow(clippy::too_many_arguments)]
fn send_message_frames<S: IoStream>(
    transport: &mut FramedTransport<S>,
    channel: u16,
    handle: u32,
    delivery_id: u32,
    tag: Bytes,
    settled: bool,
    message_format: u32,
    state: Option<DeliveryState>,
    body: &Bytes,
    max_payload: usize,
    windows: &mut SessionWindows,
) {
    if body.len() <= max_payload {
        let first = Transfer {
            handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(tag),
            message_format: Some(message_format),
            settled: Some(settled),
            more: false,
            state,
            ..Default::default()
        };
        transport.queue_transfer(channel, &Performative::Transfer(first), body.clone());
        windows.record_outgoing();
        return;
    }

    // Only the first transfer of a multi-frame delivery carries the tag; move
    // the single allocation into it rather than cloning.
    let mut tag = Some(tag);
    let mut offset = 0;
    let mut first_frame = true;
    while offset < body.len() {
        let end = (offset + max_payload).min(body.len());
        let is_last = end == body.len();
        let chunk = body.slice(offset..end);
        // The delivery state (e.g. transactional-state) rides only the first
        // frame of a multi-frame delivery.
        let transfer = if first_frame {
            Transfer {
                handle,
                delivery_id: Some(delivery_id),
                delivery_tag: tag.take(),
                message_format: Some(message_format),
                settled: Some(settled),
                more: !is_last,
                state: state.clone(),
                ..Default::default()
            }
        } else {
            Transfer {
                handle,
                more: !is_last,
                ..Default::default()
            }
        };
        transport.queue_transfer(channel, &Performative::Transfer(transfer), chunk);
        windows.record_outgoing();
        offset = end;
        first_frame = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::observe::noop_metrics;
    use crate::transport::frame::FrameBody;
    use tokio::io::DuplexStream;

    fn server_session(
        peer_begin: &Begin,
    ) -> (
        Session,
        Begin,
        mpsc::UnboundedReceiver<SessionEvent>,
        FramedTransport<DuplexStream>,
        FramedTransport<DuplexStream>,
    ) {
        let (near, far) = tokio::io::duplex(1 << 16);
        let transport = FramedTransport::new(near, 1 << 16);
        let far = FramedTransport::new(far, 1 << 16);
        let (evt_tx, evt_rx) = mpsc::unbounded_channel();
        let config = SessionConfig {
            incoming_window: 32,
            outgoing_window: 16,
            handle_max: 64,
        };
        let (session, response) = Session::accept_peer_begin(
            SessionId(1),
            0,
            5,
            peer_begin,
            &config,
            evt_tx,
            noop_metrics(),
        );
        (session, response, evt_rx, transport, far)
    }

    fn peer_begin() -> Begin {
        Begin {
            remote_channel: None,
            next_outgoing_id: 7,
            incoming_window: 100,
            outgoing_window: 50,
            handle_max: 8,
            ..Default::default()
        }
    }

    #[test]
    fn accept_peer_begin_maps_immediately_and_responds() {
        let (session, response, _evt, _t, _far) = server_session(&peer_begin());
        assert_eq!(session.phase(), SessionPhase::Mapped);
        assert_eq!(session.remote_channel, Some(5));
        // Windows learned from the peer's begin.
        assert_eq!(session.windows.next_incoming_id, 7);
        assert_eq!(session.windows.remote_incoming_window, 100);
        // Our response names the peer's channel and advertises our config.
        assert_eq!(response.remote_channel, Some(5));
        assert_eq!(response.incoming_window, 32);
        assert_eq!(response.outgoing_window, 16);
        assert_eq!(response.handle_max, 64);
        assert_eq!(response.next_outgoing_id, 0);
    }

    #[tokio::test]
    async fn accept_peer_attach_as_receiver_grants_credit_and_delivers() {
        let (mut session, _resp, _evt, mut transport, mut far) = server_session(&peer_begin());
        let (link_tx, mut link_rx) = mpsc::channel(16);

        // Peer attaches as SENDER (a producer publishing to us).
        let attach = Attach {
            name: "up".into(),
            handle: 3,
            role: Role::Sender,
            initial_delivery_count: Some(5),
            ..Default::default()
        };
        let accepted = session
            .accept_peer_attach(
                attach,
                CreditMode::Manual,
                10,
                Some(1 << 20),
                link_tx,
                &mut transport,
            )
            .expect("accepted");
        assert_eq!(accepted.role, Role::Receiver);
        transport.flush().await.unwrap();

        // The peer sees our responding attach (mirror role), then the credit flow.
        let frame = far.read_frame().await.unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Attach(a), _) => {
                assert_eq!(a.name, "up");
                assert_eq!(a.role, Role::Receiver);
                assert_eq!(a.handle, accepted.handle.0);
                assert_eq!(a.max_message_size, Some(1 << 20));
            }
            other => panic!("expected attach, got {other:?}"),
        }
        let frame = far.read_frame().await.unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Flow(f), _) => {
                assert_eq!(f.handle, Some(accepted.handle.0));
                assert_eq!(f.link_credit, Some(10));
                // Credit accounting starts from the peer's initial-delivery-count.
                assert_eq!(f.delivery_count, Some(5));
            }
            other => panic!("expected flow, got {other:?}"),
        }

        // An inbound transfer on the peer's handle routes to our mirror link.
        let transfer = Transfer {
            handle: 3,
            delivery_id: Some(0),
            delivery_tag: Some(Bytes::from_static(b"t0")),
            settled: Some(true),
            ..Default::default()
        };
        session
            .on_transfer(
                transfer,
                Some(Bytes::from_static(b"payload")),
                &mut transport,
            )
            .await
            .unwrap();
        match link_rx.try_recv().expect("delivery emitted") {
            LinkEvent::Delivery(d) => {
                assert_eq!(&d.message[..], b"payload");
                assert!(d.settled);
            }
            other => panic!("expected delivery, got {other:?}"),
        }
    }

    /// Refusing an attach (unresolvable address) completes the attach with a
    /// null source/target then immediately detaches (closed) with the error —
    /// a link-scoped refusal that leaves the session and its handles intact.
    #[tokio::test]
    async fn refuse_peer_attach_responds_null_terminus_then_detaches() {
        let (mut session, _resp, _evt, mut transport, mut far) = server_session(&peer_begin());
        let attach = Attach {
            name: "bad".into(),
            handle: 4,
            role: Role::Receiver, // peer wants to consume from an address we can't resolve
            source: Some(crate::types::messaging::Source::new("/nope")),
            ..Default::default()
        };
        let err = AmqpError::new(
            crate::types::definitions::AmqpError::NotFound,
            Some("no queue".into()),
        );
        session.refuse_peer_attach(&attach, err, &mut transport);
        transport.flush().await.unwrap();

        // Attach response first: our (mirror) role, null source AND target.
        match far.read_frame().await.unwrap().body {
            FrameBody::Amqp(Performative::Attach(a), _) => {
                assert_eq!(a.name, "bad");
                assert_eq!(a.role, Role::Sender);
                assert!(a.source.is_none(), "refused source must be null");
                assert!(a.target.is_none(), "refused target must be null");
            }
            other => panic!("expected attach, got {other:?}"),
        }
        // ...then a closed detach carrying the error.
        match far.read_frame().await.unwrap().body {
            FrameBody::Amqp(Performative::Detach(d), _) => {
                assert!(d.closed);
                assert!(d.error.is_some(), "detach carries the refusal error");
            }
            other => panic!("expected detach, got {other:?}"),
        }
        // The throwaway handle was released: the session can still attach links.
        assert!(!session.knows_link("bad", Role::Sender));
    }

    #[tokio::test]
    async fn accept_peer_attach_as_sender_sends_once_credited() {
        let (mut session, _resp, _evt, mut transport, mut far) = server_session(&peer_begin());
        let (link_tx, _link_rx) = mpsc::channel(16);

        // Peer attaches as RECEIVER (a consumer subscribing from us).
        let attach = Attach {
            name: "down".into(),
            handle: 9,
            role: Role::Receiver,
            ..Default::default()
        };
        let accepted = session
            .accept_peer_attach(attach, CreditMode::Manual, 0, None, link_tx, &mut transport)
            .expect("accepted");
        assert_eq!(accepted.role, Role::Sender);
        transport.flush().await.unwrap();
        let frame = far.read_frame().await.unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Attach(a), _) => {
                assert_eq!(a.role, Role::Sender);
                // A sender MUST declare its initial delivery-count.
                assert_eq!(a.initial_delivery_count, Some(0));
            }
            other => panic!("expected attach, got {other:?}"),
        }

        // Consumer grants credit via flow (identifying the link by ITS handle).
        let flow = Flow {
            next_incoming_id: Some(0),
            incoming_window: 100,
            next_outgoing_id: 0,
            outgoing_window: 100,
            handle: Some(9),
            link_credit: Some(5),
            delivery_count: Some(0),
            ..Default::default()
        };
        session.on_flow(flow, &mut transport, 1 << 16);

        // Now a queued message flushes as a transfer.
        session.send_transfer(
            accepted.handle.0,
            Bytes::from_static(b"queued message"),
            true,
            0,
            None,
            None,
            &mut transport,
            1 << 16,
        );
        transport.flush().await.unwrap();
        let frame = far.read_frame().await.unwrap();
        match frame.body {
            FrameBody::Amqp(Performative::Transfer(t), payload) => {
                assert_eq!(t.handle, accepted.handle.0);
                assert_eq!(payload.as_deref(), Some(&b"queued message"[..]));
            }
            other => panic!("expected transfer, got {other:?}"),
        }
    }

    /// A peer may open a sender and a receiver that SHARE a link name on one
    /// session — AMQP 1.0 §2.6.1 identifies a link by name AND role. Both must
    /// attach. Regression: the broker once keyed links by name alone, so the
    /// second same-named attach was misrouted/dropped, breaking Qpid Proton's
    /// default link naming (`<container>-<address>` for both directions).
    #[tokio::test]
    async fn same_name_sender_and_receiver_both_attach() {
        let begin = peer_begin();
        let (mut session, _resp, _evt, mut transport, _far) = server_session(&begin);

        // Peer's sender named "shared" (we mirror as receiver).
        let (tx1, _rx1) = mpsc::channel(16);
        let s = session
            .accept_peer_attach(
                Attach {
                    name: "shared".into(),
                    handle: 0,
                    role: Role::Sender,
                    ..Default::default()
                },
                CreditMode::Manual,
                0,
                None,
                tx1,
                &mut transport,
            )
            .expect("sender attach accepted");

        // Peer's receiver with the SAME name (we mirror as sender).
        let (tx2, _rx2) = mpsc::channel(16);
        let r = session
            .accept_peer_attach(
                Attach {
                    name: "shared".into(),
                    handle: 1,
                    role: Role::Receiver,
                    ..Default::default()
                },
                CreditMode::Manual,
                0,
                None,
                tx2,
                &mut transport,
            )
            .expect("same-name receiver attach accepted");

        assert_ne!(
            s.handle, r.handle,
            "the two links get distinct local handles"
        );

        // A genuine duplicate (same name AND same role) is still refused.
        let (tx3, _rx3) = mpsc::channel(16);
        let dup = session.accept_peer_attach(
            Attach {
                name: "shared".into(),
                handle: 2,
                role: Role::Sender,
                ..Default::default()
            },
            CreditMode::Manual,
            0,
            None,
            tx3,
            &mut transport,
        );
        assert!(
            dup.is_err(),
            "a same-name same-role duplicate is still rejected"
        );
    }

    #[tokio::test]
    async fn session_flow_without_handle_resumes_stalled_senders() {
        // Peer advertises a 2-transfer incoming window; we have credit for 3.
        let begin = Begin {
            incoming_window: 2,
            ..peer_begin()
        };
        let (mut session, _resp, _evt, mut transport, mut far) = server_session(&begin);
        let (link_tx, _link_rx) = mpsc::channel(16);
        let attach = Attach {
            name: "down".into(),
            handle: 9,
            role: Role::Receiver,
            ..Default::default()
        };
        let accepted = session
            .accept_peer_attach(attach, CreditMode::Manual, 0, None, link_tx, &mut transport)
            .expect("accepted");
        // Grant link credit 3 via a handle flow.
        session.on_flow(
            Flow {
                next_incoming_id: Some(0),
                incoming_window: 2,
                next_outgoing_id: 0,
                outgoing_window: 100,
                handle: Some(9),
                link_credit: Some(3),
                delivery_count: Some(0),
                ..Default::default()
            },
            &mut transport,
            1 << 16,
        );
        for _ in 0..3 {
            session.send_transfer(
                accepted.handle.0,
                Bytes::from_static(b"m"),
                true,
                0,
                None,
                None,
                &mut transport,
                1 << 16,
            );
        }
        transport.flush().await.unwrap();
        // Attach response + exactly 2 transfers (window-limited); the third is
        // stalled in the outbox.
        let mut transfers = 0;
        for _ in 0..3 {
            match far.read_frame().await.unwrap().body {
                FrameBody::Amqp(Performative::Transfer(_), _) => transfers += 1,
                FrameBody::Amqp(Performative::Attach(_), _) => {}
                other => panic!("unexpected frame {other:?}"),
            }
        }
        assert_eq!(transfers, 2, "third transfer must be window-stalled");

        // A pure SESSION flow (no handle) reopens the window: the stalled
        // transfer must flush without any link-level flow or new send.
        session.on_flow(
            Flow {
                next_incoming_id: Some(2),
                incoming_window: 100,
                next_outgoing_id: 0,
                outgoing_window: 100,
                ..Default::default()
            },
            &mut transport,
            1 << 16,
        );
        transport.flush().await.unwrap();
        match far.read_frame().await.unwrap().body {
            FrameBody::Amqp(Performative::Transfer(_), _) => {}
            other => panic!("expected the stalled transfer, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn flow_carrying_one_handle_still_unstalls_other_senders() {
        // Two consumer links (broker is sender to each) share a session whose
        // remote-incoming-window is tiny. Link A fills the window and stalls
        // with a message still queued. A flow carrying link B's handle reopens
        // the shared session window — link A's stalled message must flush.
        let begin = Begin {
            incoming_window: 1, // peer will accept only 1 transfer before flow
            ..peer_begin()
        };
        let (mut session, _resp, _evt, mut transport, mut far) = server_session(&begin);
        let (tx, _rx) = mpsc::channel(16);

        let attach = |name: &str, handle: u32| Attach {
            name: name.into(),
            handle,
            role: Role::Receiver,
            ..Default::default()
        };
        let a = session
            .accept_peer_attach(
                attach("A", 1),
                CreditMode::Manual,
                0,
                None,
                tx.clone(),
                &mut transport,
            )
            .expect("A");
        let b = session
            .accept_peer_attach(
                attach("B", 2),
                CreditMode::Manual,
                0,
                None,
                tx,
                &mut transport,
            )
            .expect("B");

        // Credit both links generously (link credit is not the bottleneck here).
        for peer_h in [1u32, 2] {
            session.on_flow(
                Flow {
                    next_incoming_id: Some(0),
                    incoming_window: 1,
                    next_outgoing_id: 0,
                    outgoing_window: 100,
                    handle: Some(peer_h),
                    link_credit: Some(10),
                    delivery_count: Some(0),
                    ..Default::default()
                },
                &mut transport,
                1 << 16,
            );
        }

        // Queue two messages on A: the first consumes the 1-frame window, the
        // second is stuck.
        for body in [&b"a1"[..], &b"a2"[..]] {
            session.send_transfer(
                a.handle.0,
                Bytes::copy_from_slice(body),
                true,
                0,
                None,
                None,
                &mut transport,
                1 << 16,
            );
        }
        transport.flush().await.unwrap();
        // Drain frames emitted so far; exactly one A transfer got out.
        let mut a_transfers = 0;
        while let Ok(Ok(f)) =
            tokio::time::timeout(std::time::Duration::from_millis(50), far.read_frame()).await
        {
            if let FrameBody::Amqp(Performative::Transfer(_), _) = f.body {
                a_transfers += 1;
            }
        }
        assert_eq!(a_transfers, 1, "window allowed only the first transfer");

        // A flow that names link B reopens the shared session window. The fix:
        // A's stalled second message must now flush even though the flow was
        // "for" B.
        session.on_flow(
            Flow {
                next_incoming_id: Some(1),
                incoming_window: 100,
                next_outgoing_id: 0,
                outgoing_window: 100,
                handle: Some(2), // link B's handle — NOT the stalled link A
                link_credit: Some(10),
                delivery_count: Some(0),
                ..Default::default()
            },
            &mut transport,
            1 << 16,
        );
        transport.flush().await.unwrap();
        let f = tokio::time::timeout(std::time::Duration::from_millis(200), far.read_frame())
            .await
            .expect("A's stalled transfer must flush")
            .unwrap();
        match f.body {
            FrameBody::Amqp(Performative::Transfer(_), payload) => {
                assert_eq!(payload.as_deref(), Some(&b"a2"[..]));
            }
            other => panic!("expected A's stalled transfer, got {other:?}"),
        }
        let _ = b;
    }

    #[tokio::test]
    async fn accept_peer_attach_rejects_duplicates_and_exhaustion() {
        let (mut session, _resp, _evt, mut transport, _far) = server_session(&peer_begin());
        let (link_tx, _link_rx) = mpsc::channel(16);

        let attach = Attach {
            name: "dup".into(),
            handle: 1,
            role: Role::Sender,
            ..Default::default()
        };
        session
            .accept_peer_attach(
                attach.clone(),
                CreditMode::Manual,
                0,
                None,
                link_tx.clone(),
                &mut transport,
            )
            .expect("first attach accepted");
        let err = session
            .accept_peer_attach(attach, CreditMode::Manual, 0, None, link_tx, &mut transport)
            .expect_err("duplicate rejected");
        assert_eq!(err.kind(), ErrorKind::ProtocolViolation);
    }

    #[tokio::test]
    async fn accept_peer_attach_respects_handle_max() {
        // Peer advertises handle-max 0: exactly one handle is available.
        let begin = Begin {
            handle_max: 0,
            ..peer_begin()
        };
        let (mut session, _resp, _evt, mut transport, _far) = server_session(&begin);
        let (link_tx, _link_rx) = mpsc::channel(16);

        let attach = |name: &str, handle: u32| Attach {
            name: name.into(),
            handle,
            role: Role::Sender,
            ..Default::default()
        };
        session
            .accept_peer_attach(
                attach("a", 0),
                CreditMode::Manual,
                0,
                None,
                link_tx.clone(),
                &mut transport,
            )
            .expect("first fits under handle-max");
        let err = session
            .accept_peer_attach(
                attach("b", 1),
                CreditMode::Manual,
                0,
                None,
                link_tx,
                &mut transport,
            )
            .expect_err("second exceeds handle-max");
        assert_eq!(err.kind(), ErrorKind::Capacity);
    }
}
