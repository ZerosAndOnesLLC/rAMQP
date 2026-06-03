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
use crate::proto::{
    IncomingDelivery, LinkAttached, LinkEvent, Reply, SessionEvent, SessionOpened,
};
use crate::session::registry::{HandleAllocator, RemoteHandleMap};
use crate::session::window::SessionWindows;
use crate::transport::IoStream;
use crate::transport::frame::FramedTransport;
use crate::types::definitions::{Error as AmqpError, ReceiverSettleMode, Role};
use crate::types::messaging::{Accepted, DeliveryState};
use crate::types::performatives::{Attach, Begin, Detach, Disposition, End, Flow, Performative, Transfer};

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
    pub fn on_peer_begin(&mut self, remote_channel: u16, begin: &Begin) {
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
    pub fn on_peer_attach<S: IoStream>(&mut self, attach: Attach, transport: &mut FramedTransport<S>) {
        let local = self.links.iter().find_map(|(h, l)| {
            let name = match l {
                Link::Sender(s) => &s.name,
                Link::Receiver(r) => &r.name,
            };
            (name == &attach.name && l.remote_handle().is_none()).then_some(*h)
        });
        let Some(local) = local else { return };
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

    /// Enqueue an outbound message on a sender link and try to flush it.
    #[allow(clippy::too_many_arguments)]
    pub fn send_transfer<S: IoStream>(
        &mut self,
        handle: u32,
        body: Bytes,
        settled: bool,
        message_format: u32,
        reply: Option<Reply<DeliveryState, crate::error::SendError>>,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) {
        match self.links.get_mut(&handle) {
            Some(Link::Sender(s)) => s.outbox.push_back(PendingSend {
                body,
                settled,
                message_format,
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
            // A message takes `frame_count` transfer frames; only begin it if the
            // session outgoing-window can cover all of them (avoids over-drawing
            // the window mid-message).
            let body_len = sender.outbox.front().expect("non-empty").body.len();
            let frame_count = body_len.div_ceil(max_payload).max(1) as u32;
            if windows.remote_incoming_window < frame_count {
                break;
            }

            let pending = sender.outbox.pop_front().expect("non-empty");
            let delivery_id = windows.next_outgoing_id;
            let tag = sender.next_delivery_tag();
            send_message_frames(
                transport,
                local_channel,
                handle,
                delivery_id,
                &tag,
                pending.settled,
                pending.message_format,
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
                sender.unsettled.insert(delivery_id, tag, None);
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
        let need_session_flow = self.windows.record_incoming(transfer.delivery_id.unwrap_or(0));

        let windows = self.windows;
        let local_channel = self.local_channel;

        // Synchronous assembly + state updates; collect what to emit/queue.
        let mut emit: Option<(LinkEvent, mpsc::Sender<LinkEvent>)> = None;
        let mut link_flow_to_send = None;
        let mut size_exceeded = false;

        if let Some(Link::Receiver(r)) = self.links.get_mut(&local) {
            if transfer.aborted {
                // Abandoned delivery: drop any partial, consume the credit, emit
                // nothing, and refill credit if due.
                r.partial = None;
                r.credit.record_received();
                if let Some(grant) = r.credit.auto_refill() {
                    r.credit.grant(grant);
                    self.metrics.on_credit(r.handle, r.credit.link_credit);
                    link_flow_to_send = Some(link_flow(r.handle, &r.credit, &windows));
                }
            } else {
                // A continuation transfer must carry the same delivery-id (or none).
                if let (Some(partial), Some(did)) = (r.partial.as_ref(), transfer.delivery_id) {
                    if partial.delivery_id().value() != did {
                        return Err(ConnectError::msg(
                            ErrorKind::ProtocolViolation,
                            "interleaved delivery-id during multi-frame transfer",
                        ));
                    }
                }
                let payload = payload.unwrap_or_default();
                let cap = r.size_cap();
                let complete = if let Some(partial) = r.partial.as_mut() {
                    partial.append(&payload);
                    !transfer.more
                } else {
                    let delivery_id = DeliveryId(transfer.delivery_id.unwrap_or(0));
                    let tag = transfer.delivery_tag.clone().unwrap_or_default();
                    let settled = transfer.settled.unwrap_or(false);
                    r.partial = Some(PartialDelivery::new(delivery_id, tag, settled, &payload));
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
                        delivery_id: delivery.delivery_id,
                        delivery_tag: delivery.delivery_tag.clone(),
                        settled: delivery.settled,
                        message: delivery.into_raw(),
                    });
                    emit = Some((event, r.events.clone()));

                    if let Some(grant) = r.credit.auto_refill() {
                        r.credit.grant(grant);
                        self.metrics.on_credit(r.handle, r.credit.link_credit);
                        link_flow_to_send = Some(link_flow(r.handle, &r.credit, &windows));
                    }
                }
            }
        }

        // Replenish the session incoming-window if exhausted.
        if need_session_flow {
            self.windows.replenish_incoming(self.windows.outgoing_window.max(1));
            let flow = self.windows.build_flow();
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
        }
        if let Some(flow) = link_flow_to_send {
            transport.queue_amqp(local_channel, &Performative::Flow(flow), None);
        }
        if let Some((event, sender)) = emit {
            // Non-blocking on the common path; only a momentarily-full consumer
            // channel falls back to an awaited send (back-pressure without loss).
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
            let _ = events.try_send(LinkEvent::Detached { error: Some(error) });
        }
        if let Some(r) = remote {
            self.remote_handles.unbind(r);
        }
        self.links.remove(&local);
        self.handles.release(local);
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
                        let affected = s.unsettled.apply_disposition(first, last, state.as_ref(), settled);
                        for id in affected {
                            // Complete the send future only on a TERMINAL outcome
                            // (or once the delivery is settled).
                            let resolve = match &state {
                                Some(st) if st.is_terminal() => Some(st.clone()),
                                _ if settled => Some(DeliveryState::Accepted(Accepted::default())),
                                _ => None,
                            };
                            if let Some(rs) = resolve {
                                if let Some((reply, sent_at)) = s.pending.remove(&id) {
                                    let _ = reply.send(Ok(rs));
                                    self.metrics.on_send_to_settle(sent_at.elapsed());
                                }
                            }
                            if settled {
                                self.metrics.on_inflight(-1);
                            }
                            if let Some(st) = &state {
                                let _ = s.events.try_send(LinkEvent::Disposition {
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
                    if let Link::Receiver(r) = link {
                        if !r.unsettled.is_empty() {
                            r.unsettled.apply_disposition(first, last, state.as_ref(), settled);
                        }
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
        transport.queue_amqp(self.local_channel, &Performative::Disposition(disposition), None);

        // Apply only to the owning link (no full-table scan).
        if let Some(Link::Receiver(r)) = self.links.get_mut(&handle) {
            if !r.unsettled.is_empty() {
                let affected = r.unsettled.apply_disposition(f, l, Some(&state), wire_settled);
                if wire_settled {
                    self.metrics.on_inflight(-(affected.len() as i64));
                }
            }
        }
    }

    /// Handle an inbound flow (session window + link credit).
    pub fn on_flow<S: IoStream>(
        &mut self,
        flow: Flow,
        transport: &mut FramedTransport<S>,
        max_frame_size: usize,
    ) {
        self.windows.on_peer_flow(&flow);
        if let Some(peer_handle) = flow.handle {
            if let Some(local) = self.remote_handles.resolve(peer_handle) {
                if let Some(Link::Sender(s)) = self.links.get_mut(&local) {
                    s.credit
                        .apply_flow_as_sender(flow.delivery_count, flow.link_credit, flow.drain);
                    self.metrics.on_credit(local, s.credit.link_credit);
                    let _ = s.events.try_send(LinkEvent::Credit {
                        credit: s.credit.link_credit,
                        drain: s.credit.drain,
                    });
                }
                self.flush_sender(local, transport, max_frame_size);
            }
        }

        // Respond to an echo request with our current flow state.
        if flow.echo {
            let mut response = self.windows.build_flow();
            if let Some(peer_handle) = flow.handle {
                if let Some(local) = self.remote_handles.resolve(peer_handle) {
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
            }
            transport.queue_amqp(self.local_channel, &Performative::Flow(response), None);
        }
    }

    /// Issue link/session flow (e.g. manual consumer credit).
    pub fn send_flow<S: IoStream>(&mut self, flow: Flow, transport: &mut FramedTransport<S>) {
        // Apply credit locally for receiver links so accounting stays consistent.
        if let Some(peer_handle) = flow.handle {
            if let Some(Link::Receiver(r)) = self.links.get_mut(&peer_handle) {
                if let Some(lc) = flow.link_credit {
                    r.credit.set_credit(lc);
                }
            }
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
            self.links.remove(&handle);
            self.handles.release(handle);
        }
        let _ = reply.send(Ok(()));
    }

    /// Handle the peer's `detach`.
    pub fn on_peer_detach<S: IoStream>(&mut self, detach: Detach, transport: &mut FramedTransport<S>) {
        let Some(local) = self.remote_handles.resolve(detach.handle) else {
            return;
        };
        let events = self.links.get(&local).map(|l| l.events().clone());
        if let Some(events) = events {
            let _ = events.try_send(LinkEvent::Detached {
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
        self.links.remove(&local);
        self.handles.release(local);
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
    tag: &Bytes,
    settled: bool,
    message_format: u32,
    body: &[u8],
    max_payload: usize,
    windows: &mut SessionWindows,
) {
    if body.len() <= max_payload {
        let first = Transfer {
            handle,
            delivery_id: Some(delivery_id),
            delivery_tag: Some(tag.clone()),
            message_format: Some(message_format),
            settled: Some(settled),
            more: false,
            ..Default::default()
        };
        transport.queue_amqp(channel, &Performative::Transfer(first), Some(body));
        windows.record_outgoing();
        return;
    }

    let mut offset = 0;
    let mut first_frame = true;
    while offset < body.len() {
        let end = (offset + max_payload).min(body.len());
        let is_last = end == body.len();
        let chunk = &body[offset..end];
        let transfer = if first_frame {
            Transfer {
                handle,
                delivery_id: Some(delivery_id),
                delivery_tag: Some(tag.clone()),
                message_format: Some(message_format),
                settled: Some(settled),
                more: !is_last,
                ..Default::default()
            }
        } else {
            Transfer {
                handle,
                more: !is_last,
                ..Default::default()
            }
        };
        transport.queue_amqp(channel, &Performative::Transfer(transfer), Some(chunk));
        windows.record_outgoing();
        offset = end;
        first_frame = false;
    }
}
