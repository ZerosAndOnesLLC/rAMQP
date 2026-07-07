//! The inter-node fabric: one multiplexed TCP transport per peer pair.
//!
//! Everything a node says to another node rides this single connection —
//! Raft RPCs for the metadata group *and* every per-queue group (the
//! shared-transport half of the multi-raft manager, broker.md §8), plus the
//! data-plane forwarding the leader-routing fabric needs (publish, subscribe,
//! demand, settle, deliver). One connection per peer pair means a thousand
//! quorum queues share one socket instead of opening a thousand.
//!
//! Hot-path shape (broker.md §3.2):
//! - **Zero-copy bodies** — message payloads ride as the raw tail of a frame,
//!   sliced out of the read buffer as refcounted `Bytes`; they are never run
//!   through serde. Only the small fixed header is bincode-encoded.
//! - **Batched writes** — the writer task drains its queue and flushes once
//!   per wakeup, so a burst of deliveries/acks is one syscall, not N.
//! - **Correlation ids, not lock-step RPC** — requests are pipelined and
//!   replies matched by id, so a slow call (a snapshot chunk) never
//!   head-of-line-blocks another group's heartbeat, and a cancelled caller
//!   just abandons its id (no desynced-stream hazard).

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use bytes::{Buf, BufMut, Bytes, BytesMut};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::sync::{Mutex, mpsc, oneshot};

use super::NodeId;

/// Hard cap on one fabric frame (a Raft snapshot chunk is the largest thing
/// that rides inside; message bodies are already capped far below this).
pub const MAX_FABRIC_FRAME: usize = 64 * 1024 * 1024;

/// How long a correlated call may wait for its reply before the caller gives
/// up. openraft wraps its own RPCs in tighter timeouts; this is the backstop
/// for data-plane calls against a wedged peer.
pub const CALL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Which Raft group a Raft RPC is for.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum GroupRef {
    /// The cluster-wide metadata group.
    Meta,
    /// The per-queue group for this queue name.
    Queue(String),
}

/// Which Raft RPC rides in the body (the waiter decodes the reply by kind).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RaftKind {
    AppendEntries,
    Vote,
    InstallSnapshot,
}

/// The outcome of a forwarded publish.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PublishStatus {
    /// Committed to the queue group's log.
    Accepted,
    /// Refused (queue full / not committed) — a terminal rejection.
    Rejected,
    /// This node does not lead the queue's group; retry against the hint.
    NotLeader(Option<NodeId>),
}

/// A correlated request. The body's meaning depends on the kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum RequestKind {
    /// Body: the bincode Raft RPC for `kind`. Reply body: its bincode result.
    Raft(GroupRef, RaftKind),
    /// Body: a bincode [`super::meta::MetaCommand`]. Reply body: bincode
    /// `Result<MetaResponse, MetaWriteError>`.
    MetaWrite,
    /// Start (or heal) this node's member of a queue group. Empty body/reply.
    StartGroup {
        /// The queue name (== group id).
        queue: String,
        /// The full replica set: `(node id, fabric address)`.
        members: Vec<(NodeId, String)>,
    },
    /// Ask which node leads a queue group. Reply body: bincode `Option<NodeId>`.
    WhoLeads {
        /// The queue name.
        queue: String,
    },
    /// Publish one message to a queue this node leads. Body: the raw message.
    /// Reply body: bincode [`PublishStatus`].
    Publish {
        /// The queue name.
        queue: String,
    },
    /// Open a subscription channel to a queue this node leads. Deliveries
    /// flow back as [`FabricHeader::Deliver`] frames carrying `sub_chan`.
    /// Reply body: bincode `Result<(), Option<NodeId>>` (`Err` = not leader).
    OpenSub {
        /// The queue name.
        queue: String,
        /// Caller-chosen channel id for this subscription (unique per
        /// connection).
        sub_chan: u64,
    },
    /// Publish into a slot previously reserved via [`RequestKind::Reserve`]
    /// (transaction commit). Body: the raw message. Reply body: bincode
    /// [`PublishStatus`].
    PublishReserved {
        /// The queue name.
        queue: String,
    },
    /// Reserve `count` capacity slots on a queue this node leads
    /// (transaction commit phase 1). Reply body: bincode `bool`.
    Reserve {
        /// The queue name.
        queue: String,
        /// Slots requested.
        count: u32,
    },
    /// Release reserved slots that will not be published (commit abort).
    /// Empty reply body.
    Unreserve {
        /// The queue name.
        queue: String,
        /// Slots to release.
        count: u32,
    },
}

/// One fabric frame's header. `corr`-carrying variants are request/reply
/// pairs; `sub_chan`-carrying variants are the subscription stream.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum FabricHeader {
    /// A correlated request.
    Request {
        /// Correlation id (unique per connection direction).
        corr: u64,
        /// What is being asked.
        req: RequestKind,
    },
    /// A successful reply; body per the request kind.
    Reply {
        /// The request's correlation id.
        corr: u64,
    },
    /// A failed reply (transport/handler error, not a domain result).
    ReplyErr {
        /// The request's correlation id.
        corr: u64,
        /// Human-readable cause.
        msg: String,
    },
    /// Consumer demand for a subscription (origin → leader).
    Demand {
        /// The subscription channel.
        sub_chan: u64,
        /// Additional messages the consumer may be sent.
        credit: u32,
        /// Deliver what is available, then drop unmet demand.
        drain: bool,
    },
    /// A settlement for a delivered message (origin → leader).
    Settle {
        /// The subscription channel.
        sub_chan: u64,
        /// The queue-assigned message id.
        msg_id: u64,
        /// The outcome, as [`SettleWire`].
        outcome: SettleWire,
    },
    /// Close a subscription (origin → leader).
    CloseSub {
        /// The subscription channel.
        sub_chan: u64,
    },
    /// A delivery (leader → origin). Body: the raw message bytes.
    Deliver {
        /// The subscription channel.
        sub_chan: u64,
        /// The queue-assigned message id (echo in `Settle`).
        msg_id: u64,
    },
    /// The leader closed a subscription (queue gone / leadership lost).
    SubClosed {
        /// The subscription channel.
        sub_chan: u64,
    },
}

/// [`crate::queue::SettleOutcome`] on the wire.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SettleWire {
    Ack,
    Requeue,
    RequeueFailed,
    Drop,
}

impl From<crate::queue::SettleOutcome> for SettleWire {
    fn from(o: crate::queue::SettleOutcome) -> Self {
        use crate::queue::SettleOutcome as S;
        match o {
            S::Ack => SettleWire::Ack,
            S::Requeue => SettleWire::Requeue,
            S::RequeueFailed => SettleWire::RequeueFailed,
            S::Drop => SettleWire::Drop,
        }
    }
}

impl From<SettleWire> for crate::queue::SettleOutcome {
    fn from(w: SettleWire) -> Self {
        use crate::queue::SettleOutcome as S;
        match w {
            SettleWire::Ack => S::Ack,
            SettleWire::Requeue => S::Requeue,
            SettleWire::RequeueFailed => S::RequeueFailed,
            SettleWire::Drop => S::Drop,
        }
    }
}

/// One outbound frame: header + zero-copy body.
#[derive(Debug)]
pub struct OutFrame {
    pub header: FabricHeader,
    pub body: Bytes,
}

impl OutFrame {
    pub fn new(header: FabricHeader, body: Bytes) -> Self {
        OutFrame { header, body }
    }

    pub fn control(header: FabricHeader) -> Self {
        OutFrame {
            header,
            body: Bytes::new(),
        }
    }
}

/// Encode one frame into `out`:
/// `[u32 total][u16 header_len][bincode header][raw body]`.
fn encode_frame(frame: &OutFrame, out: &mut BytesMut) -> std::io::Result<()> {
    let header = bincode::serialize(&frame.header).map_err(std::io::Error::other)?;
    let header_len = u16::try_from(header.len()).map_err(std::io::Error::other)?;
    let total = 2 + header.len() + frame.body.len();
    if total > MAX_FABRIC_FRAME {
        return Err(std::io::Error::other("fabric frame too large"));
    }
    out.reserve(4 + total);
    out.put_u32(total as u32);
    out.put_u16(header_len);
    out.put_slice(&header);
    out.put_slice(&frame.body);
    Ok(())
}

/// Read one frame from `reader`, buffering in `buf`. The body is sliced out
/// of the buffer as refcounted `Bytes` — no copy.
pub async fn read_frame(
    reader: &mut OwnedReadHalf,
    buf: &mut BytesMut,
) -> std::io::Result<(FabricHeader, Bytes)> {
    loop {
        if buf.len() >= 4 {
            let total = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
            if total > MAX_FABRIC_FRAME {
                return Err(std::io::Error::other("fabric frame too large"));
            }
            if buf.len() >= 4 + total {
                buf.advance(4);
                let mut frame = buf.split_to(total);
                let header_len = frame.get_u16() as usize;
                if header_len > frame.len() {
                    return Err(std::io::Error::other("fabric header length overruns frame"));
                }
                let header_bytes = frame.split_to(header_len);
                let header: FabricHeader =
                    bincode::deserialize(&header_bytes).map_err(std::io::Error::other)?;
                return Ok((header, frame.freeze()));
            }
        }
        if reader.read_buf(buf).await? == 0 {
            return Err(std::io::ErrorKind::UnexpectedEof.into());
        }
    }
}

/// Spawn the batched writer task for one connection half. Frames sent on the
/// returned channel are coalesced under one flush per wakeup. The task exits
/// when every sender is dropped or the socket errors.
pub fn spawn_writer(mut writer: OwnedWriteHalf) -> mpsc::UnboundedSender<OutFrame> {
    let (tx, mut rx) = mpsc::unbounded_channel::<OutFrame>();
    tokio::spawn(async move {
        let mut out = BytesMut::with_capacity(64 * 1024);
        while let Some(frame) = rx.recv().await {
            if encode_frame(&frame, &mut out).is_err() {
                break;
            }
            // Coalesce whatever else is already queued into this write.
            while let Ok(next) = rx.try_recv() {
                if encode_frame(&next, &mut out).is_err() {
                    return;
                }
                if out.len() >= 1024 * 1024 {
                    break; // bound per-wakeup batch
                }
            }
            if writer.write_all(&out).await.is_err() {
                return;
            }
            out.clear();
            if writer.flush().await.is_err() {
                return;
            }
        }
    });
    tx
}

/// An event pushed to a subscription's owner.
#[derive(Debug)]
pub enum SubEvent {
    /// A message delivery.
    Deliver { msg_id: u64, body: Bytes },
    /// The subscription is gone (leadership lost, queue died, or the
    /// connection dropped). The owner must re-resolve and re-subscribe.
    Closed,
}

/// The live state of one established peer connection.
#[derive(Debug)]
pub struct ConnState {
    /// Outbound frames (batched writer).
    writer: mpsc::UnboundedSender<OutFrame>,
    /// Correlated calls awaiting replies.
    pending: std::sync::Mutex<HashMap<u64, oneshot::Sender<Result<Bytes, String>>>>,
    /// Open subscriptions: `sub_chan` → the owner's event channel.
    subs: std::sync::Mutex<HashMap<u64, mpsc::UnboundedSender<(u64, SubEvent)>>>,
    /// Set (before the pending/subs drain) when the reader dies — the
    /// definitive liveness signal. The writer channel alone can look open
    /// long after the socket is gone.
    closed: std::sync::atomic::AtomicBool,
    next_corr: AtomicU64,
    next_sub_chan: AtomicU64,
}

impl ConnState {
    /// Send one correlated request and await its reply body.
    pub async fn call(&self, req: RequestKind, body: Bytes) -> Result<Bytes, String> {
        let corr = self.next_corr.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        self.pending
            .lock()
            .expect("fabric pending lock")
            .insert(corr, tx);
        // Checked AFTER registering: `shatter` sets `closed` before draining,
        // so either the drain fails our entry or this check catches it — a
        // call can never be left to dangle against a dead connection.
        let sent = !self.is_closed()
            && self
                .writer
                .send(OutFrame::new(FabricHeader::Request { corr, req }, body))
                .is_ok();
        if !sent {
            self.pending
                .lock()
                .expect("fabric pending lock")
                .remove(&corr);
            return Err("fabric connection closed".to_owned());
        }
        match tokio::time::timeout(CALL_TIMEOUT, rx).await {
            Ok(Ok(result)) => result,
            Ok(Err(_)) => Err("fabric connection dropped".to_owned()),
            Err(_) => {
                self.pending
                    .lock()
                    .expect("fabric pending lock")
                    .remove(&corr);
                Err("fabric call timed out".to_owned())
            }
        }
    }

    /// Open a subscription channel. Deliveries and closure arrive on
    /// `events` tagged with the returned `sub_chan`.
    pub async fn open_sub(
        &self,
        queue: &str,
        events: mpsc::UnboundedSender<(u64, SubEvent)>,
    ) -> Result<u64, OpenSubError> {
        let sub_chan = self.next_sub_chan.fetch_add(1, Ordering::Relaxed);
        self.subs
            .lock()
            .expect("fabric subs lock")
            .insert(sub_chan, events);
        let reply = self
            .call(
                RequestKind::OpenSub {
                    queue: queue.to_owned(),
                    sub_chan,
                },
                Bytes::new(),
            )
            .await;
        let outcome: Result<(), Option<NodeId>> = match reply {
            Ok(body) => {
                bincode::deserialize(&body).map_err(|e| OpenSubError::Transport(e.to_string()))?
            }
            Err(e) => {
                self.subs
                    .lock()
                    .expect("fabric subs lock")
                    .remove(&sub_chan);
                return Err(OpenSubError::Transport(e));
            }
        };
        match outcome {
            Ok(()) => Ok(sub_chan),
            Err(hint) => {
                self.subs
                    .lock()
                    .expect("fabric subs lock")
                    .remove(&sub_chan);
                Err(OpenSubError::NotLeader(hint))
            }
        }
    }

    /// Fire-and-forget frames for an open subscription.
    pub fn send_demand(&self, sub_chan: u64, credit: u32, drain: bool) {
        let _ = self.writer.send(OutFrame::control(FabricHeader::Demand {
            sub_chan,
            credit,
            drain,
        }));
    }

    pub fn send_settle(&self, sub_chan: u64, msg_id: u64, outcome: SettleWire) {
        let _ = self.writer.send(OutFrame::control(FabricHeader::Settle {
            sub_chan,
            msg_id,
            outcome,
        }));
    }

    pub fn close_sub(&self, sub_chan: u64) {
        self.subs
            .lock()
            .expect("fabric subs lock")
            .remove(&sub_chan);
        let _ = self
            .writer
            .send(OutFrame::control(FabricHeader::CloseSub { sub_chan }));
    }

    /// Whether the connection is still live.
    pub fn is_open(&self) -> bool {
        !self.is_closed() && !self.writer.is_closed()
    }

    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }

    /// Route one inbound frame (called by the connection's reader task).
    fn route(&self, header: FabricHeader, body: Bytes) {
        match header {
            FabricHeader::Reply { corr } => {
                if let Some(tx) = self
                    .pending
                    .lock()
                    .expect("fabric pending lock")
                    .remove(&corr)
                {
                    let _ = tx.send(Ok(body));
                }
            }
            FabricHeader::ReplyErr { corr, msg } => {
                if let Some(tx) = self
                    .pending
                    .lock()
                    .expect("fabric pending lock")
                    .remove(&corr)
                {
                    let _ = tx.send(Err(msg));
                }
            }
            FabricHeader::Deliver { sub_chan, msg_id } => {
                let subs = self.subs.lock().expect("fabric subs lock");
                if let Some(events) = subs.get(&sub_chan) {
                    let _ = events.send((sub_chan, SubEvent::Deliver { msg_id, body }));
                }
                // No subscriber: it was closed locally; the CloseSub frame is
                // already in flight and the leader will requeue on receipt.
            }
            FabricHeader::SubClosed { sub_chan } => {
                if let Some(events) = self
                    .subs
                    .lock()
                    .expect("fabric subs lock")
                    .remove(&sub_chan)
                {
                    let _ = events.send((sub_chan, SubEvent::Closed));
                }
            }
            other => {
                tracing::warn!(?other, "unexpected fabric frame on a client connection");
            }
        }
    }

    /// Tear down: fail pending calls, close open subscriptions.
    fn shatter(&self) {
        // Order matters: mark closed FIRST, then drain — see `call`.
        self.closed.store(true, Ordering::Release);
        let pending: Vec<_> = {
            let mut map = self.pending.lock().expect("fabric pending lock");
            map.drain().collect()
        };
        for (_, tx) in pending {
            let _ = tx.send(Err("fabric connection lost".to_owned()));
        }
        let subs: Vec<_> = {
            let mut map = self.subs.lock().expect("fabric subs lock");
            map.drain().collect()
        };
        for (sub_chan, events) in subs {
            let _ = events.send((sub_chan, SubEvent::Closed));
        }
    }
}

/// Errors from [`ConnState::open_sub`].
#[derive(Debug)]
pub enum OpenSubError {
    /// The target node does not lead the queue's group.
    NotLeader(Option<NodeId>),
    /// The call itself failed (connection/peer error).
    Transport(String),
}

/// A lazily-connected, reconnecting client to one peer node.
#[derive(Debug)]
pub struct PeerClient {
    addr: String,
    conn: Mutex<Option<Arc<ConnState>>>,
}

impl PeerClient {
    pub fn new(addr: String) -> Self {
        PeerClient {
            addr,
            conn: Mutex::new(None),
        }
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    /// How long a fabric dial may take before failing. A blackholed peer
    /// (SYN drop — the classic failover mode) would otherwise hang for the
    /// kernel's ~2-minute TCP timeout.
    const DIAL_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(5);

    /// The live connection, dialing if needed. The dial happens OUTSIDE the
    /// mutex: holding it across a hung connect would stall every caller
    /// behind the same lock — leader resolution sweeps, proxy binds,
    /// meta-write forwards — for the whole timeout.
    pub async fn conn(&self) -> std::io::Result<Arc<ConnState>> {
        if let Some(conn) = self.conn.lock().await.as_ref()
            && conn.is_open()
        {
            return Ok(conn.clone());
        }
        let stream = tokio::time::timeout(Self::DIAL_TIMEOUT, TcpStream::connect(&self.addr))
            .await
            .map_err(|_| {
                std::io::Error::new(std::io::ErrorKind::TimedOut, "fabric dial timed out")
            })??;
        let _ = stream.set_nodelay(true);
        let (mut reader, writer) = stream.into_split();
        let writer = spawn_writer(writer);
        let conn = Arc::new(ConnState {
            writer,
            pending: std::sync::Mutex::new(HashMap::new()),
            subs: std::sync::Mutex::new(HashMap::new()),
            closed: std::sync::atomic::AtomicBool::new(false),
            next_corr: AtomicU64::new(1),
            next_sub_chan: AtomicU64::new(1),
        });
        let reader_conn = conn.clone();
        tokio::spawn(async move {
            let mut buf = BytesMut::with_capacity(64 * 1024);
            while let Ok((header, body)) = read_frame(&mut reader, &mut buf).await {
                reader_conn.route(header, body);
            }
            reader_conn.shatter();
        });
        let mut guard = self.conn.lock().await;
        if let Some(existing) = guard.as_ref()
            && existing.is_open()
        {
            // Raced another dialer: use the winner; ours tears down when
            // dropped (the peer sees the FIN and closes its side).
            return Ok(existing.clone());
        }
        *guard = Some(conn.clone());
        Ok(conn)
    }
}

/// The per-node registry of peer clients: one [`PeerClient`] per peer id,
/// created on first use. Addresses come from cluster membership.
#[derive(Debug, Default)]
pub struct Peers {
    clients: std::sync::Mutex<HashMap<NodeId, Arc<PeerClient>>>,
}

impl Peers {
    /// The client for `id`, creating it with `addr` on first use. If the
    /// peer's address changed (member replaced), the client is rebuilt.
    pub fn client(&self, id: NodeId, addr: &str) -> Arc<PeerClient> {
        let mut map = self.clients.lock().expect("peers lock");
        match map.get(&id) {
            Some(existing) if existing.addr() == addr => existing.clone(),
            _ => {
                let client = Arc::new(PeerClient::new(addr.to_owned()));
                map.insert(id, client.clone());
                client
            }
        }
    }

    /// The client for `id` if one exists (address already known).
    pub fn get(&self, id: NodeId) -> Option<Arc<PeerClient>> {
        self.clients.lock().expect("peers lock").get(&id).cloned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_round_trip_preserves_header_and_body() {
        let frame = OutFrame::new(
            FabricHeader::Deliver {
                sub_chan: 42,
                msg_id: 7,
            },
            Bytes::from_static(b"payload bytes"),
        );
        let mut out = BytesMut::new();
        encode_frame(&frame, &mut out).expect("encode");

        // Parse back by hand (read_frame needs a socket; the framing logic is
        // identical).
        let mut buf = out;
        let total = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        buf.advance(4);
        assert_eq!(buf.len(), total);
        let header_len = buf.get_u16() as usize;
        let header: FabricHeader = bincode::deserialize(&buf.split_to(header_len)).expect("header");
        assert_eq!(
            header,
            FabricHeader::Deliver {
                sub_chan: 42,
                msg_id: 7
            }
        );
        assert_eq!(&buf.freeze()[..], b"payload bytes");
    }

    #[test]
    fn raft_rpc_payloads_survive_bincode() {
        use openraft::raft::AppendEntriesRequest;
        use openraft::{BasicNode, Entry, EntryPayload, LogId, Vote};

        use super::super::queue_group::{QueueCommand, QueueTypeConfig};

        // The exact shape replicated for a quorum queue: an enqueue entry
        // with a Bytes body, inside an AppendEntries envelope.
        let entry = Entry::<QueueTypeConfig> {
            log_id: LogId::new(openraft::CommittedLeaderId::new(3, 1), 9),
            payload: EntryPayload::Normal(QueueCommand::Enqueue {
                body: Bytes::from_static(b"\x00\x01binary body\xff"),
                enqueued_ms: 42,
            }),
        };
        let req = AppendEntriesRequest::<QueueTypeConfig> {
            vote: Vote::new(3, 1),
            prev_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 8)),
            entries: vec![entry],
            leader_commit: Some(LogId::new(openraft::CommittedLeaderId::new(3, 1), 8)),
        };
        let bytes = bincode::serialize(&req).expect("serialize");
        let back: AppendEntriesRequest<QueueTypeConfig> =
            bincode::deserialize(&bytes).expect("deserialize");
        assert_eq!(back.entries.len(), 1);
        match &back.entries[0].payload {
            EntryPayload::Normal(QueueCommand::Enqueue { body, .. }) => {
                assert_eq!(&body[..], b"\x00\x01binary body\xff");
            }
            other => panic!("wrong payload: {other:?}"),
        }
        // A membership entry (BasicNode map) must also survive.
        let m = openraft::Membership::<NodeId, BasicNode>::new(
            vec![std::collections::BTreeSet::from([1u64, 2, 3])],
            std::collections::BTreeMap::from([
                (1u64, BasicNode::new("a:1")),
                (2, BasicNode::new("b:2")),
                (3, BasicNode::new("c:3")),
            ]),
        );
        let bytes = bincode::serialize(&m).expect("membership serialize");
        let back: openraft::Membership<NodeId, BasicNode> =
            bincode::deserialize(&bytes).expect("membership deserialize");
        assert_eq!(back.get_node(&2).map(|n| n.addr.as_str()), Some("b:2"));
    }

    #[tokio::test]
    async fn correlated_calls_pipeline_over_one_connection() {
        // A toy server that answers Request{corr} with Reply{corr} carrying
        // the request body reversed — out-of-order (replies to even corrs are
        // delayed), which correlation must absorb.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let (mut reader, writer) = stream.into_split();
            let writer = spawn_writer(writer);
            let mut buf = BytesMut::new();
            while let Ok((header, body)) = read_frame(&mut reader, &mut buf).await {
                if let FabricHeader::Request { corr, .. } = header {
                    let mut reversed = body.to_vec();
                    reversed.reverse();
                    let writer = writer.clone();
                    tokio::spawn(async move {
                        if corr % 2 == 0 {
                            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                        }
                        let _ = writer.send(OutFrame::new(
                            FabricHeader::Reply { corr },
                            Bytes::from(reversed),
                        ));
                    });
                }
            }
        });

        let client = PeerClient::new(addr);
        let conn = client.conn().await.expect("connect");
        let calls: Vec<_> = (0..8u8)
            .map(|i| {
                let conn = conn.clone();
                tokio::spawn(async move {
                    conn.call(
                        RequestKind::WhoLeads {
                            queue: "q".to_owned(),
                        },
                        Bytes::from(vec![i, i + 1, i + 2]),
                    )
                    .await
                })
            })
            .collect();
        for (i, call) in calls.into_iter().enumerate() {
            let i = i as u8;
            let body = call.await.unwrap().expect("reply");
            assert_eq!(&body[..], &[i + 2, i + 1, i], "reply matched to caller");
        }
    }

    #[tokio::test]
    async fn connection_loss_fails_pending_and_closes_subs() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        // Accept, then hang without answering; drop after a beat.
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
            drop(stream);
        });

        let client = PeerClient::new(addr);
        let conn = client.conn().await.expect("connect");
        let (events_tx, mut events_rx) = mpsc::unbounded_channel();
        // Register a sub directly (the real open_sub would await a reply that
        // never comes; we are testing teardown routing).
        conn.subs.lock().expect("subs lock").insert(9, events_tx);

        let pending = conn.call(
            RequestKind::WhoLeads {
                queue: "q".to_owned(),
            },
            Bytes::new(),
        );
        let err = pending.await.expect_err("connection died");
        assert!(err.contains("lost") || err.contains("closed") || err.contains("dropped"));
        match events_rx.recv().await {
            Some((9, SubEvent::Closed)) => {}
            other => panic!("expected sub closure, got {other:?}"),
        }
    }
}
