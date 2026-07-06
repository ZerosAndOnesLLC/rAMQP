//! Shared consumer-dispatch primitives for the two queue actors.
//!
//! The transient queue ([`crate::queue`]) and the quorum queue
//! ([`crate::quorum`]) own different ready-sets — a `VecDeque` of in-memory
//! messages versus a `BTreeSet` of committed ids read back from the Raft state
//! machine — so the dispatch *loop* stays in each actor. But the subscriber
//! record, the round-robin pick, drain completion, and publish
//! confirm/refuse are identical; keeping them here stops the two copies
//! drifting (the demand/drain accounting in particular must stay in lockstep,
//! since the connection's credit reconciliation depends on it).

use tokio::sync::mpsc;

use crate::queue::{ConnCmd, PublishAck, SubId};

/// One consumer bound to a queue: where to deliver, its link identity, and its
/// current demand (link credit, reconciled by the connection).
#[derive(Debug)]
pub(crate) struct Subscriber {
    pub id: SubId,
    pub conn: mpsc::UnboundedSender<ConnCmd>,
    pub channel: u16,
    pub handle: u32,
    pub binding_gen: u64,
    pub demand: u32,
    /// A drain is in progress: zero any leftover demand after the next dispatch.
    pub drain_pending: bool,
}

impl Subscriber {
    /// A freshly subscribed consumer with no demand yet.
    pub fn new(
        id: SubId,
        conn: mpsc::UnboundedSender<ConnCmd>,
        channel: u16,
        handle: u32,
        binding_gen: u64,
    ) -> Self {
        Subscriber {
            id,
            conn,
            channel,
            handle,
            binding_gen,
            demand: 0,
            drain_pending: false,
        }
    }

    /// Add a demand grant (an increment) and note whether a drain is pending.
    pub fn grant(&mut self, credit: u32, drain: bool) {
        self.demand = self.demand.saturating_add(credit);
        self.drain_pending = drain;
    }
}

/// Round-robin: index of the next subscriber with demand, scanning from `rr`.
/// `None` when no subscriber currently has demand.
pub(crate) fn pick_ready(subs: &[Subscriber], rr: usize) -> Option<usize> {
    let n = subs.len();
    (0..n).find_map(|i| {
        let idx = (rr + i) % n;
        (subs[idx].demand > 0).then_some(idx)
    })
}

/// Drain completion: a subscriber that asked to drain keeps only the demand it
/// could satisfy this cycle; the rest is dropped so it is not re-armed against
/// messages that arrive later (the consumer said "then stop").
pub(crate) fn complete_drains(subs: &mut [Subscriber]) {
    for s in subs.iter_mut() {
        if s.drain_pending {
            s.demand = 0;
            s.drain_pending = false;
        }
    }
}

/// Confirm an unsettled publish to the producer (`accepted`); a no-op for a
/// pre-settled publish (`ack` is `None`).
pub(crate) fn confirm_publish(ack: Option<PublishAck>) {
    if let Some(ack) = ack {
        let _ = ack.conn.send(ConnCmd::SettleIncoming {
            channel: ack.channel,
            handle: ack.handle,
            binding_gen: ack.binding_gen,
            delivery_id: ack.delivery_id,
            accepted: true,
        });
    }
}

/// Refuse a publish that could not be stored (queue full / not committed):
/// answer the producer `rejected` when it wants confirmation, else log the
/// dropped pre-settled message.
pub(crate) fn refuse_publish(name: &str, ack: Option<PublishAck>) {
    match ack {
        Some(ack) => {
            let _ = ack.conn.send(ConnCmd::SettleIncoming {
                channel: ack.channel,
                handle: ack.handle,
                binding_gen: ack.binding_gen,
                delivery_id: ack.delivery_id,
                accepted: false,
            });
        }
        None => tracing::warn!(queue = %name, "pre-settled publish dropped: queue full"),
    }
}
