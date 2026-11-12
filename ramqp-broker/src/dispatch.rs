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

/// Subscribers whose connection was found gone while dispatching to them,
/// waiting for their in-flight messages to be requeued (issue #54).
///
/// A connection forwards its consumers' settlements, then their
/// `Unsubscribe`, into the queue's mailbox before it goes away — so by the
/// time a send to it fails, all of that is already queued. Requeuing the
/// subscriber's in-flight messages at once would get ahead of those
/// settlements: an accepted message would be requeued, its `Settle(Ack)`
/// would then match nothing, and the message would be delivered again.
/// So the requeue waits until the mailbox entries queued at that moment
/// have been processed (usually the connection's own `Unsubscribe` gets
/// there first and leaves nothing to do). Without the wait it would still
/// be needed: a connection that never sends an `Unsubscribe` must not strand
/// its messages (MED-9).
#[derive(Debug, Default)]
pub(crate) struct Orphans(Vec<(SubId, usize)>);

impl Orphans {
    /// `sub`'s connection is gone; `queued` messages are waiting in the
    /// mailbox right now.
    pub(crate) fn defer(&mut self, sub: SubId, queued: usize) {
        self.0.push((sub, queued));
    }

    /// One mailbox message has been processed.
    pub(crate) fn processed_one(&mut self) {
        for (_, left) in &mut self.0 {
            *left = left.saturating_sub(1);
        }
    }

    /// The subscribers whose wait is over (removed from the set).
    pub(crate) fn due(&mut self) -> Vec<SubId> {
        let mut due = Vec::new();
        self.0.retain(|(sub, left)| {
            if *left == 0 {
                due.push(*sub);
                false
            } else {
                true
            }
        });
        due
    }
}

#[cfg(test)]
mod orphan_tests {
    use super::Orphans;

    #[test]
    fn an_orphan_waits_for_what_was_queued_before_it() {
        let mut o = Orphans::default();
        o.defer(1, 2);
        o.defer(2, 0);
        assert_eq!(o.due(), vec![2], "nothing was queued: due at once");
        assert!(o.due().is_empty());
        o.processed_one();
        assert!(o.due().is_empty(), "one of two processed");
        o.processed_one();
        assert_eq!(o.due(), vec![1]);
    }
}
