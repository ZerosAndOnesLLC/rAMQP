//! Link recovery / unsettled replay decision matrix (WP-6.2).
//!
//! On re-attach with the same link name, both endpoints exchange their
//! `unsettled` maps (delivery-tag → delivery-state). This module computes, for
//! each in-flight delivery, the [`ResumeAction`] to take — the resend / resume /
//! settle / abort matrix from the AMQP 1.0 resumption rules — fed by the
//! snapshot-able [`UnsettledMap`].
//!
//! The supervisor (Phase 6) drives the actual re-attach + replay; this is the
//! reconciliation logic it applies, isolated and table-tested.

use bytes::Bytes;

use crate::codec::OrderedMap;
use crate::link::settlement::UnsettledMap;
use crate::types::messaging::DeliveryState;

/// What to do with a single unsettled delivery on link resume.
#[derive(Debug, Clone, PartialEq)]
pub enum ResumeAction {
    /// The peer never tracked it (absent from its unsettled map and we had not
    /// settled): re-deliver from scratch. Honors at-least-once (may duplicate).
    Resend,
    /// The peer is still tracking it without a terminal outcome: resume the
    /// transfer in place (`transfer.resume = true`).
    Resume,
    /// The peer reached a terminal outcome: apply it and settle locally.
    Settle(DeliveryState),
    /// Already settled on our side and the peer no longer tracks it: forget it.
    Abort,
}

/// The resume action for one sender-side delivery, given whether we had settled
/// it and the peer's state for it (from its resumed `unsettled` map).
pub fn sender_resume_action(local_settled: bool, remote: Option<&DeliveryState>) -> ResumeAction {
    match remote {
        Some(state) if state.is_terminal() => ResumeAction::Settle(state.clone()),
        Some(_) => ResumeAction::Resume,
        None if local_settled => ResumeAction::Abort,
        None => ResumeAction::Resend,
    }
}

/// Reconcile our unsettled deliveries against the peer's resumed `unsettled`
/// map, returning the per-delivery `(delivery_id, action)` pairs in id order.
pub fn reconcile(
    local: &UnsettledMap,
    remote: &OrderedMap<Bytes, DeliveryState>,
) -> Vec<(u32, ResumeAction)> {
    local
        .iter()
        .map(|(id, entry)| {
            let remote_state = remote.get(&entry.delivery_tag);
            (id, sender_resume_action(entry.settled, remote_state))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::messaging::{Accepted, Received, Rejected};

    fn accepted() -> DeliveryState {
        DeliveryState::Accepted(Accepted::default())
    }
    fn received() -> DeliveryState {
        DeliveryState::Received(Received {
            section_number: 0,
            section_offset: 0,
        })
    }
    fn rejected() -> DeliveryState {
        DeliveryState::Rejected(Rejected { error: None })
    }

    #[test]
    fn sender_matrix() {
        // peer reached a terminal outcome → apply + settle
        assert_eq!(
            sender_resume_action(false, Some(&accepted())),
            ResumeAction::Settle(accepted())
        );
        assert_eq!(
            sender_resume_action(false, Some(&rejected())),
            ResumeAction::Settle(rejected())
        );
        // peer partially received (non-terminal) → resume in place
        assert_eq!(
            sender_resume_action(false, Some(&received())),
            ResumeAction::Resume
        );
        // peer doesn't track it and we hadn't settled → resend (no loss)
        assert_eq!(sender_resume_action(false, None), ResumeAction::Resend);
        // peer doesn't track it and we had settled → forget
        assert_eq!(sender_resume_action(true, None), ResumeAction::Abort);
    }

    #[test]
    fn reconcile_over_maps() {
        let mut local = UnsettledMap::new();
        local.insert(1, Bytes::from_static(b"a"), None); // remote terminal → settle
        local.insert(2, Bytes::from_static(b"b"), None); // remote received → resume
        local.insert(3, Bytes::from_static(b"c"), None); // remote absent → resend

        let remote = OrderedMap::from(vec![
            (Bytes::from_static(b"a"), accepted()),
            (Bytes::from_static(b"b"), received()),
        ]);

        let actions = reconcile(&local, &remote);
        assert_eq!(
            actions,
            vec![
                (1, ResumeAction::Settle(accepted())),
                (2, ResumeAction::Resume),
                (3, ResumeAction::Resend),
            ]
        );
    }
}
