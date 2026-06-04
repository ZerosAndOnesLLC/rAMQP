//! Owner-local unsettled tracking (WP-4.3) — no locks (decision D-2).
//!
//! The map is ordered by delivery-id so range dispositions are cheap, and it can
//! be [snapshotted](UnsettledMap::snapshot) into the `attach.unsettled` form for
//! link recovery (Phase 6).

use std::collections::BTreeMap;

use bytes::Bytes;

use crate::codec::OrderedMap;
use crate::types::messaging::DeliveryState;

/// One in-flight (unsettled) delivery.
#[derive(Debug, Clone)]
pub struct UnsettledEntry {
    /// The delivery tag.
    pub delivery_tag: Bytes,
    /// The last known delivery state (if any disposition has been seen).
    pub state: Option<DeliveryState>,
    /// Whether this endpoint has settled the delivery.
    pub settled: bool,
}

/// Tracks unsettled deliveries for one link, keyed by delivery-id.
#[derive(Debug, Default)]
pub struct UnsettledMap {
    entries: BTreeMap<u32, UnsettledEntry>,
}

impl UnsettledMap {
    /// An empty map.
    pub fn new() -> Self {
        UnsettledMap {
            entries: BTreeMap::new(),
        }
    }

    /// Number of unsettled deliveries.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Whether there are no unsettled deliveries.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Begin tracking a delivery.
    pub fn insert(&mut self, delivery_id: u32, delivery_tag: Bytes, state: Option<DeliveryState>) {
        self.entries.insert(
            delivery_id,
            UnsettledEntry {
                delivery_tag,
                state,
                settled: false,
            },
        );
    }

    /// Look up an entry.
    pub fn get(&self, delivery_id: u32) -> Option<&UnsettledEntry> {
        self.entries.get(&delivery_id)
    }

    /// Whether a delivery id is tracked.
    pub fn contains(&self, delivery_id: u32) -> bool {
        self.entries.contains_key(&delivery_id)
    }

    /// Remove (settle) a delivery, returning its entry.
    pub fn settle(&mut self, delivery_id: u32) -> Option<UnsettledEntry> {
        self.entries.remove(&delivery_id)
    }

    /// Apply a disposition over the inclusive `[first, last]` id range, updating
    /// state and optionally settling. Returns the affected delivery ids in order.
    pub fn apply_disposition(
        &mut self,
        first: u32,
        last: u32,
        state: Option<&DeliveryState>,
        settled: bool,
    ) -> Vec<u32> {
        if settled {
            // The entries are about to be dropped, so skip cloning the state into
            // them — just collect the affected ids and remove them.
            let ids: Vec<u32> = self.entries.range(first..=last).map(|(id, _)| *id).collect();
            for id in &ids {
                self.entries.remove(id);
            }
            ids
        } else {
            // Update state in a single mutable pass (no second get_mut lookup).
            let mut ids = Vec::new();
            for (id, entry) in self.entries.range_mut(first..=last) {
                if state.is_some() {
                    entry.state = state.cloned();
                }
                entry.settled = false;
                ids.push(*id);
            }
            ids
        }
    }

    /// Snapshot the map into the `attach.unsettled` form (tag → state) for link
    /// recovery, ordered by delivery-id.
    pub fn snapshot(&self) -> OrderedMap<Bytes, DeliveryState> {
        let mut map = OrderedMap::with_capacity(self.entries.len());
        for entry in self.entries.values() {
            if let Some(state) = &entry.state {
                map.push(entry.delivery_tag.clone(), state.clone());
            }
        }
        map
    }

    /// Iterate `(delivery_id, entry)` in id order.
    pub fn iter(&self) -> impl Iterator<Item = (u32, &UnsettledEntry)> {
        self.entries.iter().map(|(id, e)| (*id, e))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::definitions::AmqpError;
    use crate::types::messaging::{Accepted, Rejected};

    #[test]
    fn insert_settle_and_range() {
        let mut m = UnsettledMap::new();
        for id in 1..=5 {
            m.insert(id, Bytes::from(vec![id as u8]), None);
        }
        assert_eq!(m.len(), 5);

        // accept ids 2..=4, settled
        let affected = m.apply_disposition(
            2,
            4,
            Some(&DeliveryState::Accepted(Accepted::default())),
            true,
        );
        assert_eq!(affected, vec![2, 3, 4]);
        assert_eq!(m.len(), 2); // 1 and 5 remain
        assert!(m.contains(1) && m.contains(5));

        // reject id 1, not settled yet
        m.apply_disposition(
            1,
            1,
            Some(&DeliveryState::Rejected(Rejected {
                error: Some(crate::types::definitions::Error::new(
                    AmqpError::NotFound,
                    None,
                )),
            })),
            false,
        );
        let e = m.get(1).unwrap();
        assert!(!e.settled);
        assert!(matches!(e.state, Some(DeliveryState::Rejected(_))));
    }

    #[test]
    fn snapshot_preserves_states_in_order() {
        let mut m = UnsettledMap::new();
        m.insert(
            2,
            Bytes::from_static(b"b"),
            Some(DeliveryState::Accepted(Accepted::default())),
        );
        m.insert(
            1,
            Bytes::from_static(b"a"),
            Some(DeliveryState::Accepted(Accepted::default())),
        );
        let snap = m.snapshot();
        // ordered by delivery-id (1 then 2)
        let tags: Vec<_> = snap.iter().map(|(k, _)| k.clone()).collect();
        assert_eq!(
            tags,
            vec![Bytes::from_static(b"a"), Bytes::from_static(b"b")]
        );
    }
}
