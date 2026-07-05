//! Queue registry: address → queue resolution and on-demand declaration.
//!
//! Address model (interim until the management API lands in Phase 9):
//! `/queues/<name>` and bare names declare **transient** queues (the Phase 4
//! in-memory actor); `/quorum/<name>` declares a **quorum** queue backed by a
//! per-queue Raft group (single-replica in this slice — multi-node placement
//! arrives with the forwarding fabric).
//!
//! Resolution happens only at attach time (never per-message), so an async
//! mutex around the map is fine — the message path stays lock-free.

use std::collections::HashMap;
use std::sync::Arc;

use openraft::BasicNode;

use crate::cluster::NodeId;
use crate::cluster::network::UnreachableNetwork;
use crate::cluster::queue_group::{QueueRaft, QueueStore};
use crate::queue::{self, QueueHandle};
use crate::quorum;

/// How a resolved address wants its queue backed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum QueueKind {
    /// Node-local, no consensus (the Phase 4 actor).
    Transient,
    /// Backed by a per-queue Raft group.
    Quorum,
}

/// Resolves addresses to queue actors, declaring queues on first use.
#[derive(Debug)]
pub(crate) struct QueueRegistry {
    queues: tokio::sync::Mutex<HashMap<String, QueueHandle>>,
    max_depth: usize,
    /// This node's id for single-replica queue groups.
    node_id: NodeId,
}

impl QueueRegistry {
    pub fn new(max_depth: usize) -> Self {
        QueueRegistry {
            queues: tokio::sync::Mutex::new(HashMap::new()),
            max_depth,
            node_id: 1,
        }
    }

    /// Normalize an AMQP address to `(kind, queue name)`. Accepts the
    /// RabbitMQ-4.x style `/queues/<name>`, `/quorum/<name>` for replicated
    /// queues, and bare names (with or without a leading `/`) as transient.
    pub fn parse_address(address: &str) -> Option<(QueueKind, &str)> {
        if let Some(name) = address.strip_prefix("/quorum/") {
            return (!name.is_empty()).then_some((QueueKind::Quorum, name));
        }
        let name = address
            .strip_prefix("/queues/")
            .unwrap_or_else(|| address.trim_start_matches('/'));
        (!name.is_empty()).then_some((QueueKind::Transient, name))
    }

    /// Resolve an address, declaring the queue if it doesn't exist.
    pub async fn resolve(&self, address: &str) -> Option<QueueHandle> {
        let (kind, name) = Self::parse_address(address)?;
        // Kind-qualified key: `/queues/foo` and `/quorum/foo` are distinct.
        let key = match kind {
            QueueKind::Transient => format!("t:{name}"),
            QueueKind::Quorum => format!("q:{name}"),
        };
        let mut map = self.queues.lock().await;
        if let Some(q) = map.get(&key) {
            return Some(q.clone());
        }
        let handle = match kind {
            QueueKind::Transient => queue::spawn(name.to_owned(), self.max_depth),
            QueueKind::Quorum => {
                spawn_quorum_group(name.to_owned(), self.node_id, self.max_depth).await?
            }
        };
        map.insert(key, handle.clone());
        Some(handle)
    }
}

/// Start a single-replica queue group and its quorum actor.
async fn spawn_quorum_group(
    name: String,
    node_id: NodeId,
    max_depth: usize,
) -> Option<QueueHandle> {
    let config = openraft::Config {
        heartbeat_interval: 100,
        election_timeout_min: 300,
        election_timeout_max: 600,
        // Compaction: snapshot every 5k applied entries and keep only a short
        // log tail behind it, so log memory tracks queue depth rather than
        // total messages ever enqueued (broker.md §3.2 bounded-memory rule).
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(5_000),
        max_in_snapshot_log_to_keep: 1_000,
        purge_batch_size: 1_000,
        ..Default::default()
    }
    .validate()
    .ok()?;
    let store = QueueStore::default();
    let (log_store, state_machine) = openraft::storage::Adaptor::new(store.clone());
    let raft = QueueRaft::new(
        node_id,
        Arc::new(config),
        UnreachableNetwork,
        log_store,
        state_machine,
    )
    .await
    .ok()?;
    raft.initialize(std::collections::BTreeMap::from([(
        node_id,
        BasicNode::new("local"),
    )]))
    .await
    .ok()?;
    raft.wait(Some(std::time::Duration::from_secs(10)))
        .current_leader(node_id, "single-replica leader")
        .await
        .ok()?;
    Some(quorum::spawn(name, raft, store, max_depth))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_normalization() {
        assert_eq!(
            QueueRegistry::parse_address("/queues/orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("/orders"),
            Some((QueueKind::Transient, "orders"))
        );
        assert_eq!(
            QueueRegistry::parse_address("/quorum/orders"),
            Some((QueueKind::Quorum, "orders"))
        );
        assert_eq!(QueueRegistry::parse_address("/queues/"), None);
        assert_eq!(QueueRegistry::parse_address("/quorum/"), None);
        assert_eq!(QueueRegistry::parse_address(""), None);
    }

    #[tokio::test]
    async fn resolve_is_idempotent_and_kind_scoped() {
        let r = QueueRegistry::new(10);
        let a = r.resolve("/queues/q1").await.unwrap();
        let b = r.resolve("q1").await.unwrap();
        assert!(a.tx.same_channel(&b.tx), "same transient queue");

        let quorum = r.resolve("/quorum/q1").await.unwrap();
        assert!(
            !a.tx.same_channel(&quorum.tx),
            "quorum q1 is a distinct queue from transient q1"
        );
        let quorum2 = r.resolve("/quorum/q1").await.unwrap();
        assert!(quorum.tx.same_channel(&quorum2.tx));
    }
}
