//! The metadata group's replicated state: the queue catalog.
//!
//! Commands are exactly the state transitions a broker node may propose;
//! they are committed through Raft and applied deterministically on every
//! node, so the catalog (and later: placement, policies) is identical
//! cluster-wide.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Whether a queue is replicated (its own Raft group) or node-local.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum QueueType {
    /// Replicated across a quorum of nodes (Phase 6).
    Quorum,
    /// Single-node, no consensus (the Phase 4 in-memory queue).
    Transient,
}

/// A declared queue's replicated description.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct QueueSpec {
    /// Replication mode.
    pub queue_type: QueueType,
    /// Desired replica count for quorum queues (placement assigns nodes).
    pub replicas: u8,
}

/// A state transition proposed to the metadata group.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaCommand {
    /// Declare a queue.
    CreateQueue {
        /// The queue name (post address-normalization).
        name: String,
        /// Its replicated description.
        spec: QueueSpec,
    },
    /// Delete a queue.
    DeleteQueue {
        /// The queue name.
        name: String,
    },
}

/// The applied result of a [`MetaCommand`].
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaResponse {
    /// The queue was created.
    Created,
    /// A queue with that name already exists (create was a no-op).
    AlreadyExists,
    /// The queue was deleted.
    Deleted,
    /// No queue with that name exists (delete was a no-op).
    NotFound,
}

/// The deterministic state machine: apply a command to the catalog.
pub(crate) fn apply(
    catalog: &mut BTreeMap<String, QueueSpec>,
    command: &MetaCommand,
) -> MetaResponse {
    match command {
        MetaCommand::CreateQueue { name, spec } => {
            if catalog.contains_key(name) {
                MetaResponse::AlreadyExists
            } else {
                catalog.insert(name.clone(), spec.clone());
                MetaResponse::Created
            }
        }
        MetaCommand::DeleteQueue { name } => {
            if catalog.remove(name).is_some() {
                MetaResponse::Deleted
            } else {
                MetaResponse::NotFound
            }
        }
    }
}
