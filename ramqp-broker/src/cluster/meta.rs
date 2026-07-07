//! The metadata group's replicated state: the queue catalog.
//!
//! Commands are exactly the state transitions a broker node may propose;
//! they are committed through Raft and applied deterministically on every
//! node, so the catalog (queue type, replica placement, and later: policies)
//! is identical cluster-wide.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::NodeId;

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
    /// Desired replica count for quorum queues.
    pub replicas: u8,
    /// The nodes hosting this queue's Raft group, chosen at declaration
    /// (rendezvous placement over the then-current voters). Stable until an
    /// explicit rebalance — membership churn does not silently move queues.
    pub placement: Vec<NodeId>,
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
    /// The queue was created with the proposed spec.
    Created,
    /// A queue with that name already exists (create was a no-op); the
    /// authoritative spec is returned so a racing declarer adopts the
    /// winner's placement instead of its own proposal.
    AlreadyExists(QueueSpec),
    /// The queue was deleted.
    Deleted,
    /// No queue with that name exists (delete was a no-op).
    NotFound,
}

/// Why a forwarded catalog write failed (rides the fabric as a typed error).
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum MetaWriteError {
    /// The receiving node is not the metadata leader; retry against the hint.
    NotLeader(Option<NodeId>),
    /// Anything else (shutting down, storage failure, ...).
    Other(String),
}

impl std::fmt::Display for MetaWriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MetaWriteError::NotLeader(hint) => write!(f, "not the metadata leader (try {hint:?})"),
            MetaWriteError::Other(msg) => f.write_str(msg),
        }
    }
}

/// The deterministic state machine: apply a command to the catalog.
pub(crate) fn apply(
    catalog: &mut BTreeMap<String, QueueSpec>,
    command: &MetaCommand,
) -> MetaResponse {
    match command {
        MetaCommand::CreateQueue { name, spec } => {
            if let Some(existing) = catalog.get(name) {
                MetaResponse::AlreadyExists(existing.clone())
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
