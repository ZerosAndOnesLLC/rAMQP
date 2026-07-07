//! Broker configuration.

use ramqp_core::config::{ConnectionConfig, SessionConfig};

/// Configuration for a broker instance.
///
/// Connection-level knobs reuse the role-neutral [`ConnectionConfig`]
/// (container-id, `max-frame-size`, `channel-max`, idle-timeout). Its
/// `connect_timeout` bounds the **inbound handshake** here (header + SASL +
/// `open`), guarding against a client that opens the socket then stalls
/// (slow-loris); its client-only fields (`command_buffer`, `reconnect`) are
/// unused by the broker.
#[derive(Debug, Clone)]
pub struct BrokerConfig {
    /// Connection-level settings (also the source of our `open`).
    pub connection: ConnectionConfig,
    /// Session window defaults for accepted sessions.
    pub session: SessionConfig,
    /// Link credit granted to an inbound producer link at attach (topped up
    /// in batches as deliveries are ingested).
    pub initial_credit: u32,
    /// Per-delivery size cap for inbound producer links (`None` = only the
    /// built-in hard ceiling).
    pub max_message_size: Option<u64>,
    /// Maximum messages a queue holds (ready + unacked) before refusing
    /// publishes (`rejected`, `resource-limit-exceeded`). Bounded always —
    /// an unbounded queue is an OOM (broker.md §3.2).
    pub max_queue_depth: usize,
    /// Maximum number of concurrently established connections. Once reached,
    /// further accepted sockets are closed immediately (rather than spawning
    /// unbounded per-connection state) — a network-facing DoS guard. Each
    /// connection also holds per-connection buffers and a task, so this is the
    /// primary memory/fd bound. `0` disables the cap (not recommended on a
    /// public bind).
    pub max_connections: usize,
    /// Maximum number of distinct queues that may be auto-declared. Since any
    /// attach declares a queue on first use, this bounds how many actors (and,
    /// for `/quorum/*`, Raft groups) a client can spawn — an unbounded-
    /// allocation DoS guard. At the cap, an attach to a *new* address is
    /// refused. `0` disables the cap.
    pub max_queues: usize,
    /// Cluster membership, when this broker is one node of a cluster.
    /// `None` (the default) runs standalone: `/quorum/*` queues are
    /// single-replica groups on this node.
    pub cluster: Option<ClusterMemberConfig>,
    /// Where the broker keeps on-disk state: durable-queue data
    /// (`/durable/<name>`, needs the `store-redb` feature) and quorum-queue
    /// paging + snapshot blobs. `None` (the default) refuses durable
    /// attaches and keeps quorum queues fully in memory.
    pub data_dir: Option<std::path::PathBuf>,
    /// Per-quorum-queue resident-body budget: bodies beyond this many bytes
    /// page out to disk under `data_dir` (deep queues must not live in RAM —
    /// broker.md §3.1/§8). Ignored without a `data_dir`.
    pub resident_bytes_max: usize,
}

/// Cluster membership settings for one broker node.
///
/// Every node runs the inter-node **fabric** (Raft replication + queue
/// forwarding) on `listen`; the founding members are the static `seeds`
/// list, identical on every node. The lowest seed id forms the cluster;
/// formation is idempotent, so restarts and start-order races are safe.
#[derive(Debug, Clone)]
pub struct ClusterMemberConfig {
    /// This node's id (must appear in `seeds`, unique per node).
    pub node_id: u64,
    /// The fabric listen address (e.g. `0.0.0.0:7472`).
    pub listen: String,
    /// All founding members: `(node id, fabric address as peers reach it)`.
    pub seeds: Vec<(u64, String)>,
    /// Replica count for newly declared quorum queues (capped at the current
    /// cluster size at declaration).
    pub replicas: u8,
}

impl ClusterMemberConfig {
    /// A member config with the conventional replication factor of 3.
    pub fn new(node_id: u64, listen: impl Into<String>, seeds: Vec<(u64, String)>) -> Self {
        ClusterMemberConfig {
            node_id,
            listen: listen.into(),
            seeds,
            replicas: 3,
        }
    }
}

impl Default for BrokerConfig {
    fn default() -> Self {
        BrokerConfig {
            connection: ConnectionConfig::default(),
            session: SessionConfig::default(),
            initial_credit: 512,
            max_message_size: Some(16 * 1024 * 1024),
            max_queue_depth: 1_000_000,
            max_connections: 16_384,
            max_queues: 100_000,
            cluster: None,
            data_dir: None,
            resident_bytes_max: 64 * 1024 * 1024,
        }
    }
}
