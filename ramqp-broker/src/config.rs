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
    /// Maximum BYTES of message bodies a queue holds (ready + unacked)
    /// before refusing publishes. The depth bound alone admits
    /// `max_queue_depth × max_message_size` (~16 TiB at the defaults) — this
    /// is the actual memory bound for transient and in-memory quorum queues
    /// (and the disk bound for durable ones). Overridden per queue by
    /// [`QueuePolicy::max_length_bytes`]. `0` disables (not recommended).
    pub max_queue_bytes: usize,
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
    ///
    /// **Clustered** with `None`, Raft hard state (votes, log) is in-memory
    /// only: a node restart violates Raft's durability assumptions — the
    /// restarted voter can vote twice in one term, and committed messages
    /// can be silently lost. The broker warns loudly at bootstrap; use a
    /// data dir (with `store-redb`) for any cluster that must survive
    /// restarts.
    pub data_dir: Option<std::path::PathBuf>,
    /// Per-quorum-queue resident-body budget: bodies beyond this many bytes
    /// page out to disk under `data_dir` (deep queues must not live in RAM —
    /// broker.md §3.1/§8). Ignored without a `data_dir`.
    pub resident_bytes_max: usize,
    /// Management/metrics HTTP listen address (e.g. `127.0.0.1:15692`):
    /// `GET /metrics` (Prometheus) and `GET /queues` (JSON). `None` (the
    /// default) disables the endpoint. No auth — bind it to loopback or a
    /// management network.
    pub management_listen: Option<String>,
    /// Queue policies: `(name prefix, policy)` pairs, first match wins (an
    /// empty prefix matches every queue). Matched against the normalized
    /// queue name (no `/queues/` etc. prefix) at declaration. The management
    /// API (Phase 9) will supersede this interim surface.
    pub policies: Vec<(String, QueuePolicy)>,
}

/// Per-queue behavior policies: TTL, length bounds, dead-lettering.
#[derive(Debug, Clone, Default)]
pub struct QueuePolicy {
    /// Messages older than this are expired instead of delivered (checked
    /// lazily when a message reaches the head of the queue, RabbitMQ-classic
    /// style). Expired messages dead-letter when `dead_letter` is set,
    /// otherwise drop.
    pub message_ttl: Option<std::time::Duration>,
    /// Maximum messages held (ready + unacked), overriding the broker-wide
    /// `max_queue_depth` for matching queues.
    pub max_length: Option<usize>,
    /// Maximum bytes of message bodies held (ready + unacked), overriding
    /// the broker-wide `max_queue_bytes` for matching queues.
    pub max_length_bytes: Option<usize>,
    /// What happens to a publish that would exceed `max_length`.
    pub overflow: OverflowBehavior,
    /// Where expired / dropped / delivery-exhausted messages go: any queue
    /// address (e.g. `/queues/dead`, `/durable/dead`). Best-effort delivery
    /// (a full or missing dead-letter queue drops). A queue whose resolved
    /// target is itself (e.g. the DLX queue matching a catch-all prefix)
    /// has dead-lettering disabled; longer cycles (a→b→a) are NOT detected —
    /// do not point queues' dead-letter targets at each other.
    pub dead_letter: Option<String>,
    /// After this many failed delivery attempts (`modified{delivery-failed}`
    /// requeues), the message is dead-lettered (or dropped) instead of
    /// requeued.
    pub max_delivery_attempts: Option<u32>,
}

/// Overflow behavior at [`QueuePolicy::max_length`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum OverflowBehavior {
    /// Refuse the new publish (`rejected`, `resource-limit-exceeded`) — the
    /// default, matching the broker-wide depth cap.
    #[default]
    RejectPublish,
    /// Make room: drop (or dead-letter) the oldest ready message and accept
    /// the new one.
    DropHead,
}

/// Cluster membership settings for one broker node.
///
/// Every node runs the inter-node **fabric** (Raft replication + queue
/// forwarding) on `listen`; the founding members are the static `seeds`
/// list, identical on every node. The lowest seed id forms the cluster;
/// formation is idempotent, so restarts and start-order races are safe.
///
/// # Security
/// The fabric port carries **no authentication and no encryption**: any
/// host that can open a TCP connection to it can publish to, consume from,
/// and acknowledge every queue this node leads, rewrite the replicated
/// queue catalog, and inject Raft RPCs into every group (a forged
/// high-term vote alone is a cluster-wide liveness DoS). Until fabric
/// auth/TLS lands, the fabric MUST run on an isolated, trusted network —
/// a private VLAN/VPC, a WireGuard mesh, or strict firewall rules limiting
/// the port to the cluster's own nodes. The broker logs a warning at
/// startup when the fabric binds a non-loopback address.
#[derive(Debug, Clone)]
pub struct ClusterMemberConfig {
    /// This node's id (must appear in `seeds`, unique per node).
    pub node_id: u64,
    /// The fabric listen address (e.g. `0.0.0.0:7472`). See the
    /// struct-level **Security** note: this port must only be reachable
    /// from the cluster's own nodes.
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
            max_queue_bytes: 1024 * 1024 * 1024,
            max_connections: 16_384,
            max_queues: 100_000,
            cluster: None,
            data_dir: None,
            resident_bytes_max: 64 * 1024 * 1024,
            management_listen: None,
            policies: Vec::new(),
        }
    }
}
