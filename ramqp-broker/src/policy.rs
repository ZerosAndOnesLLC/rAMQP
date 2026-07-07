//! Queue-policy runtime (broker.md Phase 7): TTL, max-length, and
//! dead-lettering, enforced inside the queue actors.
//!
//! Policies are declared in [`crate::config::BrokerConfig::policies`] and
//! resolved to an [`EffectivePolicy`] when a queue is declared. Dead-letter
//! traffic rides one broker-wide router task: actors emit
//! `(target address, body)` pairs on an unbounded channel (bounded by the
//! emitting queue's own depth), and the router resolves the target through
//! the registry and republishes best-effort (pre-settled; a full or missing
//! dead-letter queue drops — there is no infinite retry).

use std::sync::Arc;

use bytes::Bytes;
use tokio::sync::mpsc;

use crate::config::{OverflowBehavior, QueuePolicy};
use crate::queue::QueueMsg;
use crate::registry::QueueRegistry;

/// One message bound for a dead-letter queue.
///
/// Targets resolve in the DEFAULT namespace: a per-vhost policy addresses a
/// tenant's dead-letter queue by its qualified name (e.g.
/// `/queues/<vhost>/dead`), which lands on the same storage key as
/// `/queues/dead` resolved inside that vhost — the qualification scheme
/// composes, no inference needed.
#[derive(Debug)]
pub(crate) struct DeadLetter {
    /// The dead-letter target address (any queue address).
    pub target: String,
    /// The raw message bytes.
    pub body: Bytes,
    /// When set, resolved (fired or dropped) once the copy's fate is known —
    /// durably stored by the target, refused, or unroutable. A durable
    /// source orders its own Remove after this (MED-12): removing first
    /// opens a crash window that loses the message from both the source and
    /// the DLQ.
    pub confirm: Option<tokio::sync::oneshot::Sender<()>>,
}

/// Where actors send dead letters.
pub(crate) type DeadLetterSender = mpsc::UnboundedSender<DeadLetter>;

/// Spawn the broker-wide dead-letter router.
///
/// Holds the registry WEAKLY: the router must not keep a shut-down broker's
/// registry (and its durable store / file lock) alive — actors' policy
/// handles keep the channel open long after the broker is gone.
pub(crate) fn spawn_dlx_router(registry: &Arc<QueueRegistry>) -> DeadLetterSender {
    let weak = Arc::downgrade(registry);
    let (tx, mut rx) = mpsc::unbounded_channel::<DeadLetter>();
    tokio::spawn(async move {
        while let Some(dl) = rx.recv().await {
            let Some(registry) = weak.upgrade() else {
                return; // broker gone; nothing to route into
            };
            match registry.resolve(&dl.target).await {
                Some(queue) => {
                    // Confirmed dead letters ride with an ack so the source
                    // can order its durable Remove after the copy's fate is
                    // known; the rest stay pre-settled (best-effort by
                    // contract).
                    let waiter = dl.confirm.map(|confirm| {
                        let (ack_tx, ack_rx) = mpsc::unbounded_channel();
                        (
                            confirm,
                            ack_rx,
                            crate::queue::PublishAck {
                                conn: ack_tx,
                                channel: 0,
                                handle: 0,
                                binding_gen: 0,
                                delivery_id: 0,
                            },
                        )
                    });
                    let (ack, waiter) = match waiter {
                        Some((confirm, ack_rx, ack)) => (Some(ack), Some((confirm, ack_rx))),
                        None => (None, None),
                    };
                    if queue
                        .tx
                        .send(QueueMsg::Publish {
                            body: dl.body,
                            ack,
                        })
                        .await
                        .is_err()
                    {
                        // `waiter` (and its confirm) drops here: the fate is
                        // resolved — dropped — and the source may proceed.
                        tracing::warn!(target = %dl.target, "dead-letter queue actor gone; message dropped");
                    } else if let Some((confirm, mut ack_rx)) = waiter {
                        // Await the target's own durability confirm off this
                        // task: a slow durable/quorum DLQ must not stall
                        // dead-lettering broker-wide.
                        tokio::spawn(async move {
                            let _ = ack_rx.recv().await;
                            let _ = confirm.send(());
                        });
                    }
                }
                None => {
                    // `dl.confirm` (if any) drops with `dl`: fate resolved.
                    tracing::warn!(target = %dl.target, "dead-letter target unresolvable; message dropped");
                }
            }
        }
    });
    tx
}

/// A queue's resolved policy, with everything the actor needs at runtime.
#[derive(Debug, Clone)]
pub(crate) struct EffectivePolicy {
    /// Message TTL in milliseconds (lazy head-of-queue expiry).
    pub ttl_ms: Option<u64>,
    /// Effective depth bound (policy `max_length`, else the broker-wide
    /// `max_queue_depth`).
    pub max_len: usize,
    /// Effective byte bound on held bodies (`usize::MAX` = unbounded):
    /// policy `max_length_bytes`, else the broker-wide `max_queue_bytes`.
    pub max_bytes: usize,
    /// `true` → drop the oldest ready message to admit a new one at the
    /// bound; `false` → reject the publish.
    pub drop_head: bool,
    /// Dead-letter address for expired / dropped / exhausted messages.
    pub dead_letter: Option<String>,
    /// Failed-delivery cap before dead-lettering instead of requeueing.
    pub max_attempts: Option<u32>,
    /// The router channel (`None` in unit tests without a router).
    pub dlx: Option<DeadLetterSender>,
}

impl EffectivePolicy {
    /// The no-policy default: only the broker-wide depth bound (bytes
    /// unbounded — unit-test convenience).
    pub fn depth_only(max_len: usize) -> Self {
        EffectivePolicy {
            ttl_ms: None,
            max_len,
            max_bytes: usize::MAX,
            drop_head: false,
            dead_letter: None,
            max_attempts: None,
            dlx: None,
        }
    }

    /// Resolve the first matching policy (prefix match on the normalized
    /// queue name) against the broker config. `global_max_bytes == 0`
    /// disables the byte bound.
    pub fn resolve(
        config_policies: &[(String, QueuePolicy)],
        queue: &str,
        global_max_depth: usize,
        global_max_bytes: usize,
        dlx: Option<DeadLetterSender>,
    ) -> Self {
        let global_bytes = if global_max_bytes == 0 {
            usize::MAX
        } else {
            global_max_bytes
        };
        let matched = config_policies
            .iter()
            .find(|(prefix, _)| queue.starts_with(prefix.as_str()))
            .map(|(_, p)| p);
        match matched {
            Some(p) => EffectivePolicy {
                ttl_ms: p.message_ttl.map(|d| d.as_millis() as u64),
                max_len: p.max_length.unwrap_or(global_max_depth),
                max_bytes: p.max_length_bytes.unwrap_or(global_bytes),
                drop_head: p.overflow == OverflowBehavior::DropHead,
                dead_letter: p.dead_letter.clone(),
                max_attempts: p.max_delivery_attempts,
                dlx,
            },
            None => EffectivePolicy {
                max_bytes: global_bytes,
                ..Self::depth_only(global_max_depth)
            },
        }
    }

    /// Whether a message enqueued at `enqueued_ms` has outlived its TTL.
    pub fn expired(&self, enqueued_ms: u64, now_ms: u64) -> bool {
        self.ttl_ms
            .is_some_and(|ttl| now_ms.saturating_sub(enqueued_ms) >= ttl)
    }

    /// Whether a message with this many failed attempts is out of retries.
    pub fn attempts_exhausted(&self, failures: u32) -> bool {
        self.max_attempts.is_some_and(|max| failures >= max)
    }

    /// Route one dead message: to the dead-letter queue when configured,
    /// otherwise it just drops.
    pub fn dead_letter(&self, queue: &str, reason: &str, body: Bytes) {
        match (&self.dead_letter, &self.dlx) {
            (Some(target), Some(dlx)) => {
                let _ = dlx.send(DeadLetter {
                    target: target.clone(),
                    body,
                    confirm: None,
                });
            }
            (Some(target), None) => {
                tracing::warn!(queue = %queue, %target, reason, "no dead-letter router; message dropped");
            }
            (None, _) => {
                tracing::debug!(queue = %queue, reason, "message dropped (no dead-letter target)");
            }
        }
    }

    /// Route one dead message like [`dead_letter`](Self::dead_letter), and —
    /// when it actually rides to a dead-letter queue — hand back a receiver
    /// that resolves once the copy's fate is known (durably stored, refused,
    /// or dropped). A durable source orders its own Remove after it; `None`
    /// means the message simply dropped and the caller may proceed at once.
    pub fn dead_letter_ordered(
        &self,
        queue: &str,
        reason: &str,
        body: Bytes,
    ) -> Option<tokio::sync::oneshot::Receiver<()>> {
        match (&self.dead_letter, &self.dlx) {
            (Some(target), Some(dlx)) => {
                let (confirm, resolved) = tokio::sync::oneshot::channel();
                // A dead router drops the message (and the confirm with it):
                // the receiver resolves immediately and the caller proceeds.
                let _ = dlx.send(DeadLetter {
                    target: target.clone(),
                    body,
                    confirm: Some(confirm),
                });
                Some(resolved)
            }
            (Some(target), None) => {
                tracing::warn!(queue = %queue, %target, reason, "no dead-letter router; message dropped");
                None
            }
            (None, _) => {
                tracing::debug!(queue = %queue, reason, "message dropped (no dead-letter target)");
                None
            }
        }
    }
}

/// Milliseconds since the Unix epoch (TTL timestamps).
pub(crate) fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}
