//! Queue registry: address → queue resolution and on-demand declaration.
//!
//! The registry is touched only at attach time (never per-message), so a
//! plain `RwLock<HashMap>` is fine — the hot path stays lock-free.

use std::collections::HashMap;
use std::sync::RwLock;

use crate::queue::{self, QueueHandle};

/// Resolves addresses to queue actors, declaring transient queues on first
/// use (explicit declaration/management arrives in Phase 9).
#[derive(Debug)]
pub(crate) struct QueueRegistry {
    queues: RwLock<HashMap<String, QueueHandle>>,
    max_depth: usize,
}

impl QueueRegistry {
    pub fn new(max_depth: usize) -> Self {
        QueueRegistry {
            queues: RwLock::new(HashMap::new()),
            max_depth,
        }
    }

    /// Normalize an AMQP address to a queue name. Accepts the RabbitMQ-4.x
    /// style `/queues/<name>` and bare names (with or without a leading `/`).
    pub fn queue_name(address: &str) -> Option<&str> {
        let name = address
            .strip_prefix("/queues/")
            .unwrap_or_else(|| address.trim_start_matches('/'));
        (!name.is_empty()).then_some(name)
    }

    /// Resolve an address, declaring the queue if it doesn't exist.
    pub fn resolve(&self, address: &str) -> Option<QueueHandle> {
        let name = Self::queue_name(address)?;
        if let Some(q) = self.queues.read().expect("registry lock").get(name) {
            return Some(q.clone());
        }
        let mut w = self.queues.write().expect("registry lock");
        // Double-checked: another connection may have declared it meanwhile.
        Some(
            w.entry(name.to_owned())
                .or_insert_with(|| queue::spawn(name.to_owned(), self.max_depth))
                .clone(),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn address_normalization() {
        assert_eq!(QueueRegistry::queue_name("/queues/orders"), Some("orders"));
        assert_eq!(QueueRegistry::queue_name("orders"), Some("orders"));
        assert_eq!(QueueRegistry::queue_name("/orders"), Some("orders"));
        assert_eq!(QueueRegistry::queue_name("/queues/"), None);
        assert_eq!(QueueRegistry::queue_name(""), None);
    }

    #[tokio::test]
    async fn resolve_is_idempotent() {
        let r = QueueRegistry::new(10);
        let a = r.resolve("/queues/q1").unwrap();
        let b = r.resolve("q1").unwrap();
        assert_eq!(a.name, b.name);
        // Same underlying actor: same channel.
        assert!(a.tx.same_channel(&b.tx));
    }
}
