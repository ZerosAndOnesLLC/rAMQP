//! Connection lifecycle events and their broadcast bus (WP-0.4).
//!
//! The [`EventBus`] lets applications observe connection health (including
//! reconnect transitions) without enabling `tracing` or any log backend.

use tokio::sync::broadcast;

/// A coarse, cloneable snapshot of connection health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionState {
    /// The initial connect / handshake is in progress.
    Connecting,
    /// Open and healthy.
    Connected,
    /// Connection lost; the supervisor is attempting to re-establish.
    Reconnecting,
    /// Connected but degraded (e.g. operating from a bounded outbound buffer).
    Degraded,
    /// Permanently closed.
    Closed,
}

/// A connection lifecycle event published on the [`EventBus`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum ConnectionEvent {
    /// The initial connection / handshake started.
    Connecting,
    /// The connection opened successfully (or re-opened after a drop).
    Connected,
    /// A reconnect attempt is starting (1-based `attempt`).
    Reconnecting {
        /// The attempt number.
        attempt: u32,
    },
    /// The connection is degraded but still usable.
    Degraded {
        /// A short, human-readable reason.
        reason: String,
    },
    /// The connection closed.
    Closed {
        /// Whether the close was caused by an error (vs. a graceful close).
        error: bool,
        /// A short, human-readable reason.
        reason: String,
    },
}

impl ConnectionEvent {
    /// The coarse [`ConnectionState`] this event implies.
    pub fn state(&self) -> ConnectionState {
        match self {
            ConnectionEvent::Connecting => ConnectionState::Connecting,
            ConnectionEvent::Connected => ConnectionState::Connected,
            ConnectionEvent::Reconnecting { .. } => ConnectionState::Reconnecting,
            ConnectionEvent::Degraded { .. } => ConnectionState::Degraded,
            ConnectionEvent::Closed { .. } => ConnectionState::Closed,
        }
    }
}

/// A multi-producer/multi-consumer broadcast of [`ConnectionEvent`]s.
///
/// Subscribers that fall behind by more than the bus capacity observe a lag
/// notification (standard `broadcast` semantics) rather than blocking the
/// runtime — health events are lossy by design.
#[derive(Debug, Clone)]
pub struct EventBus {
    tx: broadcast::Sender<ConnectionEvent>,
}

impl EventBus {
    /// Create a bus buffering up to `capacity` recent events per subscriber.
    pub fn new(capacity: usize) -> Self {
        let (tx, _rx) = broadcast::channel(capacity);
        EventBus { tx }
    }

    /// Publish an event (ignored if there are no subscribers).
    pub fn publish(&self, event: ConnectionEvent) {
        let _ = self.tx.send(event);
    }

    /// Subscribe to subsequent events.
    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.tx.subscribe()
    }

    /// The current number of subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

impl Default for EventBus {
    fn default() -> Self {
        EventBus::new(64)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn subscribers_receive_lifecycle_sequence() {
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();
        bus.publish(ConnectionEvent::Connecting);
        bus.publish(ConnectionEvent::Connected);
        bus.publish(ConnectionEvent::Reconnecting { attempt: 1 });
        bus.publish(ConnectionEvent::Connected);

        assert_eq!(rx.recv().await.unwrap(), ConnectionEvent::Connecting);
        assert_eq!(rx.recv().await.unwrap(), ConnectionEvent::Connected);
        assert_eq!(
            rx.recv().await.unwrap().state(),
            ConnectionState::Reconnecting
        );
        assert_eq!(rx.recv().await.unwrap(), ConnectionEvent::Connected);
    }
}
