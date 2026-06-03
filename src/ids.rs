//! Identifier newtypes (WP-0.2).
//!
//! These wrap the raw integer/string wire identifiers so the runtime cannot
//! confuse, say, a [`ChannelId`] with a [`Handle`] (a recurring footgun in
//! integer-typed AMQP APIs).

use bytes::Bytes;
use std::sync::atomic::{AtomicU64, Ordering};

/// The `container-id` identifying this client container to the peer.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ContainerId(String);

impl ContainerId {
    /// Wrap an existing id string.
    pub fn new(id: impl Into<String>) -> Self {
        ContainerId(id.into())
    }

    /// Generate a fresh random container id (`ramqp-<uuid>`).
    pub fn generate() -> Self {
        ContainerId(format!("ramqp-{}", uuid::Uuid::new_v4()))
    }

    /// The id as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for ContainerId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

impl From<&str> for ContainerId {
    fn from(s: &str) -> Self {
        ContainerId(s.to_owned())
    }
}

impl From<String> for ContainerId {
    fn from(s: String) -> Self {
        ContainerId(s)
    }
}

/// A connection channel number (the wire address of a session).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ChannelId(pub u16);

impl ChannelId {
    /// The raw channel number.
    pub fn value(self) -> u16 {
        self.0
    }
}

impl From<u16> for ChannelId {
    fn from(v: u16) -> Self {
        ChannelId(v)
    }
}

impl std::fmt::Display for ChannelId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ch:{}", self.0)
    }
}

/// A per-session link handle.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Handle(pub u32);

impl Handle {
    /// The raw handle value.
    pub fn value(self) -> u32 {
        self.0
    }
}

impl From<u32> for Handle {
    fn from(v: u32) -> Self {
        Handle(v)
    }
}

impl std::fmt::Display for Handle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "h:{}", self.0)
    }
}

/// A session-scoped delivery number.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct DeliveryId(pub u32);

impl DeliveryId {
    /// The raw delivery number.
    pub fn value(self) -> u32 {
        self.0
    }

    /// The next delivery number (wrapping, per AMQP serial arithmetic).
    pub fn next(self) -> DeliveryId {
        DeliveryId(self.0.wrapping_add(1))
    }
}

impl From<u32> for DeliveryId {
    fn from(v: u32) -> Self {
        DeliveryId(v)
    }
}

impl std::fmt::Display for DeliveryId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "d:{}", self.0)
    }
}

/// A sender-chosen delivery tag (≤ 32 octets).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DeliveryTag(pub Bytes);

impl DeliveryTag {
    /// Wrap raw tag bytes.
    pub fn new(bytes: impl Into<Bytes>) -> Self {
        DeliveryTag(bytes.into())
    }

    /// A delivery tag from a `u64` counter (8 big-endian octets).
    pub fn from_u64(n: u64) -> Self {
        DeliveryTag(Bytes::copy_from_slice(&n.to_be_bytes()))
    }

    /// The raw tag bytes.
    pub fn as_bytes(&self) -> &Bytes {
        &self.0
    }
}

/// A link name (unique within a connection for a given direction).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct LinkName(String);

impl LinkName {
    /// Wrap an existing link name.
    pub fn new(name: impl Into<String>) -> Self {
        LinkName(name.into())
    }

    /// Generate a fresh random link name.
    pub fn generate(prefix: &str) -> Self {
        LinkName(format!("{prefix}-{}", uuid::Uuid::new_v4()))
    }

    /// The name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Consume into the inner `String`.
    pub fn into_inner(self) -> String {
        self.0
    }
}

impl std::fmt::Display for LinkName {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// A process-local logical session identifier (stable across reconnects, unlike
/// the wire [`ChannelId`] which is reassigned on re-establishment).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SessionId(pub u64);

impl SessionId {
    /// Allocate a fresh, process-unique session id.
    pub fn next() -> Self {
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        SessionId(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// The raw id value.
    pub fn value(self) -> u64 {
        self.0
    }
}

impl std::fmt::Display for SessionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "s:{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn newtypes_are_distinct_and_display() {
        let c = ChannelId::from(3);
        let h = Handle::from(3);
        assert_eq!(c.value(), 3);
        assert_eq!(h.value(), 3);
        assert_eq!(c.to_string(), "ch:3");
        assert_eq!(h.to_string(), "h:3");
        assert_eq!(DeliveryId(5).next(), DeliveryId(6));
        assert_eq!(DeliveryId(u32::MAX).next(), DeliveryId(0));
    }

    #[test]
    fn session_ids_are_unique() {
        let a = SessionId::next();
        let b = SessionId::next();
        assert_ne!(a, b);
    }

    #[test]
    fn delivery_tag_from_u64() {
        assert_eq!(DeliveryTag::from_u64(1).as_bytes().len(), 8);
    }
}
