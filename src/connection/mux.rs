//! Channel multiplexing (WP-2.3): allocation of outgoing channel numbers and
//! the incoming-channel → session routing map.

use std::collections::HashMap;

/// Allocates outgoing channel numbers up to the negotiated `channel-max`,
/// reusing freed numbers (lowest-free policy).
#[derive(Debug)]
pub struct ChannelAllocator {
    channel_max: u16,
    next: u32,
    free: Vec<u16>,
}

impl ChannelAllocator {
    /// Create an allocator bounded by `channel_max` (inclusive).
    pub fn new(channel_max: u16) -> Self {
        ChannelAllocator {
            channel_max,
            next: 0,
            free: Vec::new(),
        }
    }

    /// Allocate the lowest available channel, or `None` if exhausted.
    pub fn allocate(&mut self) -> Option<u16> {
        if let Some(ch) = self.free.pop() {
            return Some(ch);
        }
        if self.next <= self.channel_max as u32 {
            let ch = self.next as u16;
            self.next += 1;
            Some(ch)
        } else {
            None
        }
    }

    /// Return a channel to the free pool.
    pub fn release(&mut self, channel: u16) {
        self.free.push(channel);
    }

    /// Number of channels currently in use.
    pub fn in_use(&self) -> usize {
        (self.next as usize).saturating_sub(self.free.len())
    }
}

/// Maps a peer's outgoing channel (which we see on inbound frames) to our local
/// channel for the same session.
#[derive(Debug, Default)]
pub struct RemoteChannelMap {
    map: HashMap<u16, u16>,
}

impl RemoteChannelMap {
    /// Record that the peer's `remote` channel corresponds to our `local`.
    pub fn bind(&mut self, remote: u16, local: u16) {
        self.map.insert(remote, local);
    }

    /// Resolve an inbound channel to our local channel.
    pub fn resolve(&self, remote: u16) -> Option<u16> {
        self.map.get(&remote).copied()
    }

    /// Forget a binding (on session end).
    pub fn unbind(&mut self, remote: u16) {
        self.map.remove(&remote);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_and_reuses_lowest() {
        let mut a = ChannelAllocator::new(2);
        assert_eq!(a.allocate(), Some(0));
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.allocate(), Some(2));
        assert_eq!(a.allocate(), None); // exhausted (channel_max inclusive)
        a.release(1);
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.in_use(), 3);
    }

    #[test]
    fn remote_channel_routing() {
        let mut m = RemoteChannelMap::default();
        m.bind(7, 0);
        assert_eq!(m.resolve(7), Some(0));
        m.unbind(7);
        assert_eq!(m.resolve(7), None);
    }
}
