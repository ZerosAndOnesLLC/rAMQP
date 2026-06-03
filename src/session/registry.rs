//! Link handle allocation and routing within a session (WP-3.3).

use std::collections::HashMap;

/// Allocates link handles up to the session's `handle-max`, reusing freed
/// handles (lowest-free policy).
#[derive(Debug)]
pub struct HandleAllocator {
    handle_max: u32,
    next: u64,
    free: Vec<u32>,
}

impl HandleAllocator {
    /// Create an allocator bounded by `handle_max` (inclusive).
    pub fn new(handle_max: u32) -> Self {
        HandleAllocator {
            handle_max,
            next: 0,
            free: Vec::new(),
        }
    }

    /// Allocate the lowest available handle, or `None` if exhausted.
    pub fn allocate(&mut self) -> Option<u32> {
        if let Some(h) = self.free.pop() {
            return Some(h);
        }
        if self.next <= self.handle_max as u64 {
            let h = self.next as u32;
            self.next += 1;
            Some(h)
        } else {
            None
        }
    }

    /// Return a handle to the free pool.
    pub fn release(&mut self, handle: u32) {
        self.free.push(handle);
    }
}

/// Maps the peer's link handle (seen on inbound attach/transfer/detach) to our
/// local handle for the same link.
#[derive(Debug, Default)]
pub struct RemoteHandleMap {
    map: HashMap<u32, u32>,
}

impl RemoteHandleMap {
    /// Record that the peer's `remote` handle corresponds to our `local`.
    pub fn bind(&mut self, remote: u32, local: u32) {
        self.map.insert(remote, local);
    }

    /// Resolve an inbound handle to our local handle.
    pub fn resolve(&self, remote: u32) -> Option<u32> {
        self.map.get(&remote).copied()
    }

    /// Forget a binding (on detach).
    pub fn unbind(&mut self, remote: u32) {
        self.map.remove(&remote);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allocates_and_releases() {
        let mut a = HandleAllocator::new(1);
        assert_eq!(a.allocate(), Some(0));
        assert_eq!(a.allocate(), Some(1));
        assert_eq!(a.allocate(), None);
        a.release(0);
        assert_eq!(a.allocate(), Some(0));
    }

    #[test]
    fn remote_handle_routing() {
        let mut m = RemoteHandleMap::default();
        m.bind(9, 0);
        assert_eq!(m.resolve(9), Some(0));
        m.unbind(9);
        assert_eq!(m.resolve(9), None);
    }
}
