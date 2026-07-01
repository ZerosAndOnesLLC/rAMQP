//! Session flow-control windows (WP-3.2).
//!
//! Tracks the outgoing/incoming transfer-id counters and both peers' windows so
//! the link layer can gate transfers and replenish credit via `flow`.

use crate::config::SessionConfig;
use crate::types::performatives::{Begin, Flow};

/// Per-session flow-control state.
#[derive(Debug, Clone, Copy)]
pub struct SessionWindows {
    /// The transfer-id we will assign to our next outgoing transfer.
    pub next_outgoing_id: u32,
    /// Our outgoing-window (advertised capacity to send).
    pub outgoing_window: u32,
    /// How many transfers we may still send before the peer's incoming-window
    /// is exhausted.
    pub remote_incoming_window: u32,
    /// Our incoming-window (how many more transfers we will accept).
    pub incoming_window: u32,
    /// The transfer-id we expect on the peer's next transfer.
    pub next_incoming_id: u32,
    /// The peer's advertised outgoing-window.
    pub remote_outgoing_window: u32,
    /// Whether the incoming side has been initialized by the peer's `begin`.
    initialized: bool,
}

impl SessionWindows {
    /// Initialize from session config (before the peer's `begin`).
    pub fn new(config: &SessionConfig) -> Self {
        SessionWindows {
            next_outgoing_id: 0,
            outgoing_window: config.outgoing_window,
            remote_incoming_window: 0,
            incoming_window: config.incoming_window,
            next_incoming_id: 0,
            remote_outgoing_window: 0,
            initialized: false,
        }
    }

    /// Apply the peer's `begin` (learn its windows + initial transfer-id).
    pub fn on_peer_begin(&mut self, begin: &Begin) {
        self.next_incoming_id = begin.next_outgoing_id;
        self.remote_incoming_window = begin.incoming_window;
        self.remote_outgoing_window = begin.outgoing_window;
        self.initialized = true;
    }

    /// Whether we may send another transfer right now.
    pub fn can_send(&self) -> bool {
        self.remote_incoming_window > 0
    }

    /// Record that we sent a transfer.
    pub fn record_outgoing(&mut self) {
        self.next_outgoing_id = self.next_outgoing_id.wrapping_add(1);
        self.remote_incoming_window = self.remote_incoming_window.saturating_sub(1);
    }

    /// Record that we received a transfer with `transfer_id`. Returns `true` if
    /// our incoming-window is now low enough to warrant replenishing via `flow`.
    pub fn record_incoming(&mut self, _transfer_id: u32) -> bool {
        self.next_incoming_id = self.next_incoming_id.wrapping_add(1);
        self.incoming_window = self.incoming_window.saturating_sub(1);
        self.incoming_window == 0
    }

    /// Reset our incoming-window to `window` (after sending a replenishing flow).
    pub fn replenish_incoming(&mut self, window: u32) {
        self.incoming_window = window;
    }

    /// Apply a peer `flow`, recomputing how much we may send.
    pub fn on_peer_flow(&mut self, flow: &Flow) {
        if let Some(ni) = flow.next_incoming_id {
            // remote-incoming-window = peer.next-incoming-id + peer.incoming-window
            //                          - our next-outgoing-id
            self.remote_incoming_window = ni
                .wrapping_add(flow.incoming_window)
                .wrapping_sub(self.next_outgoing_id);
        } else {
            // Peer has not received anything yet: window is just its incoming-window.
            self.remote_incoming_window = flow.incoming_window.wrapping_sub(self.next_outgoing_id);
        }
        self.next_incoming_id = self.next_incoming_id.max(flow.next_outgoing_id);
        self.remote_outgoing_window = flow.outgoing_window;
    }

    /// Build a `flow` performative advertising our current windows (link fields
    /// left unset; the caller fills them for a link-level flow).
    pub fn build_flow(&self) -> Flow {
        Flow {
            next_incoming_id: self.initialized.then_some(self.next_incoming_id),
            incoming_window: self.incoming_window,
            next_outgoing_id: self.next_outgoing_id,
            outgoing_window: self.outgoing_window,
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn windows() -> SessionWindows {
        SessionWindows::new(&SessionConfig {
            incoming_window: 10,
            outgoing_window: 10,
            handle_max: 100,
        })
    }

    #[test]
    fn outgoing_gated_by_peer_window() {
        let mut w = windows();
        assert!(!w.can_send()); // peer window unknown until begin

        let begin = Begin {
            next_outgoing_id: 5,
            incoming_window: 2,
            outgoing_window: 8,
            ..Default::default()
        };
        w.on_peer_begin(&begin);

        assert!(w.can_send());
        w.record_outgoing();
        assert_eq!(w.next_outgoing_id, 1);
        assert_eq!(w.remote_incoming_window, 1);
        w.record_outgoing();
        assert!(!w.can_send()); // exhausted
        assert_eq!(w.next_incoming_id, 5);
    }

    #[test]
    fn flow_reopens_window() {
        let mut w = windows();
        let begin = Begin {
            incoming_window: 0,
            ..Default::default()
        };
        w.on_peer_begin(&begin);
        assert!(!w.can_send());

        let flow = Flow {
            next_incoming_id: Some(0),
            incoming_window: 4,
            next_outgoing_id: 0,
            outgoing_window: 10,
            ..Default::default()
        };
        w.on_peer_flow(&flow);
        assert_eq!(w.remote_incoming_window, 4);
        assert!(w.can_send());
    }

    #[test]
    fn incoming_window_replenish() {
        let mut w = windows();
        let begin = Begin {
            incoming_window: 1,
            ..Default::default()
        };
        w.on_peer_begin(&begin);
        for _ in 0..9 {
            w.record_incoming(0);
        }
        assert_eq!(w.incoming_window, 1);
        assert!(w.record_incoming(0)); // hits zero → replenish
        w.replenish_incoming(10);
        assert_eq!(w.incoming_window, 10);
    }
}
