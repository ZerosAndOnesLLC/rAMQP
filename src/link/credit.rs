//! Link credit / flow control (WP-4.4).
//!
//! Credit is a first-class, runtime-tunable value (fixing `fe2o3-amqp`'s
//! attach-time-only credit). The same type serves both roles: a sender consumes
//! credit as it sends; a receiver grants it and auto-refills.

use crate::config::CreditMode;

/// Link-level credit/flow state.
#[derive(Debug, Clone, Copy)]
pub struct LinkCredit {
    /// The link delivery-count (serial; wraps).
    pub delivery_count: u32,
    /// Current link credit.
    pub link_credit: u32,
    /// Messages available at the sender but not yet sent (sender hint).
    pub available: u32,
    /// Whether a drain is in progress.
    pub drain: bool,
    mode: CreditMode,
}

impl LinkCredit {
    /// Create credit state with an initial delivery-count.
    pub fn new(initial_delivery_count: u32, mode: CreditMode) -> Self {
        LinkCredit {
            delivery_count: initial_delivery_count,
            link_credit: 0,
            available: 0,
            drain: false,
            mode,
        }
    }

    // ---- sender side ----

    /// Whether the sender has credit to send.
    pub fn can_send(&self) -> bool {
        self.link_credit > 0
    }

    /// Record that the sender sent one transfer.
    pub fn record_sent(&mut self) {
        self.delivery_count = self.delivery_count.wrapping_add(1);
        self.link_credit = self.link_credit.saturating_sub(1);
    }

    /// Apply a peer `flow` to a *sender*: the receiver dictates our credit as
    /// `flow.delivery-count + flow.link-credit - our delivery-count`.
    pub fn apply_flow_as_sender(
        &mut self,
        flow_delivery_count: Option<u32>,
        flow_link_credit: Option<u32>,
        drain: bool,
    ) {
        let peer_dc = flow_delivery_count.unwrap_or(self.delivery_count);
        if let Some(lc) = flow_link_credit {
            self.link_credit = peer_dc.wrapping_add(lc).wrapping_sub(self.delivery_count);
        }
        self.drain = drain;
    }

    // ---- receiver side ----

    /// Grant `credit` additional units (receiver).
    pub fn grant(&mut self, credit: u32) {
        self.link_credit = self.link_credit.saturating_add(credit);
        self.drain = false;
    }

    /// Set absolute credit to `credit` (receiver).
    pub fn set_credit(&mut self, credit: u32) {
        self.link_credit = credit;
        self.drain = false;
    }

    /// Record that the receiver consumed one transfer.
    pub fn record_received(&mut self) {
        self.delivery_count = self.delivery_count.wrapping_add(1);
        self.link_credit = self.link_credit.saturating_sub(1);
    }

    /// For auto credit mode: the amount to top back up to `initial` when credit
    /// has fallen to the refill threshold. `None` for manual mode or when not yet
    /// due.
    pub fn auto_refill(&self) -> Option<u32> {
        match self.mode {
            CreditMode::Auto {
                initial,
                refill_threshold,
            } if self.link_credit <= refill_threshold => {
                Some(initial.saturating_sub(self.link_credit))
            }
            _ => None,
        }
    }

    /// The credit mode in effect.
    pub fn mode(&self) -> CreditMode {
        self.mode
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sender_consumes_credit() {
        let mut c = LinkCredit::new(0, CreditMode::Manual);
        assert!(!c.can_send());
        // receiver granted 2 credit at delivery-count 0
        c.apply_flow_as_sender(Some(0), Some(2), false);
        assert_eq!(c.link_credit, 2);
        c.record_sent();
        assert_eq!(c.delivery_count, 1);
        assert_eq!(c.link_credit, 1);
        c.record_sent();
        assert!(!c.can_send());
    }

    #[test]
    fn receiver_auto_refill() {
        let mode = CreditMode::Auto {
            initial: 100,
            refill_threshold: 50,
        };
        let mut c = LinkCredit::new(0, mode);
        c.set_credit(100);
        assert_eq!(c.auto_refill(), None);
        for _ in 0..50 {
            c.record_received();
        }
        // at 50 == threshold → refill by 50 back to 100
        assert_eq!(c.auto_refill(), Some(50));
    }
}
