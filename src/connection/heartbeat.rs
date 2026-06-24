//! Heartbeat / idle-timeout handling (WP-2.4).
//!
//! Drives the keepalive: emits empty frames often enough to satisfy the peer's
//! advertised idle-timeout, and flags the peer as timed-out when no frame has
//! arrived within our own advertised idle-timeout.

use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

/// What the driver should do on a heartbeat tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeartbeatAction {
    /// Nothing due yet.
    Idle,
    /// Emit an empty (keepalive) frame.
    SendEmpty,
    /// The peer exceeded our idle-timeout — treat as a reconnectable failure.
    PeerTimedOut,
}

/// Tracks send/receive activity and produces [`HeartbeatAction`]s.
#[derive(Debug)]
pub struct Heartbeat {
    timer: Option<Interval>,
    send_after: Option<Duration>,
    recv_timeout: Option<Duration>,
    last_recv: Instant,
    last_send: Instant,
    // Activity is flagged with a cheap bool per frame and folded into `last_recv`
    // / `last_send` once per (infrequent) timer tick — so the per-message hot
    // path never calls `Instant::now()` (a `clock_gettime` syscall/vDSO call).
    recv_since_tick: bool,
    send_since_tick: bool,
}

impl Heartbeat {
    /// Build from the negotiated `send_after` (how often we must emit) and
    /// `recv_timeout` (how long before the peer is considered idle). The check
    /// timer fires at the finer of the two cadences.
    pub fn new(send_after: Option<Duration>, recv_timeout: Option<Duration>) -> Self {
        let check = match (send_after, recv_timeout) {
            (Some(s), Some(r)) => Some(s.min(r / 2)),
            (Some(s), None) => Some(s),
            (None, Some(r)) => Some(r / 2),
            (None, None) => None,
        }
        .map(|d| d.max(Duration::from_millis(1)));

        let now = Instant::now();
        let timer = check.map(|d| {
            // Delay the first tick by one period (tokio's `interval` fires the
            // first tick immediately, which we don't want for a keepalive).
            let mut iv = interval_at(now + d, d);
            iv.set_missed_tick_behavior(MissedTickBehavior::Delay);
            iv
        });

        Heartbeat {
            timer,
            send_after,
            recv_timeout,
            last_recv: now,
            last_send: now,
            recv_since_tick: false,
            send_since_tick: false,
        }
    }

    /// Record that a frame was received from the peer (hot path: no clock call).
    pub fn record_recv(&mut self) {
        self.recv_since_tick = true;
    }

    /// Record that a frame was sent to the peer (hot path: no clock call).
    pub fn record_send(&mut self) {
        self.send_since_tick = true;
    }

    /// Await the next heartbeat tick and decide what to do. Cancel-safe.
    ///
    /// When no timers are configured this future is pending forever, so it is
    /// inert inside a `select!`.
    pub async fn tick(&mut self) -> HeartbeatAction {
        match &mut self.timer {
            Some(iv) => {
                iv.tick().await;
                // Fold the per-frame activity flags into the timestamps with a
                // single clock read, then evaluate the timeouts against `now`.
                let now = Instant::now();
                if std::mem::take(&mut self.recv_since_tick) {
                    self.last_recv = now;
                }
                if std::mem::take(&mut self.send_since_tick) {
                    self.last_send = now;
                }
                if let Some(to) = self.recv_timeout {
                    if now.saturating_duration_since(self.last_recv) >= to {
                        return HeartbeatAction::PeerTimedOut;
                    }
                }
                if let Some(sa) = self.send_after {
                    if now.saturating_duration_since(self.last_send) >= sa {
                        return HeartbeatAction::SendEmpty;
                    }
                }
                HeartbeatAction::Idle
            }
            None => std::future::pending().await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn sends_keepalive_when_idle() {
        let mut hb = Heartbeat::new(Some(Duration::from_millis(100)), None);
        // After the send interval with no sends, a keepalive is due.
        assert_eq!(hb.tick().await, HeartbeatAction::SendEmpty);
    }

    #[tokio::test(start_paused = true)]
    async fn detects_peer_timeout() {
        let mut hb = Heartbeat::new(None, Some(Duration::from_millis(100)));
        // No recv recorded; after recv_timeout the peer is declared idle.
        // The check timer fires at recv_timeout/2, so two ticks cover the window.
        let mut action = hb.tick().await;
        if action == HeartbeatAction::Idle {
            action = hb.tick().await;
        }
        assert_eq!(action, HeartbeatAction::PeerTimedOut);
    }
}
