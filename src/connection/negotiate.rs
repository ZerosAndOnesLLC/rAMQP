//! Connection open/close negotiation (WP-2.2).
//!
//! Builds our `open`, reconciles it with the peer's using the min-of-both rules,
//! and maps a peer `close{error}` into the flat error model.

use std::time::Duration;

use crate::config::ConnectionConfig;
use crate::error::{ConnectError, ErrorKind, RemoteError};
use crate::types::performatives::{Close, Open};

/// The minimum `max-frame-size` permitted by the spec (512 octets).
pub const MIN_MAX_FRAME_SIZE: u32 = 512;

/// Parameters agreed during `open`.
#[derive(Debug, Clone, Copy)]
pub struct Negotiated {
    /// The effective max frame size (min of both peers' advertised sizes).
    pub max_frame_size: u32,
    /// The effective channel max (min of both peers).
    pub channel_max: u16,
    /// How often *we* must emit a frame to satisfy the peer's idle timeout
    /// (half the peer's advertised idle-timeout). `None` if the peer disabled it.
    pub send_interval: Option<Duration>,
    /// How long we wait for *any* frame before declaring the peer idle (our own
    /// advertised idle-timeout). `None` if we disabled it.
    pub recv_timeout: Option<Duration>,
}

/// Build our `open` performative from connection config.
pub fn build_open(config: &ConnectionConfig) -> Open {
    Open {
        container_id: config.container_id.as_str().to_owned(),
        hostname: config.hostname.clone(),
        max_frame_size: config.max_frame_size,
        channel_max: config.channel_max,
        idle_time_out: config
            .idle_timeout
            .map(|d| d.as_millis().min(u32::MAX as u128) as u32)
            .filter(|ms| *ms > 0),
        ..Default::default()
    }
}

/// Reconcile our `open` with the peer's into the [`Negotiated`] parameters.
pub fn reconcile(local: &Open, remote: &Open) -> Negotiated {
    let max_frame_size = local
        .max_frame_size
        .min(remote.max_frame_size)
        .max(MIN_MAX_FRAME_SIZE);
    let channel_max = local.channel_max.min(remote.channel_max);

    // The peer's idle-timeout obliges us to send within half of it.
    let send_interval = remote
        .idle_time_out
        .filter(|ms| *ms > 0)
        .map(|ms| Duration::from_millis((ms / 2).max(1) as u64));
    // Our advertised idle-timeout is what we enforce against the peer.
    let recv_timeout = local
        .idle_time_out
        .filter(|ms| *ms > 0)
        .map(|ms| Duration::from_millis(ms as u64));

    Negotiated {
        max_frame_size,
        channel_max,
        send_interval,
        recv_timeout,
    }
}

/// Map a peer `close` into a connection error (or `None` for a graceful close).
pub fn close_to_error(close: &Close) -> Option<ConnectError> {
    close
        .error
        .as_ref()
        .map(|e| ConnectError::from_remote(ErrorKind::PeerClosed, RemoteError::new(e.clone())))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::ContainerId;

    #[test]
    fn reconciles_min_of_both() {
        let mut local = Open::new("us");
        local.max_frame_size = 131_072;
        local.channel_max = 1024;
        local.idle_time_out = Some(60_000);

        let mut remote = Open::new("them");
        remote.max_frame_size = 65_536;
        remote.channel_max = 256;
        remote.idle_time_out = Some(30_000);

        let n = reconcile(&local, &remote);
        assert_eq!(n.max_frame_size, 65_536);
        assert_eq!(n.channel_max, 256);
        assert_eq!(n.send_interval, Some(Duration::from_millis(15_000)));
        assert_eq!(n.recv_timeout, Some(Duration::from_millis(60_000)));
    }

    #[test]
    fn enforces_min_frame_size_floor() {
        let mut local = Open::new("us");
        local.max_frame_size = 256;
        let mut remote = Open::new("them");
        remote.max_frame_size = 256;
        assert_eq!(
            reconcile(&local, &remote).max_frame_size,
            MIN_MAX_FRAME_SIZE
        );
    }

    #[test]
    fn build_open_carries_config() {
        let cfg = ConnectionConfig {
            container_id: ContainerId::new("my-id"),
            max_frame_size: 65_536,
            ..Default::default()
        };
        let open = build_open(&cfg);
        assert_eq!(open.container_id, "my-id");
        assert_eq!(open.max_frame_size, 65_536);
        assert!(open.idle_time_out.is_some());
    }
}
