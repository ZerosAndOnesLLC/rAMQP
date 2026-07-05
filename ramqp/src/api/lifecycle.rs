//! Graceful lifecycle helpers (WP-5.6).
//!
//! Dropping a `Producer`/`Consumer` fires a best-effort, non-blocking detach so
//! links are torn down cleanly even without an explicit `detach().await` (fixing
//! `fe2o3-amqp`'s drop-doesn't-detach wart).

use tokio::sync::{mpsc, oneshot};

use crate::ids::{ChannelId, Handle};
use crate::proto::DriverCommand;

/// Fire a non-blocking `DetachLink` (closed) on drop. Best-effort: if the
/// command channel is full or closed, the link is torn down when the connection
/// closes instead.
pub(crate) fn detach_on_drop(
    commands: &mpsc::Sender<DriverCommand>,
    channel: ChannelId,
    handle: Handle,
) {
    let (reply, _rx) = oneshot::channel();
    let _ = commands.try_send(DriverCommand::DetachLink {
        channel,
        handle,
        closed: true,
        error: None,
        reply,
    });
}
