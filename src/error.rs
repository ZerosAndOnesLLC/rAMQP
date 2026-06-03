//! The flat, classified error model (WP-0.1, decision D-4).
//!
//! One error type per public operation surface ([`ConnectError`],
//! [`SendError`], [`RecvError`], [`SessionError`], [`LinkError`]), each sharing
//! an [`ErrorKind`] classifier with [`is_retryable`](ErrorKind::is_retryable) /
//! [`is_fatal`](ErrorKind::is_fatal), a real `source()` chain, and a typed
//! accessor for any peer-sent [`RemoteError`].

use crate::types::definitions::{Error as AmqpProtoError, ErrorCondition};

/// A boxed, thread-safe error usable in a `source()` chain.
pub type BoxError = Box<dyn std::error::Error + Send + Sync + 'static>;

/// The classification of an error, shared by every operation error type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// Underlying socket / IO failure.
    Io,
    /// TLS negotiation or session failure.
    Tls,
    /// SASL authentication failure.
    Sasl,
    /// The peer (or we) violated the protocol; the pipe is unusable.
    ProtocolViolation,
    /// The peer closed the connection / session / link.
    PeerClosed,
    /// An operation or the idle-timeout watchdog timed out.
    Timeout,
    /// The link was detached out from under the operation.
    Detached,
    /// The peer redirected the link/connection to another address.
    LinkRedirect,
    /// A bounded resource (window, credit, buffer, pool) was exhausted.
    Capacity,
    /// A settlement-state error (e.g. illegal delivery-state transition).
    Settlement,
    /// We failed to encode an outbound value.
    Encode,
    /// No live connection is available for the operation.
    NotConnected,
    /// The operation was cancelled (graceful shutdown / dropped handle).
    Cancelled,
}

impl ErrorKind {
    /// Whether an operation failing with this kind is worth retrying once the
    /// supervisor re-establishes the connection.
    pub fn is_retryable(self) -> bool {
        matches!(
            self,
            ErrorKind::Io
                | ErrorKind::Timeout
                | ErrorKind::PeerClosed
                | ErrorKind::Detached
                | ErrorKind::Capacity
                | ErrorKind::NotConnected
        )
    }

    /// Whether this kind renders the underlying connection unusable (a reconnect
    /// is required; the current transport cannot continue).
    pub fn is_fatal(self) -> bool {
        matches!(
            self,
            ErrorKind::ProtocolViolation | ErrorKind::Tls | ErrorKind::Sasl | ErrorKind::Encode
        )
    }

    fn label(self) -> &'static str {
        match self {
            ErrorKind::Io => "io",
            ErrorKind::Tls => "tls",
            ErrorKind::Sasl => "sasl",
            ErrorKind::ProtocolViolation => "protocol-violation",
            ErrorKind::PeerClosed => "peer-closed",
            ErrorKind::Timeout => "timeout",
            ErrorKind::Detached => "detached",
            ErrorKind::LinkRedirect => "link-redirect",
            ErrorKind::Capacity => "capacity",
            ErrorKind::Settlement => "settlement",
            ErrorKind::Encode => "encode",
            ErrorKind::NotConnected => "not-connected",
            ErrorKind::Cancelled => "cancelled",
        }
    }
}

/// A structured error the peer sent in a `close`/`end`/`detach`/`disposition`.
///
/// Never opaque: the underlying [`condition`](RemoteError::condition) and
/// [`description`](RemoteError::description) are always accessible.
#[derive(Debug, Clone, PartialEq)]
pub struct RemoteError(AmqpProtoError);

impl RemoteError {
    /// Wrap a peer-sent AMQP error.
    pub fn new(error: AmqpProtoError) -> Self {
        RemoteError(error)
    }

    /// The peer's error condition.
    pub fn condition(&self) -> &ErrorCondition {
        &self.0.condition
    }

    /// The peer's human-readable description, if any.
    pub fn description(&self) -> Option<&str> {
        self.0.description.as_deref()
    }

    /// The underlying protocol error.
    pub fn as_amqp(&self) -> &AmqpProtoError {
        &self.0
    }
}

impl std::fmt::Display for RemoteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl std::error::Error for RemoteError {}

macro_rules! op_error {
    ($(#[$m:meta])* $name:ident) => {
        $(#[$m])*
        pub struct $name {
            kind: ErrorKind,
            message: Option<String>,
            source: Option<BoxError>,
            remote: Option<RemoteError>,
        }

        impl $name {
            /// Construct a bare error of the given kind.
            pub fn new(kind: ErrorKind) -> Self {
                Self { kind, message: None, source: None, remote: None }
            }

            /// Construct an error of the given kind with a context message.
            pub fn msg(kind: ErrorKind, message: impl Into<String>) -> Self {
                Self { kind, message: Some(message.into()), source: None, remote: None }
            }

            /// The error classification.
            pub fn kind(&self) -> ErrorKind { self.kind }

            /// Whether retrying after reconnect may succeed.
            pub fn is_retryable(&self) -> bool { self.kind.is_retryable() }

            /// Whether the underlying connection is now unusable.
            pub fn is_fatal(&self) -> bool { self.kind.is_fatal() }

            /// The peer-sent error, if this failure originated remotely.
            pub fn remote(&self) -> Option<&RemoteError> { self.remote.as_ref() }

            /// Attach an underlying cause (builder).
            pub fn with_source(mut self, source: impl Into<BoxError>) -> Self {
                self.source = Some(source.into());
                self
            }

            /// Attach a peer-sent error (builder).
            pub fn with_remote(mut self, remote: RemoteError) -> Self {
                self.remote = Some(remote);
                self
            }

            /// Build from a peer-sent error, classified as the given kind.
            pub fn from_remote(kind: ErrorKind, remote: RemoteError) -> Self {
                Self { kind, message: None, source: None, remote: Some(remote) }
            }
        }

        impl std::fmt::Debug for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.debug_struct(stringify!($name))
                    .field("kind", &self.kind)
                    .field("message", &self.message)
                    .field("source", &self.source)
                    .field("remote", &self.remote)
                    .finish()
            }
        }

        impl std::fmt::Display for $name {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "{} [{}]", stringify!($name), self.kind.label())?;
                if let Some(m) = &self.message {
                    write!(f, ": {m}")?;
                }
                if let Some(r) = &self.remote {
                    write!(f, " (remote: {r})")?;
                }
                Ok(())
            }
        }

        impl std::error::Error for $name {
            fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                self.source.as_deref().map(|s| s as &(dyn std::error::Error + 'static))
            }
        }

        impl From<std::io::Error> for $name {
            fn from(e: std::io::Error) -> Self {
                Self::new(ErrorKind::Io).with_source(e)
            }
        }

        impl From<$crate::codec::DecodeError> for $name {
            fn from(e: $crate::codec::DecodeError) -> Self {
                Self::new(ErrorKind::ProtocolViolation).with_source(e)
            }
        }
    };
}

op_error! {
    /// Failure opening or maintaining a connection.
    ConnectError
}
op_error! {
    /// Failure beginning, using, or ending a session.
    SessionError
}
op_error! {
    /// Failure attaching, using, or detaching a link.
    LinkError
}
op_error! {
    /// Failure sending a message / awaiting its disposition.
    SendError
}
op_error! {
    /// Failure receiving or settling a delivery.
    RecvError
}

// Cross-surface conversions: a lower-layer failure can surface in a higher-layer
// operation while preserving its kind, message, and remote error.
macro_rules! convert_error {
    ($from:ident => $to:ident) => {
        impl From<$from> for $to {
            fn from(e: $from) -> Self {
                let mut out = $to::new(e.kind);
                out.message = e.message;
                out.source = e.source;
                out.remote = e.remote;
                out
            }
        }
    };
}

convert_error!(ConnectError => SessionError);
convert_error!(ConnectError => LinkError);
convert_error!(ConnectError => SendError);
convert_error!(ConnectError => RecvError);
convert_error!(SessionError => LinkError);
convert_error!(SessionError => SendError);
convert_error!(SessionError => RecvError);
convert_error!(LinkError => SendError);
convert_error!(LinkError => RecvError);

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::definitions::{AmqpError, ConnectionError};

    #[test]
    fn classification() {
        assert!(ErrorKind::Io.is_retryable());
        assert!(ErrorKind::Timeout.is_retryable());
        assert!(!ErrorKind::Sasl.is_retryable());
        assert!(ErrorKind::ProtocolViolation.is_fatal());
        assert!(!ErrorKind::PeerClosed.is_fatal());
    }

    #[test]
    fn source_chain_is_real() {
        let io = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let e = ConnectError::from(io);
        assert_eq!(e.kind(), ErrorKind::Io);
        assert!(e.is_retryable());
        // the io::Error is reachable via source()
        let src = std::error::Error::source(&e).expect("source present");
        assert!(src.to_string().contains("reset"));
    }

    #[test]
    fn remote_error_is_typed_not_opaque() {
        let proto = AmqpProtoError::new(ConnectionError::ConnectionForced, Some("bye".into()));
        let e = ConnectError::from_remote(ErrorKind::PeerClosed, RemoteError::new(proto));
        let r = e.remote().expect("remote present");
        assert_eq!(
            r.condition(),
            &ErrorCondition::Connection(ConnectionError::ConnectionForced)
        );
        assert_eq!(r.description(), Some("bye"));
    }

    #[test]
    fn cross_surface_conversion_preserves_classification() {
        let proto = AmqpProtoError::new(AmqpError::ResourceLimitExceeded, None);
        let link = LinkError::from_remote(ErrorKind::Capacity, RemoteError::new(proto));
        let send: SendError = link.into();
        assert_eq!(send.kind(), ErrorKind::Capacity);
        assert!(send.is_retryable());
        assert!(send.remote().is_some());
    }
}
