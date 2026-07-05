//! Public-API surface lock.
//!
//! The `ramqp-core` extraction preserves every pre-0.8 `ramqp::...` path via
//! re-exports; this test nails that promise down at compile time. If a
//! re-export is dropped, this file — not a downstream user — fails to build.
//!
//! Compile-time only: `use` declarations and type-position references. Items
//! behind cargo features sit behind matching `#[cfg]`s so both `cargo test`
//! and `cargo test --all-features` exercise what they build.

// ---- Root convenience re-exports (pre-0.8 surface) ----
#[allow(unused_imports)]
use ramqp::{
    Config, Connection, ConnectionBuilder, Consumer, Delivery, Message, Pool, PoolBuilder,
    Producer, Session, TlsConfig,
};

// The composite-codegen macro stays importable from the root.
#[allow(unused_imports)]
use ramqp::amqp_composite as _;

// ---- codec ----
#[allow(unused_imports)]
use ramqp::codec::{Decode, DecodeError, Encode, OrderedMap, Symbol, Value, from_slice, to_vec};

// ---- types ----
#[allow(unused_imports)]
use ramqp::types::{
    definitions::{ReceiverSettleMode, Role, SenderSettleMode},
    messaging::{Accepted, Coordinator, DeliveryState, Outcome, Source, Target, TargetArchetype},
    performatives::{Attach, Begin, Close, Disposition, End, Flow, Open, Performative, Transfer},
    sasl::{SaslCode, SaslFrame, SaslInit, SaslMechanisms, SaslOutcome, SaslResponse},
};

// ---- error ----
#[allow(unused_imports)]
use ramqp::error::{
    BoxError, ConnectError, ErrorKind, LinkError, RecvError, RemoteError, SendError, SessionError,
};

// ---- ids ----
#[allow(unused_imports)]
use ramqp::ids::{ChannelId, ContainerId, DeliveryId, DeliveryTag, Handle, LinkName, SessionId};

// ---- config ----
#[allow(unused_imports)]
use ramqp::config::{
    Config as FullConfig, ConnectionConfig, CreditMode, LinkConfig, ReconnectConfig, SessionConfig,
};

// ---- observe ----
#[allow(unused_imports)]
use ramqp::observe::{
    ConnectionEvent, ConnectionState, EventBus, Metrics, NoopMetrics, SharedMetrics, noop_metrics,
};

// ---- proto ----
#[allow(unused_imports)]
use ramqp::proto::{
    DriverCommand, IncomingDelivery, LinkAttached, LinkEvent, Reply, SessionEvent, SessionOpened,
};

// ---- transport ----
#[allow(unused_imports)]
use ramqp::transport::{
    Address, IoStream, Scheme, Transport, connect, connect_tcp,
    frame::{Frame, FrameBody, FramedTransport},
    header::ProtocolHeader,
};

// ---- connection (driver stays client; helpers re-exported from core) ----
#[allow(unused_imports)]
use ramqp::connection::{
    negotiate::{build_open, close_to_error, reconcile},
    {driver, heartbeat, mux},
};

// ---- session / link ----
#[allow(unused_imports)]
use ramqp::link::{Delivery as LinkDelivery, Link, credit, delivery, receiver, sender, settlement};
#[allow(unused_imports)]
use ramqp::session::{registry, state, window};

// ---- sasl ----
#[allow(unused_imports)]
use ramqp::sasl::{SaslProfile, negotiate as sasl_negotiate};

// ---- api / resilience ----
#[allow(unused_imports)]
use ramqp::resilience::{Backoff, connect_with_retry};

// ---- feature-gated surfaces ----
#[cfg(feature = "scram")]
#[allow(unused_imports)]
use ramqp::sasl::ScramMechanism;

#[cfg(feature = "transaction")]
#[allow(unused_imports)]
use ramqp::txn::{
    Declare, Declared, Discharge, TransactionController, TransactionalState, TxnId, capabilities,
    transactional_state,
};

/// A handful of items referenced in type position, so the imports above cannot
/// be optimized into "name exists but is a different kind of item".
#[test]
fn public_api_surface_compiles() {
    fn _takes_types(
        _c: Option<Config>,
        _m: Option<Message>,
        _a: Option<Address>,
        _e: Option<ConnectError>,
        _k: Option<ErrorKind>,
        _s: Option<Scheme>,
        _cm: Option<CreditMode>,
    ) {
    }
    let cfg = Config::default();
    assert!(cfg.session.incoming_window > 0);
    let parsed = Address::parse("amqp://localhost:5672/q").expect("parse");
    assert_eq!(parsed.scheme, Scheme::Amqp);
    assert_eq!(Scheme::Amqp.default_port(), 5672);
}
