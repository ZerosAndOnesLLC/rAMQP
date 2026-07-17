//! Unified AMQP 1.0 conformance matrix for the broker.
//!
//! One place that pins the broker's *wire-level* obedience to the spec across
//! four axes — **framing**, **error conditions**, **flow/credit**, and
//! **settlement** — against a loopback instance, no external broker involved.
//! It complements (does not duplicate) the behavioral suites: `produce_consume`
//! / `quorum_queue` prove queue semantics, `adversarial`'s raw-socket cases are
//! folded in here, and the SASL/SCRAM RFC vectors live in `ramqp-core`.
//!
//! The distinguishing value over "does a message round-trip" tests is that the
//! error-condition axis asserts the *exact* `amqp:*` condition symbol the
//! broker returns, and the flow axis asserts the credit ceiling at the
//! protocol level.

mod harness;

use harness::*;

// ---------------------------------------------------------------------------
// Framing: the byte/frame-level rules independent of any queue.
// ---------------------------------------------------------------------------
mod framing {
    use super::*;
    use bytes::BytesMut;
    use tokio::io::AsyncWriteExt;

    use ramqp_core::transport::frame::FrameBody;
    use ramqp_core::transport::header::ProtocolHeader;
    use ramqp_core::types::performatives::{Begin, Close, Open, Performative};

    /// The baseline handshake: bare-AMQP header, `open`/`open`, then a graceful
    /// `close` is echoed with no error.
    #[tokio::test]
    async fn header_open_and_graceful_close() {
        let lb = loopback().await;
        let mut peer = RawPeer::open(lb.addr, "conformance", 65536).await;

        peer.send(0, Performative::Close(Close { error: None }))
            .await;
        match peer.wait_for_close().await {
            CloseOutcome::Clean | CloseOutcome::Dropped => {}
            CloseOutcome::Error(e) => panic!("graceful close drew an error: {}", e.condition),
        }
    }

    /// max-frame-size is directional (spec §2.7.1): a peer may advertise a small
    /// *receive* limit yet legally send frames as large as the BROKER
    /// advertised. The broker's inbound decode must honor its own advertised
    /// max, not the negotiated min — otherwise it kills a spec-legal frame.
    #[tokio::test]
    async fn oversized_inbound_frame_from_small_advertiser_is_accepted() {
        // Broker advertises the default (128 KiB); the raw client advertises 4 KiB.
        let lb = loopback().await;
        let mut stream = tokio::net::TcpStream::connect(lb.addr)
            .await
            .expect("connect");
        ProtocolHeader::AMQP
            .negotiate(&mut stream)
            .await
            .expect("header");

        let mut small_open = Open::new("small-advertiser");
        small_open.max_frame_size = 4096;
        stream
            .write_all(&encode_frame(0, &Performative::Open(small_open)).await)
            .await
            .expect("send open");

        let mut buf = BytesMut::new();
        loop {
            match read_raw_frame(&mut stream, &mut buf).await.body {
                FrameBody::Amqp(Performative::Open(_), _) => break,
                FrameBody::Empty => continue,
                other => panic!("expected broker open, got {other:?}"),
            }
        }

        // Build an 8 KiB Begin frame (> the client's advertised 4 KiB, <= the
        // broker's 128 KiB): pad the encoded Begin and fix its size header. The
        // padding decodes as ignored trailing payload.
        let begin = Begin {
            next_outgoing_id: 0,
            incoming_window: 8,
            outgoing_window: 8,
            handle_max: 16,
            ..Default::default()
        };
        let mut big = encode_frame(0, &Performative::Begin(begin)).await;
        assert!(big.len() < 8192);
        big.resize(8192, 0);
        big[0..4].copy_from_slice(&(8192u32).to_be_bytes());
        stream.write_all(&big).await.expect("send big begin");

        // The broker must ACCEPT the oversized frame — a Begin response, not a
        // close{error} from an over-strict inbound limit.
        match read_raw_frame(&mut stream, &mut buf).await.body {
            FrameBody::Amqp(Performative::Begin(_), _) => {}
            FrameBody::Amqp(Performative::Close(c), _) => {
                panic!(
                    "broker rejected a spec-legal oversized frame: {:?}",
                    c.error
                )
            }
            other => panic!("expected a begin response, got {other:?}"),
        }
    }
}

// ---------------------------------------------------------------------------
// Error conditions: which protocol violation earns which `amqp:*` condition.
// This axis asserts the *exact* symbol, not just that an error was present.
// ---------------------------------------------------------------------------
mod error_conditions {
    use super::*;
    use tokio::io::AsyncReadExt;

    use ramqp_broker::{Broker, BrokerConfig};
    use ramqp_core::config::ConnectionConfig;
    use ramqp_core::types::performatives::{Detach, Open, Performative};

    /// A duplicate `open` on an already-open connection is a connection-level
    /// framing error — answered with `close{error}`, never a silent reset.
    #[tokio::test]
    async fn duplicate_open() {
        let lb = loopback().await;
        let mut peer = RawPeer::open(lb.addr, "dup", 65536).await;

        peer.send(0, Performative::Open(Open::new("dup-again")))
            .await;

        match peer.wait_for_close().await {
            CloseOutcome::Error(e) => {
                assert_eq!(e.condition.as_str(), "amqp:connection:framing-error")
            }
            other => panic!("expected close with error, got {other:?}"),
        }
    }

    /// A link frame on a channel that was never begun is a hard framing error
    /// (unlike `end`, which tolerates an end/end race).
    #[tokio::test]
    async fn frame_on_unmapped_channel() {
        let lb = loopback().await;
        let mut peer = RawPeer::open(lb.addr, "unmapped", 65536).await;

        peer.send(
            7,
            Performative::Detach(Detach {
                handle: 0,
                closed: true,
                error: None,
            }),
        )
        .await;

        match peer.wait_for_close().await {
            CloseOutcome::Error(e) => {
                assert_eq!(e.condition.as_str(), "amqp:connection:framing-error")
            }
            other => panic!("expected close with error, got {other:?}"),
        }
    }

    /// Slow-loris guard: a peer that connects then sends nothing is dropped once
    /// the inbound-handshake timeout fires, observed as EOF on our end.
    #[tokio::test]
    async fn stalled_handshake_is_timed_out() {
        let mut config = BrokerConfig::default();
        config.connection = ConnectionConfig {
            connect_timeout: Some(std::time::Duration::from_millis(200)),
            ..Default::default()
        };
        let lb = loopback_with(Broker::new(config)).await;

        // Connect but never send the protocol header (the slow-loris).
        let mut stream = tokio::net::TcpStream::connect(lb.addr)
            .await
            .expect("connect");
        let mut buf = [0u8; 1];
        let observed =
            tokio::time::timeout(std::time::Duration::from_secs(2), stream.read(&mut buf))
                .await
                .expect("broker must drop the stalled handshake well before 2s");
        assert!(
            matches!(observed, Ok(0)),
            "expected EOF from the timed-out handshake, got {observed:?}"
        );
    }
}

// ---------------------------------------------------------------------------
// Flow: the broker must never put more deliveries in flight than granted
// link-credit (spec §2.6.7), driven through the client's manual-credit mode.
// ---------------------------------------------------------------------------
mod flow {
    use super::*;
    use ramqp::Message;
    use ramqp::config::CreditMode;

    #[tokio::test]
    async fn broker_never_exceeds_granted_credit() {
        let lb = loopback().await;
        let conn = connect(&lb.url()).await;
        let session = conn.begin_session().await.expect("session");

        let producer = session
            .create_producer("/queues/flow-credit")
            .await
            .expect("producer");
        for i in 0..5 {
            producer
                .send(Message::text(format!("m{i}")))
                .await
                .expect("send");
        }

        // Manual credit: no automatic window, we grant explicitly.
        let mut consumer = session
            .create_consumer_with("/queues/flow-credit", CreditMode::Manual)
            .await
            .expect("consumer");

        // Grant exactly 2 against a queue of 5.
        consumer.credit(2).await.expect("grant credit");
        let d0 = consumer.recv().await.expect("first");
        let d1 = consumer.recv().await.expect("second");
        consumer.accept(&d0).await.expect("accept 0");
        consumer.accept(&d1).await.expect("accept 1");

        // A 3rd delivery must NOT arrive without more credit.
        let third =
            tokio::time::timeout(std::time::Duration::from_millis(300), consumer.recv()).await;
        assert!(
            third.is_err(),
            "broker sent a 3rd delivery beyond the granted credit of 2"
        );

        // Grant the rest; the remaining 3 then flow.
        consumer.credit(3).await.expect("grant more");
        for _ in 0..3 {
            let d = tokio::time::timeout(std::time::Duration::from_secs(2), consumer.recv())
                .await
                .expect("delivery after top-up")
                .expect("recv");
            consumer.accept(&d).await.expect("accept");
        }

        conn.close().await.expect("close");
    }
}

// ---------------------------------------------------------------------------
// Settlement: the terminal `accepted` outcome removes a message — the
// complement of produce_consume's "unacked is requeued" cases.
// ---------------------------------------------------------------------------
mod settlement {
    use super::*;
    use ramqp::Message;

    #[tokio::test]
    async fn accepted_delivery_is_not_redelivered() {
        let lb = loopback().await;
        let conn = connect(&lb.url()).await;
        let session = conn.begin_session().await.expect("session");

        let producer = session
            .create_producer("/queues/settle-accept")
            .await
            .expect("producer");
        producer
            .send(Message::text("only-once"))
            .await
            .expect("send");

        // First consumer receives and accepts. We keep it attached: detaching
        // races the accept disposition against the link's in-flight-requeue on
        // teardown, which would test teardown, not the `accepted` outcome.
        let mut c1 = session
            .create_consumer("/queues/settle-accept")
            .await
            .expect("c1");
        let d = c1.recv().await.expect("recv");
        assert_eq!(text_of(&d), "only-once");
        c1.accept(&d).await.expect("accept");

        // Let the accept disposition settle at the broker before probing.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        // A second consumer on the same queue must find it empty — the accepted
        // message is gone, not merely invisible to c1.
        let mut c2 = session
            .create_consumer("/queues/settle-accept")
            .await
            .expect("c2");
        let again = tokio::time::timeout(std::time::Duration::from_millis(300), c2.recv()).await;
        assert!(again.is_err(), "an accepted delivery was redelivered");

        conn.close().await.expect("close");
    }
}
