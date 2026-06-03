//! The [`Connection`] handle (WP-5.2): a cheap, clonable entry point that
//! spawns and talks to the driver task.

use std::sync::Arc;

use tokio::sync::{broadcast, mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::api::session::Session;
use crate::config::Config;
use crate::connection::driver::Driver;
use crate::error::{ConnectError, ErrorKind, SessionError};
use crate::observe::{ConnectionEvent, EventBus, SharedMetrics};
use crate::proto::DriverCommand;
use crate::sasl::SaslProfile;
use crate::transport::header::ProtocolHeader;
use crate::transport::{self, Address};
use crate::transport::frame::FramedTransport;
use crate::types::performatives::Begin;

/// An open AMQP connection. Dropping the last handle (this plus all sessions)
/// triggers a graceful close; [`close`](Connection::close) awaits it explicitly.
#[derive(Debug)]
pub struct Connection {
    commands: mpsc::Sender<DriverCommand>,
    events: EventBus,
    config: Arc<Config>,
    driver: Option<JoinHandle<Result<(), ConnectError>>>,
}

impl Connection {
    /// Open a connection to `url` with default config (PLAIN if the URL carries
    /// credentials, else ANONYMOUS).
    pub async fn open(url: &str) -> Result<Connection, ConnectError> {
        crate::api::client::ConnectionBuilder::new(url).connect().await
    }

    /// Start building a connection to `url`.
    pub fn builder(url: &str) -> crate::api::client::ConnectionBuilder {
        crate::api::client::ConnectionBuilder::new(url)
    }

    /// Open a connection, retrying retryable failures with jittered backoff per
    /// `config.connection.reconnect`.
    pub async fn open_resilient(url: &str, config: Config) -> Result<Connection, ConnectError> {
        crate::resilience::connect_with_retry(url, config, crate::observe::noop_metrics()).await
    }

    /// Establish the transport, run the SASL + AMQP handshakes, and spawn the
    /// driver. Used by [`ConnectionBuilder`](crate::api::client::ConnectionBuilder).
    pub(crate) async fn establish(
        addr: Address,
        config: Config,
        metrics: SharedMetrics,
        profile: SaslProfile,
        tls: crate::transport::TlsConfig,
    ) -> Result<Connection, ConnectError> {
        let config = Arc::new(config);

        let mut stream = transport::connect(&addr, &tls).await?;

        // SASL layer: header → mechanism negotiation → AMQP header.
        ProtocolHeader::SASL.negotiate(&mut stream).await?;
        let mut framed = FramedTransport::new(stream, config.connection.max_frame_size);
        crate::sasl::negotiate(&mut framed, &profile, Some(&addr.host)).await?;
        ProtocolHeader::AMQP.negotiate(framed.stream_mut()).await?;

        // Connection open + driver spawn.
        let events = EventBus::default();
        let (commands, rx) = mpsc::channel(config.connection.command_buffer);
        let driver = Driver::open(framed, config.clone(), metrics, events.clone(), rx).await?;
        let join = tokio::spawn(driver.run());

        Ok(Connection {
            commands,
            events,
            config,
            driver: Some(join),
        })
    }

    /// Begin a new session on this connection.
    pub async fn begin_session(&self) -> Result<Session, SessionError> {
        let begin = Begin {
            next_outgoing_id: 0,
            incoming_window: self.config.session.incoming_window,
            outgoing_window: self.config.session.outgoing_window,
            handle_max: self.config.session.handle_max,
            ..Default::default()
        };
        let (reply_tx, reply_rx) = oneshot::channel();
        let (evt_tx, evt_rx) = mpsc::unbounded_channel();
        self.commands
            .send(DriverCommand::BeginSession {
                begin: Box::new(begin),
                events: evt_tx,
                reply: reply_tx,
            })
            .await
            .map_err(|_| SessionError::msg(ErrorKind::NotConnected, "connection closed"))?;
        let opened = reply_rx
            .await
            .map_err(|_| SessionError::msg(ErrorKind::Cancelled, "driver dropped"))??;
        Ok(Session::new(
            self.commands.clone(),
            opened.channel,
            evt_rx,
            self.config.clone(),
        ))
    }

    /// Subscribe to connection lifecycle events.
    pub fn subscribe(&self) -> broadcast::Receiver<ConnectionEvent> {
        self.events.subscribe()
    }

    /// The configuration in effect.
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Whether the driver task is still running (used by the connection pool).
    pub fn is_alive(&self) -> bool {
        self.driver.as_ref().map(|j| !j.is_finished()).unwrap_or(false)
    }

    /// Gracefully close the connection and await the driver's shutdown,
    /// surfacing a peer-error close or a driver failure to the caller.
    pub async fn close(mut self) -> Result<(), ConnectError> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .send(DriverCommand::CloseConnection {
                error: None,
                reply: tx,
            })
            .await
            .map_err(|_| ConnectError::msg(ErrorKind::NotConnected, "connection already closed"))?;
        let result = rx
            .await
            .map_err(|_| ConnectError::msg(ErrorKind::Cancelled, "driver dropped"))?;
        if let Some(join) = self.driver.take() {
            let _ = join.await;
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    use crate::codec::{Symbol, to_vec};
    use crate::transport::frame::FrameBody;
    use crate::types::definitions::Role;
    use crate::types::messaging::{Accepted, DeliveryState, Message, Source, Target, TargetArchetype};
    use crate::types::performatives::{
        Attach, Begin, Close, Detach, Disposition, End, Flow, Open, Performative, Transfer,
    };
    use crate::types::sasl::{SaslCode, SaslFrame, SaslMechanisms, SaslOutcome};

    /// Complete the SASL + AMQP handshakes and a `begin`, returning the framed
    /// transport positioned just after the session is mapped.
    async fn broker_handshake(stream: TcpStream) -> FramedTransport<TcpStream> {
        let mut stream = stream;
        let mut hdr = [0u8; 8];
        stream.read_exact(&mut hdr).await.unwrap();
        stream
            .write_all(&ProtocolHeader::SASL.to_bytes())
            .await
            .unwrap();
        let mut framed = FramedTransport::new(stream, 1 << 16);
        framed
            .send_sasl(&SaslFrame::Mechanisms(SaslMechanisms {
                sasl_server_mechanisms: vec![Symbol::new("ANONYMOUS")],
            }))
            .await
            .unwrap();
        let _init = framed.read_frame().await.unwrap();
        framed
            .send_sasl(&SaslFrame::Outcome(SaslOutcome {
                code: SaslCode::Ok,
                additional_data: None,
            }))
            .await
            .unwrap();
        let mut hdr2 = [0u8; 8];
        framed.stream_mut().read_exact(&mut hdr2).await.unwrap();
        framed
            .stream_mut()
            .write_all(&ProtocolHeader::AMQP.to_bytes())
            .await
            .unwrap();
        let _ = framed.read_frame().await.unwrap();
        framed
            .send_amqp(0, &Performative::Open(Open::new("broker")), None)
            .await
            .unwrap();
        let begin = framed.read_frame().await.unwrap();
        let ch = begin.channel;
        framed
            .send_amqp(
                0,
                &Performative::Begin(Begin {
                    remote_channel: Some(ch),
                    incoming_window: 100,
                    outgoing_window: 100,
                    ..Default::default()
                }),
                None,
            )
            .await
            .unwrap();
        framed
    }

    /// A minimal AMQP broker that echoes one message in each direction.
    async fn mock_broker(stream: TcpStream) {
        let mut framed = broker_handshake(stream).await;
        let mut sent_to_consumer = false;
        let mut current_delivery: Option<u32> = None;
        loop {
            let frame = match framed.read_frame().await {
                Ok(f) => f,
                Err(_) => break,
            };
            match frame.body {
                FrameBody::Amqp(Performative::Attach(a), _) => {
                    if a.role == Role::Sender {
                        // The client is producing; we are the receiver + grant credit.
                        framed
                            .send_amqp(
                                0,
                                &Performative::Attach(Attach {
                                    name: a.name.clone(),
                                    handle: 0,
                                    role: Role::Receiver,
                                    source: Some(Source::default()),
                                    target: Some(TargetArchetype::from(Target::new("queue"))),
                                    ..Default::default()
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                        framed
                            .send_amqp(
                                0,
                                &Performative::Flow(Flow {
                                    next_incoming_id: Some(0),
                                    incoming_window: 100,
                                    next_outgoing_id: 0,
                                    outgoing_window: 100,
                                    handle: Some(0),
                                    delivery_count: Some(0),
                                    link_credit: Some(10),
                                    ..Default::default()
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    } else {
                        // The client is consuming; we are the sender.
                        framed
                            .send_amqp(
                                0,
                                &Performative::Attach(Attach {
                                    name: a.name.clone(),
                                    handle: 0,
                                    role: Role::Sender,
                                    source: Some(Source::new("queue")),
                                    target: Some(TargetArchetype::from(Target::default())),
                                    initial_delivery_count: Some(0),
                                    ..Default::default()
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                }
                FrameBody::Amqp(Performative::Flow(f), _) => {
                    // Consumer granted us credit: deliver one message.
                    if !sent_to_consumer && f.link_credit.unwrap_or(0) > 0 {
                        sent_to_consumer = true;
                        let body = to_vec(&Message::text("from-broker"));
                        framed
                            .send_amqp(
                                0,
                                &Performative::Transfer(Transfer {
                                    handle: 0,
                                    delivery_id: Some(0),
                                    delivery_tag: Some(Bytes::from_static(b"d1")),
                                    message_format: Some(0),
                                    settled: Some(false),
                                    more: false,
                                    ..Default::default()
                                }),
                                Some(&body),
                            )
                            .await
                            .unwrap();
                    }
                }
                FrameBody::Amqp(Performative::Transfer(t), _) => {
                    // Track the delivery id (present only on the first frame) and
                    // settle once the final (more = false) frame arrives.
                    if let Some(id) = t.delivery_id {
                        current_delivery = Some(id);
                    }
                    if !t.more {
                        let id = current_delivery.take().unwrap();
                        framed
                            .send_amqp(
                                0,
                                &Performative::Disposition(Disposition {
                                    role: Role::Receiver,
                                    first: id,
                                    last: None,
                                    settled: true,
                                    state: Some(DeliveryState::Accepted(Accepted::default())),
                                    batchable: false,
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                }
                FrameBody::Amqp(Performative::Detach(d), _) => {
                    framed
                        .send_amqp(
                            0,
                            &Performative::Detach(Detach {
                                handle: d.handle,
                                closed: true,
                                error: None,
                            }),
                            None,
                        )
                        .await
                        .unwrap();
                }
                FrameBody::Amqp(Performative::End(_), _) => {
                    framed
                        .send_amqp(0, &Performative::End(End { error: None }), None)
                        .await
                        .unwrap();
                }
                FrameBody::Amqp(Performative::Close(_), _) => {
                    framed
                        .send_amqp(0, &Performative::Close(Close { error: None }), None)
                        .await
                        .unwrap();
                    break;
                }
                _ => {}
            }
        }
    }

    async fn spawn_broker() -> (String, tokio::task::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let handle = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            mock_broker(sock).await;
        });
        (format!("amqp://127.0.0.1:{port}"), handle)
    }

    #[tokio::test]
    async fn end_to_end_produce() {
        let (url, broker) = spawn_broker().await;
        let conn = Connection::open(&url).await.unwrap();
        let session = conn.begin_session().await.unwrap();
        let producer = session.create_producer("queue").await.unwrap();

        let outcome = producer.send(Message::text("hello")).await.unwrap();
        assert!(matches!(outcome, DeliveryState::Accepted(_)));

        producer.detach().await.unwrap();
        session.end().await.unwrap();
        conn.close().await.unwrap();
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn send_settled_outbox_backpressures_without_credit() {
        use std::time::Duration;
        // A broker that attaches the sender but never grants credit, so nothing
        // can be written: the bounded outbox must back-pressure rather than
        // buffer without limit.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("amqp://127.0.0.1:{port}");
        let broker = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut framed = broker_handshake(sock).await;
            while let Ok(frame) = framed.read_frame().await {
                if let FrameBody::Amqp(Performative::Attach(a), _) = frame.body {
                    framed
                        .send_amqp(
                            0,
                            &Performative::Attach(Attach {
                                name: a.name.clone(),
                                handle: 0,
                                role: Role::Receiver,
                                source: Some(Source::default()),
                                target: Some(TargetArchetype::from(Target::new("queue"))),
                                ..Default::default()
                            }),
                            None,
                        )
                        .await
                        .unwrap();
                    // Deliberately grant NO link credit.
                }
            }
        });

        let mut config = Config::default();
        config.link.max_outbox = 2;
        let conn = crate::api::client::ConnectionBuilder::new(&url)
            .config(config)
            .connect()
            .await
            .unwrap();
        let session = conn.begin_session().await.unwrap();
        let producer = session.create_producer("queue").await.unwrap();

        // Two fire-and-forget sends fill the bounded outbox (unwritten: no credit).
        producer.send_settled(Message::text("a")).await.unwrap();
        producer.send_settled(Message::text("b")).await.unwrap();
        // The third must block until a slot frees — which never happens here.
        let blocked = tokio::time::timeout(
            Duration::from_millis(300),
            producer.send_settled(Message::text("c")),
        )
        .await;
        assert!(
            blocked.is_err(),
            "send_settled must back-pressure once the bounded outbox is full"
        );

        drop(producer);
        drop(session);
        drop(conn);
        broker.abort();
    }

    #[tokio::test]
    async fn metrics_are_emitted() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicU64, Ordering};

        #[derive(Default)]
        struct Counters {
            frames_in: AtomicU64,
            frames_out: AtomicU64,
            sent: AtomicU64,
            settle_latencies: AtomicU64,
        }
        impl crate::observe::Metrics for Counters {
            fn on_frame_received(&self, _bytes: usize) {
                self.frames_in.fetch_add(1, Ordering::Relaxed);
            }
            fn on_frame_sent(&self, _bytes: usize) {
                self.frames_out.fetch_add(1, Ordering::Relaxed);
            }
            fn on_transfer_sent(&self) {
                self.sent.fetch_add(1, Ordering::Relaxed);
            }
            fn on_send_to_settle(&self, _latency: std::time::Duration) {
                self.settle_latencies.fetch_add(1, Ordering::Relaxed);
            }
        }

        let (url, broker) = spawn_broker().await;
        let counters = Arc::new(Counters::default());
        let conn = crate::api::client::ConnectionBuilder::new(&url)
            .metrics(counters.clone())
            .connect()
            .await
            .unwrap();
        let session = conn.begin_session().await.unwrap();
        let producer = session.create_producer("queue").await.unwrap();
        producer.send(Message::text("m")).await.unwrap();
        producer.detach().await.unwrap();
        session.end().await.unwrap();
        conn.close().await.unwrap();
        broker.await.unwrap();

        assert!(counters.frames_in.load(Ordering::Relaxed) > 0);
        assert!(counters.frames_out.load(Ordering::Relaxed) > 0);
        assert_eq!(counters.sent.load(Ordering::Relaxed), 1);
        // the send-to-settle latency metric fired once for the settled delivery
        assert_eq!(counters.settle_latencies.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn end_to_end_consume() {
        let (url, broker) = spawn_broker().await;
        let conn = Connection::open(&url).await.unwrap();
        let session = conn.begin_session().await.unwrap();
        let mut consumer = session.create_consumer("queue").await.unwrap();

        let delivery = consumer.recv().await.unwrap();
        assert_eq!(delivery.message().unwrap(), Message::text("from-broker"));
        consumer.accept(&delivery).await.unwrap();

        consumer.detach().await.unwrap();
        session.end().await.unwrap();
        conn.close().await.unwrap();
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn end_to_end_produce_multiframe() {
        let (url, broker) = spawn_broker().await;
        let mut config = Config::default();
        config.connection.max_frame_size = 512; // force multi-frame splitting
        let conn = crate::api::client::ConnectionBuilder::new(&url)
            .config(config)
            .connect()
            .await
            .unwrap();
        let session = conn.begin_session().await.unwrap();
        let producer = session.create_producer("queue").await.unwrap();

        // A body well over the frame size must split into multiple transfers and
        // be reassembled by the peer.
        let big = "x".repeat(4000);
        let outcome = producer.send(Message::text(&big)).await.unwrap();
        assert!(matches!(outcome, DeliveryState::Accepted(_)));

        producer.detach().await.unwrap();
        session.end().await.unwrap();
        conn.close().await.unwrap();
        broker.await.unwrap();
    }

    #[tokio::test]
    async fn second_mode_settlement() {
        use crate::types::definitions::ReceiverSettleMode;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, Ordering};

        let unsettled_seen = Arc::new(AtomicBool::new(false));
        let flag = unsettled_seen.clone();

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let url = format!("amqp://127.0.0.1:{port}");
        let broker = tokio::spawn(async move {
            let (sock, _) = listener.accept().await.unwrap();
            let mut framed = broker_handshake(sock).await;
            loop {
                let frame = match framed.read_frame().await {
                    Ok(f) => f,
                    Err(_) => break,
                };
                match frame.body {
                    FrameBody::Amqp(Performative::Attach(a), _) => {
                        framed
                            .send_amqp(
                                0,
                                &Performative::Attach(Attach {
                                    name: a.name.clone(),
                                    handle: 0,
                                    role: Role::Sender,
                                    rcv_settle_mode: ReceiverSettleMode::Second,
                                    source: Some(Source::new("queue")),
                                    target: Some(TargetArchetype::from(Target::default())),
                                    initial_delivery_count: Some(0),
                                    ..Default::default()
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    FrameBody::Amqp(Performative::Flow(f), _) => {
                        if f.link_credit.unwrap_or(0) > 0 {
                            let body = to_vec(&Message::text("second"));
                            framed
                                .send_amqp(
                                    0,
                                    &Performative::Transfer(Transfer {
                                        handle: 0,
                                        delivery_id: Some(0),
                                        delivery_tag: Some(Bytes::from_static(b"d")),
                                        message_format: Some(0),
                                        settled: Some(false),
                                        more: false,
                                        ..Default::default()
                                    }),
                                    Some(&body),
                                )
                                .await
                                .unwrap();
                        }
                    }
                    FrameBody::Amqp(Performative::Disposition(d), _) => {
                        // In `second` mode the consumer proposes the outcome unsettled.
                        if !d.settled {
                            flag.store(true, Ordering::Relaxed);
                        }
                        // The sender then confirms (settled) to complete settlement.
                        framed
                            .send_amqp(
                                0,
                                &Performative::Disposition(Disposition {
                                    role: Role::Sender,
                                    first: d.first,
                                    last: d.last,
                                    settled: true,
                                    state: Some(DeliveryState::Accepted(Accepted::default())),
                                    batchable: false,
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    FrameBody::Amqp(Performative::Detach(d), _) => {
                        framed
                            .send_amqp(
                                0,
                                &Performative::Detach(Detach {
                                    handle: d.handle,
                                    closed: true,
                                    error: None,
                                }),
                                None,
                            )
                            .await
                            .unwrap();
                    }
                    FrameBody::Amqp(Performative::End(_), _) => {
                        framed
                            .send_amqp(0, &Performative::End(End { error: None }), None)
                            .await
                            .unwrap();
                    }
                    FrameBody::Amqp(Performative::Close(_), _) => {
                        framed
                            .send_amqp(0, &Performative::Close(Close { error: None }), None)
                            .await
                            .unwrap();
                        break;
                    }
                    _ => {}
                }
            }
        });

        let mut config = Config::default();
        config.link.receiver_settle_mode = ReceiverSettleMode::Second;
        let conn = crate::api::client::ConnectionBuilder::new(&url)
            .config(config)
            .connect()
            .await
            .unwrap();
        let session = conn.begin_session().await.unwrap();
        let mut consumer = session.create_consumer("queue").await.unwrap();
        let delivery = consumer.recv().await.unwrap();
        assert_eq!(delivery.message().unwrap(), Message::text("second"));
        consumer.accept(&delivery).await.unwrap();

        consumer.detach().await.unwrap();
        session.end().await.unwrap();
        conn.close().await.unwrap();
        broker.await.unwrap();

        assert!(
            unsettled_seen.load(Ordering::Relaxed),
            "a second-mode consumer must send an unsettled disposition first"
        );
    }
}
