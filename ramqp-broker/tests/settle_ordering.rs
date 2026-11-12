//! Issue #50: settlements a consumer sends right before it detaches (or
//! drops, or ends its session) reach the queue before the unsubscribe that
//! requeues whatever it still holds — so accepted messages are never
//! redelivered. Exercised directly and through a proxy that coalesces the
//! client's frames into one read (as `kubectl port-forward` does), on every
//! queue kind.

use std::net::SocketAddr;
use std::time::Duration;

use ramqp::{ConnectionBuilder, Message};
use ramqp_broker::{Broker, BrokerConfig, ShutdownHandle};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const ROUNDS: usize = 5;
const MESSAGES: usize = 10;
const ACCEPTED: usize = 2;

async fn start(config: BrokerConfig) -> (SocketAddr, ShutdownHandle) {
    let bound = Broker::new(config).bind("127.0.0.1:0").await.unwrap();
    let addr = bound.local_addr();
    let shutdown = bound.shutdown_handle();
    tokio::spawn(bound.run());
    (addr, shutdown)
}

/// A TCP proxy that holds client→broker bytes for `delay`, then forwards
/// everything that arrived meanwhile in one write: frames the client wrote
/// back to back (dispositions, then detach) reach the broker in one read.
async fn coalescing_proxy(target: SocketAddr, delay: Duration) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((client, _)) = listener.accept().await {
            let Ok(server) = tokio::net::TcpStream::connect(target).await else {
                continue;
            };
            let (mut cr, mut cw) = client.into_split();
            let (mut sr, mut sw) = server.into_split();
            tokio::spawn(async move {
                let mut buf = vec![0u8; 64 * 1024];
                loop {
                    let n = match cr.read(&mut buf).await {
                        Ok(0) | Err(_) => break,
                        Ok(n) => n,
                    };
                    let mut data = buf[..n].to_vec();
                    tokio::time::sleep(delay).await;
                    while let Ok(Ok(n)) =
                        tokio::time::timeout(Duration::from_millis(1), cr.read(&mut buf)).await
                    {
                        if n == 0 {
                            break;
                        }
                        data.extend_from_slice(&buf[..n]);
                    }
                    if sw.write_all(&data).await.is_err() {
                        break;
                    }
                }
                let _ = sw.shutdown().await;
            });
            tokio::spawn(async move {
                let _ = tokio::io::copy(&mut sr, &mut cw).await;
            });
        }
    });
    addr
}

/// How the consumer goes away right after its accepts.
#[derive(Debug, Clone, Copy)]
enum Exit {
    /// `consumer.detach()`.
    Detach,
    /// Drop the consumer (which detaches), then close the connection.
    DropThenClose,
    /// Drop the consumer, then end the session.
    DropThenEnd,
    /// End the session with the consumer still attached (no detach).
    EndSession,
    /// Close the connection with the consumer still attached.
    Close,
}

/// The next `count` messages on `address` (over a direct connection), by
/// body. Queues redeliver in publish order, so a redelivered accept (an
/// earlier message) would show up first — no idle wait needed to spot it.
async fn next_messages(addr: SocketAddr, address: &str, count: usize) -> Vec<String> {
    use ramqp::codec::Value;
    use ramqp::types::messaging::Body;
    let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
        .connect()
        .await
        .unwrap();
    let s = conn.begin_session().await.unwrap();
    let mut c = s.create_consumer(address).await.unwrap();
    let mut got = Vec::new();
    for _ in 0..count {
        let d = tokio::time::timeout(Duration::from_secs(10), c.recv())
            .await
            .unwrap_or_else(|_| panic!("{address}: only {got:?} arrived"))
            .unwrap();
        got.push(match d.message().unwrap().body {
            Body::Value(Value::String(text)) => text,
            other => panic!("unexpected body {other:?}"),
        });
        c.accept(&d).await.unwrap();
    }
    conn.close().await.ok();
    got
}

/// Run `ROUNDS` rounds (a fresh queue each): publish `MESSAGES`, accept
/// `ACCEPTED` through `client`, exit at once, and check exactly the
/// accepted ones are gone and the rest come back in publish order (the
/// departing consumer's unsettled in-flights included).
async fn rounds(addr: SocketAddr, client: SocketAddr, prefix: &str, tag: &str, exit: Exit) {
    for round in 0..ROUNDS {
        let address = format!("{prefix}{tag}-{exit:?}-{round}");
        let conn = ConnectionBuilder::new(format!("amqp://{addr}"))
            .connect()
            .await
            .unwrap();
        let s = conn.begin_session().await.unwrap();
        let p = s.create_producer(&address).await.unwrap();
        for i in 0..MESSAGES {
            p.send(Message::text(format!("msg-{i}"))).await.unwrap();
        }
        conn.close().await.unwrap();

        let conn = ConnectionBuilder::new(format!("amqp://{client}"))
            .connect()
            .await
            .unwrap();
        let s = conn.begin_session().await.unwrap();
        let mut c = s.create_consumer(&address).await.unwrap();
        for _ in 0..ACCEPTED {
            let d = tokio::time::timeout(Duration::from_secs(5), c.recv())
                .await
                .expect("delivery in time")
                .unwrap();
            c.accept(&d).await.unwrap();
        }
        match exit {
            Exit::Detach => {
                c.detach().await.unwrap();
                conn.close().await.ok();
            }
            Exit::DropThenClose => {
                drop(c);
                conn.close().await.ok();
            }
            Exit::DropThenEnd => {
                drop(c);
                s.end().await.ok();
                conn.close().await.ok();
            }
            Exit::EndSession => {
                s.end().await.ok();
                conn.close().await.ok();
                drop(c);
            }
            Exit::Close => {
                conn.close().await.ok();
                drop(c);
            }
        }
        let expected: Vec<String> = (ACCEPTED..MESSAGES).map(|i| format!("msg-{i}")).collect();
        assert_eq!(
            next_messages(addr, &address, MESSAGES - ACCEPTED).await,
            expected,
            "{address}: accepted messages redelivered, or the rest out of order"
        );
    }
}

async fn every_exit(prefix: &str) {
    let dir = tempfile::tempdir().unwrap();
    let mut config = BrokerConfig::default();
    config.data_dir = Some(dir.path().to_path_buf());
    let (addr, shutdown) = start(config).await;
    let proxy = coalescing_proxy(addr, Duration::from_millis(50)).await;
    for exit in [
        Exit::Detach,
        Exit::DropThenClose,
        Exit::DropThenEnd,
        Exit::EndSession,
        Exit::Close,
    ] {
        rounds(addr, addr, prefix, "direct", exit).await;
        rounds(addr, proxy, prefix, "proxy", exit).await;
    }
    shutdown.shutdown();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn transient_accepts_survive_an_immediate_exit() {
    every_exit("/queues/t").await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn quorum_accepts_survive_an_immediate_exit() {
    every_exit("/quorum/q").await;
}

#[cfg(feature = "store-redb")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn durable_accepts_survive_an_immediate_exit() {
    every_exit("/durable/d").await;
}
