//! Transparent WebSocket -> TCP proxy, so AMQP-1.0-over-WebSocket can be
//! benchmarked against the *same* plain-TCP broker (RabbitMQ 4.x doesn't expose
//! AMQP 1.0 over WebSocket natively). It accepts WS connections — selecting the
//! `amqp` subprotocol the ramqp client requires — and byte-pipes each one to the
//! upstream AMQP TCP port. WS message boundaries are irrelevant: the AMQP layer
//! treats the payload as an opaque byte stream.
//!
//!   WS_LISTEN=127.0.0.1:5673 WS_UPSTREAM=127.0.0.1:5672 \
//!     cargo run -p ramqp-bench-compare --release --bin wsproxy
//!
//! Then point the rig at it:  AMQP_URL=ws://127.0.0.1:5673

use std::error::Error;

use futures_util::{SinkExt, StreamExt};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tokio_tungstenite::tungstenite::http::header::SEC_WEBSOCKET_PROTOCOL;

type BoxErr = Box<dyn Error + Send + Sync>;

#[tokio::main]
async fn main() -> Result<(), BoxErr> {
    let listen = std::env::var("WS_LISTEN").unwrap_or_else(|_| "127.0.0.1:5673".into());
    let upstream = std::env::var("WS_UPSTREAM").unwrap_or_else(|_| "127.0.0.1:5672".into());
    let listener = TcpListener::bind(&listen).await?;
    eprintln!("wsproxy: ws://{listen} -> tcp://{upstream}");
    loop {
        let (sock, _) = listener.accept().await?;
        let upstream = upstream.clone();
        tokio::spawn(async move {
            if let Err(e) = handle(sock, &upstream).await {
                eprintln!("wsproxy: connection ended: {e}");
            }
        });
    }
}

async fn handle(sock: TcpStream, upstream: &str) -> Result<(), BoxErr> {
    sock.set_nodelay(true).ok();
    // Complete the WS handshake, selecting the `amqp` subprotocol in the response
    // (the client rejects the connection otherwise). The callback's large `Err`
    // variant is tungstenite's `ErrorResponse` type — fixed by its API, not ours.
    #[allow(clippy::result_large_err)]
    let ws = tokio_tungstenite::accept_hdr_async(sock, |_req: &Request, mut resp: Response| {
        resp.headers_mut()
            .insert(SEC_WEBSOCKET_PROTOCOL, HeaderValue::from_static("amqp"));
        Ok(resp)
    })
    .await?;

    let tcp = TcpStream::connect(upstream).await?;
    tcp.set_nodelay(true).ok();
    let (mut tcp_r, mut tcp_w) = tcp.into_split();
    let (mut ws_tx, mut ws_rx) = ws.split();

    // WS -> upstream: write each binary message's payload to the TCP socket.
    let ws_to_tcp = async move {
        while let Some(msg) = ws_rx.next().await {
            match msg? {
                Message::Binary(b) => tcp_w.write_all(&b).await?,
                Message::Close(_) => break,
                _ => {} // ping/pong/text carry no AMQP bytes
            }
        }
        tcp_w.shutdown().await.ok();
        Ok::<(), BoxErr>(())
    };

    // upstream -> WS: forward TCP bytes as binary messages.
    let tcp_to_ws = async move {
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let n = tcp_r.read(&mut buf).await?;
            if n == 0 {
                break;
            }
            ws_tx
                .send(Message::Binary(bytes::Bytes::copy_from_slice(&buf[..n])))
                .await?;
        }
        ws_tx.close().await.ok();
        Ok::<(), BoxErr>(())
    };

    tokio::select! {
        r = ws_to_tcp => r?,
        r = tcp_to_ws => r?,
    }
    Ok(())
}
