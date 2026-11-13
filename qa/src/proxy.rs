//! A fault-injecting TCP proxy: network trouble without root.
//!
//! - `--coalesce-ms`: holds client→broker bytes, then forwards everything
//!   that arrived meanwhile in one write, so frames the client wrote back to
//!   back reach the broker in one read (as `kubectl port-forward` does —
//!   the condition behind issue #50).
//! - `--delay-ms` / `--jitter-ms`: extra latency on every client→broker
//!   chunk, both directions' ordering kept.
//! - `--reset-every-ms`: each connection is cut after a random 0.5–1.5×
//!   this long, mid-whatever it was doing (both sockets closed at once, with
//!   no AMQP `close`: to both peers the connection just dies).
//!
//! Runs until killed (or `--secs`).

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

use crate::common::Rng;

#[derive(clap::Args)]
pub struct Args {
    /// Listen address, e.g. 127.0.0.1:5800.
    #[arg(long)]
    listen: String,
    /// Broker address, e.g. 127.0.0.1:5672.
    #[arg(long)]
    target: String,
    #[arg(long, default_value_t = 0)]
    coalesce_ms: u64,
    #[arg(long, default_value_t = 0)]
    delay_ms: u64,
    #[arg(long, default_value_t = 0)]
    jitter_ms: u64,
    /// Cut each connection after a random 0.5–1.5× this (0 = never).
    #[arg(long, default_value_t = 0)]
    reset_every_ms: u64,
    /// Stop after this many seconds (0 = run until killed).
    #[arg(long, default_value_t = 0)]
    secs: u64,
}

pub async fn run(a: Args) -> bool {
    let listener = match TcpListener::bind(&a.listen).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("proxy: cannot listen on {}: {e}", a.listen);
            return false;
        }
    };
    eprintln!("proxy: {} -> {}", a.listen, a.target);
    let a = std::sync::Arc::new(a);
    let serve = async {
        let mut n: u64 = 0;
        while let Ok((client, _)) = listener.accept().await {
            n += 1;
            tokio::spawn(connection(a.clone(), client, n));
        }
    };
    if a.secs > 0 {
        let _ = tokio::time::timeout(Duration::from_secs(a.secs), serve).await;
    } else {
        serve.await;
    }
    true
}

async fn connection(a: std::sync::Arc<Args>, client: TcpStream, n: u64) {
    let Ok(server) = TcpStream::connect(&a.target).await else {
        return;
    };
    let _ = client.set_nodelay(true);
    let _ = server.set_nodelay(true);
    let mut rng = Rng::seeded(n);
    let reset_after = (a.reset_every_ms > 0).then(|| {
        let base = a.reset_every_ms;
        Duration::from_millis(base / 2 + rng.below(base.max(1)))
    });
    let (mut cr, mut cw) = client.into_split();
    let (mut sr, mut sw) = server.into_split();
    let args = a.clone();
    let up = async move {
        let mut rng = Rng::seeded(n ^ 0xABCD);
        let mut buf = vec![0u8; 64 * 1024];
        loop {
            let k = match cr.read(&mut buf).await {
                Ok(0) | Err(_) => break,
                Ok(k) => k,
            };
            let mut data = buf[..k].to_vec();
            let mut wait = args.delay_ms;
            if args.jitter_ms > 0 {
                wait += rng.below(args.jitter_ms + 1);
            }
            if wait > 0 {
                tokio::time::sleep(Duration::from_millis(wait)).await;
            }
            if args.coalesce_ms > 0 {
                tokio::time::sleep(Duration::from_millis(args.coalesce_ms)).await;
                while let Ok(Ok(k)) =
                    tokio::time::timeout(Duration::from_millis(1), cr.read(&mut buf)).await
                {
                    if k == 0 {
                        break;
                    }
                    data.extend_from_slice(&buf[..k]);
                }
            }
            if sw.write_all(&data).await.is_err() {
                break;
            }
        }
        let _ = sw.shutdown().await;
    };
    let down = async move {
        let _ = tokio::io::copy(&mut sr, &mut cw).await;
        let _ = cw.shutdown().await;
    };
    let both = async {
        tokio::join!(up, down);
    };
    match reset_after {
        // Dropping the halves mid-flight cuts the connection both ways.
        Some(after) => {
            let _ = tokio::time::timeout(after, both).await;
        }
        None => both.await,
    }
}
