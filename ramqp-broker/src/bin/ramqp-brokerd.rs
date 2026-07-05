//! `ramqp-brokerd` — the broker daemon.
//!
//! Minimal Phase 4 surface: listen address from `--listen`/`RAMQP_LISTEN`
//! (default `0.0.0.0:5672`), AllowAll auth, ctrl-c for graceful shutdown.
//! Real configuration (TLS, auth backends, policies) arrives with Phase 9.

use ramqp_broker::{Broker, BrokerConfig};

fn listen_addr() -> String {
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--listen" => {
                if let Some(v) = args.next() {
                    return v;
                }
            }
            "--help" | "-h" => {
                eprintln!("usage: ramqp-brokerd [--listen <addr:port>]");
                eprintln!("  RAMQP_LISTEN env var is honored; default 0.0.0.0:5672");
                std::process::exit(0);
            }
            other => {
                eprintln!("unknown argument: {other}");
                std::process::exit(2);
            }
        }
    }
    std::env::var("RAMQP_LISTEN").unwrap_or_else(|_| "0.0.0.0:5672".to_owned())
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let addr = listen_addr();
    let bound = Broker::new(BrokerConfig::default()).bind(&addr).await?;
    let shutdown = bound.shutdown_handle();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("ctrl-c received; shutting down");
            shutdown.shutdown();
        }
    });

    bound.run().await
}
