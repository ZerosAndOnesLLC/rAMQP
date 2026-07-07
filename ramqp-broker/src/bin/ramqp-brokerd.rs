//! `ramqp-brokerd` — the broker daemon.
//!
//! Standalone by default. Clustered when the node flags are given:
//!
//! ```text
//! ramqp-brokerd --listen 0.0.0.0:5672 \
//!   --node-id 1 --cluster-listen 0.0.0.0:7472 \
//!   --seed 1=host-a:7472 --seed 2=host-b:7472 --seed 3=host-c:7472
//! ```
//!
//! Env equivalents: `RAMQP_LISTEN`, `RAMQP_NODE_ID`, `RAMQP_CLUSTER_LISTEN`,
//! `RAMQP_SEEDS` (comma-separated `id=addr` pairs). Real configuration (TLS,
//! auth backends, policies) arrives with Phase 9.

use ramqp_broker::{Broker, BrokerConfig, ClusterMemberConfig};

struct Args {
    listen: String,
    node_id: Option<u64>,
    cluster_listen: Option<String>,
    seeds: Vec<(u64, String)>,
}

fn usage() -> ! {
    eprintln!(
        "usage: ramqp-brokerd [--listen <addr:port>]\n\
         \x20                    [--node-id <n> --cluster-listen <addr:port> --seed <id>=<addr> ...]\n\
         env: RAMQP_LISTEN, RAMQP_NODE_ID, RAMQP_CLUSTER_LISTEN, RAMQP_SEEDS=1=a:7472,2=b:7472"
    );
    std::process::exit(2);
}

fn parse_seed(s: &str) -> Option<(u64, String)> {
    let (id, addr) = s.split_once('=')?;
    Some((id.trim().parse().ok()?, addr.trim().to_owned()))
}

fn parse_args() -> Args {
    let mut args = Args {
        listen: std::env::var("RAMQP_LISTEN").unwrap_or_else(|_| "0.0.0.0:5672".to_owned()),
        node_id: std::env::var("RAMQP_NODE_ID")
            .ok()
            .and_then(|v| v.parse().ok()),
        cluster_listen: std::env::var("RAMQP_CLUSTER_LISTEN").ok(),
        seeds: std::env::var("RAMQP_SEEDS")
            .map(|v| v.split(',').filter_map(parse_seed).collect())
            .unwrap_or_default(),
    };
    let mut argv = std::env::args().skip(1);
    while let Some(arg) = argv.next() {
        match arg.as_str() {
            "--listen" => args.listen = argv.next().unwrap_or_else(|| usage()),
            "--node-id" => {
                args.node_id = Some(
                    argv.next()
                        .and_then(|v| v.parse().ok())
                        .unwrap_or_else(|| usage()),
                );
            }
            "--cluster-listen" => {
                args.cluster_listen = Some(argv.next().unwrap_or_else(|| usage()))
            }
            "--seed" => {
                let seed = argv
                    .next()
                    .and_then(|v| parse_seed(&v))
                    .unwrap_or_else(|| usage());
                args.seeds.push(seed);
            }
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown argument: {other}");
                usage();
            }
        }
    }
    args
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = parse_args();
    let mut config = BrokerConfig::default();
    match (&args.node_id, &args.cluster_listen, args.seeds.is_empty()) {
        (Some(node_id), Some(listen), false) => {
            if !args.seeds.iter().any(|(id, _)| id == node_id) {
                eprintln!("--node-id {node_id} must appear in the --seed list");
                std::process::exit(2);
            }
            config.cluster = Some(ClusterMemberConfig::new(
                *node_id,
                listen.clone(),
                args.seeds.clone(),
            ));
        }
        (None, None, true) => {} // standalone
        _ => {
            eprintln!("clustering needs all of --node-id, --cluster-listen, and --seed(s)");
            std::process::exit(2);
        }
    }

    let clustered = config.cluster.is_some();
    let bound = Broker::new(config).bind(&args.listen).await?;

    // The default broker accepts every connection (AllowAll). That is fine on a
    // loopback / trusted network but an open relay on a public bind — warn
    // loudly so it is never an accident.
    let listen = bound.local_addr();
    if !listen.ip().is_loopback() {
        tracing::warn!(
            addr = %listen,
            "listening on a non-loopback address with NO authentication (AllowAll) — \
             anyone who can reach this port can use the broker; configure an authenticator \
             before exposing it"
        );
    }
    if clustered {
        tracing::info!(node_id = ?args.node_id, "cluster member starting");
    }

    let shutdown = bound.shutdown_handle();

    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            tracing::info!("ctrl-c received; shutting down");
            shutdown.shutdown();
        }
    });

    bound.run().await
}
