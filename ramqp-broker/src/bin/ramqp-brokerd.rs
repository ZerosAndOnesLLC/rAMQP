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
//! Configuration layers, lowest to highest precedence: the config file
//! (`--config <path>` / `RAMQP_CONFIG`; TOML — users, permissions, policies,
//! pre-declared topology, listeners, limits; see
//! `ramqp_broker::file_config`), then environment variables (`RAMQP_LISTEN`,
//! `RAMQP_NODE_ID`, `RAMQP_CLUSTER_LISTEN`, `RAMQP_SEEDS`, `RAMQP_DATA_DIR`,
//! `RAMQP_MANAGEMENT_LISTEN`, `RAMQP_ALLOW_ANONYMOUS`), then flags.
//!
//! The daemon **refuses to start** on a non-loopback listener with no users
//! configured unless anonymous access is explicitly allowed
//! (`--allow-anonymous` / `allow_anonymous = true`).
//!
//! `ramqp-brokerd hash-password [--mechanism SCRAM-SHA-256] [--iterations N]`
//! reads a password from stdin and prints an RFC 5803 verifier line for a
//! user's `verifier_file`.
//!
//! **Security:** the `--cluster-listen` fabric port is unauthenticated and
//! unencrypted — it must only be reachable from the cluster's own nodes
//! (isolated network / firewall). See `ClusterMemberConfig`'s security note.

use std::sync::Arc;

use ramqp_broker::file_config::{FileConfig, parse_seed};
use ramqp_broker::{Authenticator, Broker, BrokerConfig, ClusterMemberConfig};

// Use jemalloc as the global allocator. glibc's malloc fragments its per-thread
// arenas under the broker's high connection open/close churn, growing RSS
// without bound and collapsing throughput over a long run (issue #23); jemalloc
// holds both flat. Disable with `--no-default-features` where jemalloc is
// unavailable.
#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(Default)]
struct Args {
    config: Option<std::path::PathBuf>,
    listen: Option<String>,
    node_id: Option<u64>,
    cluster_listen: Option<String>,
    seeds: Vec<(u64, String)>,
    data_dir: Option<std::path::PathBuf>,
    management_listen: Option<String>,
    management_token_file: Option<std::path::PathBuf>,
    tls_listen: Option<String>,
    tls_cert: Option<std::path::PathBuf>,
    tls_key: Option<std::path::PathBuf>,
    tls_client_ca: Option<std::path::PathBuf>,
    allow_anonymous: bool,
}

fn usage() -> ! {
    eprintln!(
        "usage: ramqp-brokerd [--config <file.toml>] [--listen <addr:port>]\n\
         \x20                    [--node-id <n> --cluster-listen <addr:port> --seed <id>=<addr> ...]\n\
         \x20                    [--data-dir <path>] [--management-listen <addr:port>]\n\
         \x20                    [--management-token-file <path>]\n\
         \x20                    [--tls-listen <addr:port> --tls-cert <pem> --tls-key <pem>\n\
         \x20                     [--tls-client-ca <pem>]]   (a `tls` or `fips` build)\n\
         \x20                    [--allow-anonymous]\n\
         \x20      ramqp-brokerd hash-password [--mechanism SCRAM-SHA-256|SCRAM-SHA-512|SCRAM-SHA-1]\n\
         \x20                    [--iterations <n>]   (password on stdin)\n\
         env: RAMQP_CONFIG, RAMQP_LISTEN, RAMQP_NODE_ID, RAMQP_CLUSTER_LISTEN,\n\
         \x20    RAMQP_SEEDS=1=a:7472,2=b:7472, RAMQP_DATA_DIR (durable queues need a build with\n\
         \x20    --features store-redb), RAMQP_MANAGEMENT_LISTEN,\n\
         \x20    RAMQP_MANAGEMENT_TOKEN_FILE, RAMQP_ALLOW_ANONYMOUS=1, RAMQP_TLS_LISTEN,\n\
         \x20    RAMQP_TLS_CERT, RAMQP_TLS_KEY, RAMQP_TLS_CLIENT_CA"
    );
    std::process::exit(2);
}

fn fail(msg: impl std::fmt::Display) -> ! {
    eprintln!("ramqp-brokerd: {msg}");
    std::process::exit(2);
}

fn env_flag(name: &str) -> bool {
    std::env::var(name).is_ok_and(|v| matches!(v.as_str(), "1" | "true" | "yes" | "on"))
}

/// Environment first, then flags (flags win).
fn parse_args(argv: impl Iterator<Item = String>) -> Args {
    let mut args = Args {
        config: std::env::var("RAMQP_CONFIG").ok().map(Into::into),
        listen: std::env::var("RAMQP_LISTEN").ok(),
        node_id: std::env::var("RAMQP_NODE_ID")
            .ok()
            .and_then(|v| v.parse().ok()),
        cluster_listen: std::env::var("RAMQP_CLUSTER_LISTEN").ok(),
        seeds: std::env::var("RAMQP_SEEDS")
            .map(|v| v.split(',').filter_map(parse_seed).collect())
            .unwrap_or_default(),
        data_dir: std::env::var("RAMQP_DATA_DIR").ok().map(Into::into),
        management_listen: std::env::var("RAMQP_MANAGEMENT_LISTEN").ok(),
        management_token_file: std::env::var("RAMQP_MANAGEMENT_TOKEN_FILE")
            .ok()
            .map(Into::into),
        tls_listen: std::env::var("RAMQP_TLS_LISTEN").ok(),
        tls_cert: std::env::var("RAMQP_TLS_CERT").ok().map(Into::into),
        tls_key: std::env::var("RAMQP_TLS_KEY").ok().map(Into::into),
        tls_client_ca: std::env::var("RAMQP_TLS_CLIENT_CA").ok().map(Into::into),
        allow_anonymous: env_flag("RAMQP_ALLOW_ANONYMOUS"),
    };
    let mut argv = argv;
    let mut flag_seeds = Vec::new();
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--config" => args.config = Some(value().into()),
            "--listen" => args.listen = Some(value()),
            "--node-id" => {
                args.node_id = Some(value().parse().unwrap_or_else(|_| usage()));
            }
            "--cluster-listen" => args.cluster_listen = Some(value()),
            "--data-dir" => args.data_dir = Some(value().into()),
            "--management-listen" => args.management_listen = Some(value()),
            "--management-token-file" => args.management_token_file = Some(value().into()),
            "--seed" => flag_seeds.push(parse_seed(&value()).unwrap_or_else(|| usage())),
            "--allow-anonymous" => args.allow_anonymous = true,
            "--tls-listen" => args.tls_listen = Some(value()),
            "--tls-cert" => args.tls_cert = Some(value().into()),
            "--tls-key" => args.tls_key = Some(value().into()),
            "--tls-client-ca" => args.tls_client_ca = Some(value().into()),
            "--help" | "-h" => usage(),
            other => {
                eprintln!("unknown argument: {other}");
                usage();
            }
        }
    }
    if !flag_seeds.is_empty() {
        args.seeds = flag_seeds;
    }
    args
}

/// `hash-password`: stdin password → RFC 5803 verifier line on stdout.
fn hash_password(argv: impl Iterator<Item = String>) -> ! {
    use ramqp_core::sasl::scram::ScramMechanism;
    let mut mechanism = ScramMechanism::Sha256;
    let mut iterations = ramqp_broker::StaticScram::ITERATIONS;
    let mut argv = argv;
    while let Some(arg) = argv.next() {
        let mut value = || argv.next().unwrap_or_else(|| usage());
        match arg.as_str() {
            "--mechanism" => {
                mechanism = match value().to_ascii_uppercase().as_str() {
                    "SCRAM-SHA-1" => ScramMechanism::Sha1,
                    "SCRAM-SHA-256" => ScramMechanism::Sha256,
                    "SCRAM-SHA-512" => ScramMechanism::Sha512,
                    other => fail(format!("unknown mechanism {other:?}")),
                }
            }
            "--iterations" => {
                iterations = value().parse().unwrap_or_else(|_| usage());
                if iterations < 4096 {
                    fail("iterations must be at least 4096 (RFC 7677)");
                }
            }
            _ => usage(),
        }
    }
    let mut password = String::new();
    if std::io::stdin().read_line(&mut password).is_err() {
        fail("could not read the password from stdin");
    }
    let password = password.trim_end_matches(['\n', '\r']);
    if password.is_empty() {
        fail("empty password");
    }
    let verifier = ramqp_broker::auth::derive_scram_verifier(mechanism, password, iterations)
        .unwrap_or_else(|| fail("password rejected by SASLprep"));
    println!(
        "{}",
        ramqp_broker::auth::format_scram_verifier(mechanism, &verifier)
    );
    std::process::exit(0);
}

#[tokio::main]
async fn main() -> std::io::Result<()> {
    let mut argv = std::env::args().skip(1).peekable();
    if argv.peek().map(String::as_str) == Some("hash-password") {
        argv.next();
        hash_password(argv);
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    if cfg!(feature = "fips") {
        if !ramqp_broker::fips_enabled() {
            fail("this is a `fips` build but the AWS-LC FIPS module is not active");
        }
        tracing::info!("FIPS build: TLS and SCRAM run on the AWS-LC FIPS 140-3 module");
    }
    let args = parse_args(argv);
    let file = match &args.config {
        Some(path) => FileConfig::load(path).unwrap_or_else(|e| fail(e)),
        None => FileConfig::default(),
    };

    let mut config = BrokerConfig::default();
    file.apply(&mut config).unwrap_or_else(|e| fail(e));

    // Cluster: flags/env (all three together) override the file section.
    match (&args.node_id, &args.cluster_listen, args.seeds.is_empty()) {
        (Some(node_id), Some(listen), false) => {
            if !args.seeds.iter().any(|(id, _)| id == node_id) {
                fail(format!(
                    "--node-id {node_id} must appear in the --seed list"
                ));
            }
            // Flags override membership; replicas and fabric TLS still come
            // from the file's [cluster] section.
            let previous = config.cluster.take();
            let mut member = ClusterMemberConfig::new(*node_id, listen.clone(), args.seeds.clone());
            if let Some(prev) = previous {
                member.replicas = prev.replicas;
                member.tls = prev.tls;
            }
            config.cluster = Some(member);
        }
        (None, None, true) => {} // file section (if any) or standalone
        _ => fail("clustering needs all of --node-id, --cluster-listen, and --seed(s)"),
    }

    if let Some(d) = &args.data_dir {
        config.data_dir = Some(d.clone());
    }
    if let Some(m) = &args.management_listen {
        config.management_listen = Some(m.clone());
    }
    if let Some(f) = &args.management_token_file {
        config.management_token =
            Some(ramqp_broker::file_config::read_token(f).unwrap_or_else(|e| fail(e)));
    }
    match (&args.tls_cert, &args.tls_key) {
        (Some(cert), Some(key)) => {
            let mut tls = ramqp_broker::TlsServerConfig::new(cert.clone(), key.clone());
            tls.client_ca_file = args
                .tls_client_ca
                .clone()
                .or_else(|| config.tls.as_ref().and_then(|t| t.client_ca_file.clone()));
            config.tls = Some(tls);
        }
        (None, None) => {}
        _ => fail("--tls-cert and --tls-key go together"),
    }
    if let Some(l) = &args.tls_listen {
        config.tls_listen = Some(l.clone());
    }
    if config.data_dir.is_some() && !cfg!(feature = "store-redb") {
        fail("a data dir is configured but this build lacks the `store-redb` feature");
    }
    // `listen = ""` / `--listen none` disables the plain listener (TLS-only).
    let listen = args
        .listen
        .clone()
        .or_else(|| file.listen.clone())
        .unwrap_or_else(|| "0.0.0.0:5672".to_owned());
    let listen = if listen == "none" {
        String::new()
    } else {
        listen
    };
    let allow_anonymous = args.allow_anonymous || file.allow_anonymous.unwrap_or(false);

    let users = {
        let mut f = file.clone();
        if allow_anonymous {
            f.allow_anonymous = Some(true);
        }
        f.authenticator().unwrap_or_else(|e| fail(e))
    };
    let clustered = config.cluster.is_some();
    let mut broker = Broker::new(config);
    let user_count = users.as_ref().map_or(0, |u| u.user_count());
    if let Some(users) = users {
        broker = broker.with_authenticator(Arc::new(users) as Arc<dyn Authenticator>);
    }
    let bound = broker.bind(&listen).await?;

    // Open by accident is not an option: a public bind with no users needs
    // an explicit opt-in.
    let public = std::iter::once(bound.local_addr())
        .chain(bound.tls_local_addr())
        .find(|a| !a.ip().is_loopback());
    if let Some(addr) = public
        && user_count == 0
    {
        if !allow_anonymous {
            fail(format!(
                "refusing to listen on non-loopback {addr} with no users configured; \
                 configure [[users]] in --config, or pass --allow-anonymous to run open"
            ));
        }
        tracing::warn!(
            %addr,
            "listening on a non-loopback address with anonymous access allowed — \
             anyone who can reach this port can use the broker"
        );
    }
    if clustered {
        tracing::info!(node_id = ?args.node_id, "cluster member starting");
    }

    // Signals first, so a stop request during the startup declarations
    // below (which can wait on cluster formation) still ends the process.
    let serving = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let shutdown = bound.shutdown_handle();
    {
        let serving = serving.clone();
        tokio::spawn(async move {
            let ctrl_c = tokio::signal::ctrl_c();
            #[cfg(unix)]
            {
                let mut term =
                    tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                        .expect("SIGTERM handler");
                tokio::select! {
                    _ = ctrl_c => {}
                    _ = term.recv() => {}
                }
            }
            #[cfg(not(unix))]
            let _ = ctrl_c.await;
            tracing::info!("shutdown signal received; shutting down");
            if !serving.load(std::sync::atomic::Ordering::Acquire) {
                std::process::exit(0);
            }
            shutdown.shutdown();
        });
    }

    // Startup topology — exchanges, then queues, then bindings — is in place
    // BEFORE the listener starts accepting, so no client sees a missing
    // exchange or publishes ahead of its bindings. All idempotent across
    // restarts. Transient unavailability (cluster still forming, store lock
    // lingering) is retried; anything else is a configuration error and
    // stops the daemon.
    declare_topology(bound.broker(), &file).await;
    serving.store(true, std::sync::atomic::Ordering::Release);

    bound.run().await
}

/// Declare the config file's exchanges, queues, and bindings, retrying only
/// transient failures. A permanent one (e.g. an exchange whose type changed
/// while the old definition is persisted) exits with a clear message.
async fn declare_topology(broker: &Broker, file: &FileConfig) {
    use ramqp_broker::ExchangeError;
    enum Outcome {
        Done,
        Retry(String),
        Fatal(String),
    }
    async fn retry<F, Fut>(what: String, mut op: F)
    where
        F: FnMut() -> Fut,
        Fut: std::future::Future<Output = Outcome>,
    {
        let mut delay = std::time::Duration::from_millis(200);
        loop {
            match op().await {
                Outcome::Done => {
                    tracing::info!(%what, "declared");
                    return;
                }
                Outcome::Retry(e) => {
                    tracing::warn!(%what, error = %e, "declare not possible yet; retrying");
                    tokio::time::sleep(delay).await;
                    delay = (delay * 2).min(std::time::Duration::from_secs(10));
                }
                Outcome::Fatal(e) => fail(format!("{what}: {e}")),
            }
        }
    }
    let exchange_outcome = |r: Result<bool, ExchangeError>| match r {
        Ok(_) => Outcome::Done,
        Err(ExchangeError::Unavailable(e)) => Outcome::Retry(e),
        Err(e) => Outcome::Fatal(e.to_string()),
    };
    for e in &file.exchanges {
        retry(format!("exchange {:?}", e.name), || async {
            exchange_outcome(
                broker
                    .declare_exchange(&e.vhost, &e.name, e.kind(), e.alternate.as_deref())
                    .await,
            )
        })
        .await;
    }
    for q in &file.queues {
        // Addresses were validated at load; a failure here is the cluster,
        // store, or a limit — all worth retrying.
        retry(format!("queue {}", q.address), || async {
            match broker.declare_queue(&q.vhost, &q.address).await {
                Ok(()) => Outcome::Done,
                Err(e) => Outcome::Retry(e),
            }
        })
        .await;
    }
    for b in &file.bindings {
        retry(
            format!("binding {} -> {} ({:?})", b.exchange, b.queue, b.key),
            || async {
                exchange_outcome(
                    broker
                        .bind_queue(&b.vhost, &b.exchange, &b.queue, &b.key)
                        .await,
                )
            },
        )
        .await;
    }
}
