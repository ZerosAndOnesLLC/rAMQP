//! `ramqp-qa`: the pre-release QA tool. `qa/prerelease.sh` drives it; each
//! subcommand also runs on its own against any AMQP 1.0 broker.

mod bench;
mod common;
mod compare;
mod diff;
mod proxy;
mod resources;
mod workload;

use clap::{Parser, Subcommand};

#[derive(Parser)]
#[command(
    name = "ramqp-qa",
    about = "Pre-release QA harness for ramqp and ramqp-broker"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Concurrent produce/consume with consumer churn, then check delivery
    /// guarantees: no acknowledged message lost, no accepted message
    /// redelivered, per-producer order kept.
    Workload(workload::Args),
    /// A TCP proxy that coalesces client frames, delays them, and resets
    /// connections — network trouble without root.
    Proxy(proxy::Args),
    /// Throughput and end-to-end latency percentiles for one scenario.
    Bench(bench::Args),
    /// Compare benchmark results against a baseline; fail on regressions.
    Compare(compare::Args),
    /// Run the same protocol scenarios against several brokers and report
    /// where they behave differently.
    Diff(diff::Args),
    /// Sample a process's memory, file descriptors and threads; fail on
    /// growth (a leak) after warm-up.
    Resources(resources::Args),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let ok = match cli.command {
        Command::Workload(a) => workload::run(a).await,
        Command::Proxy(a) => proxy::run(a).await,
        Command::Bench(a) => bench::run(a).await,
        Command::Compare(a) => compare::run(a),
        Command::Diff(a) => diff::run(a).await,
        Command::Resources(a) => resources::run(a).await,
    };
    std::process::exit(if ok { 0 } else { 1 });
}
