//! Sample a process's resident memory, open file descriptors and threads
//! (Linux `/proc`), and fail on growth after warm-up — the signature of a
//! leak in a soak run.
//!
//! The first `--warmup-pct` of the run is excluded (caches, pools and
//! allocator arenas fill up first). The rest is split in two halves, and
//! growth is measured between their low points (10th percentile): memory
//! that a queue's log or a batch legitimately holds for a while and then
//! releases makes a sawtooth whose low points stay level, while a leak
//! raises them. Open file descriptors use the halves' medians.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use serde_json::json;

use crate::common::write_json;

#[derive(clap::Args)]
pub struct Args {
    /// Process to watch.
    #[arg(long)]
    pid: u32,
    #[arg(long, default_value_t = 60)]
    secs: u64,
    #[arg(long, default_value_t = 1000)]
    interval_ms: u64,
    #[arg(long, default_value_t = 25.0)]
    warmup_pct: f64,
    /// Fail if resident memory grows by more than this percent.
    #[arg(long, default_value_t = 25.0)]
    max_rss_growth_pct: f64,
    /// Fail if open file descriptors grow by more than this.
    #[arg(long, default_value_t = 32)]
    max_fd_growth: i64,
    /// Write every sample here as CSV.
    #[arg(long)]
    csv: Option<PathBuf>,
    #[arg(long)]
    json: Option<PathBuf>,
}

#[derive(Clone, Copy)]
struct Sample {
    t: f64,
    rss_kib: i64,
    fds: i64,
    threads: i64,
}

fn sample(pid: u32, t: f64) -> Option<Sample> {
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let field = |name: &str| {
        status
            .lines()
            .find(|l| l.starts_with(name))
            .and_then(|l| l.split_whitespace().nth(1))
            .and_then(|v| v.parse::<i64>().ok())
            .unwrap_or(0)
    };
    let fds = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count() as i64;
    Some(Sample {
        t,
        rss_kib: field("VmRSS:"),
        fds,
        threads: field("Threads:"),
    })
}

/// The `p` quantile (0.0..=1.0) of `v`.
fn quantile(mut v: Vec<i64>, p: f64) -> i64 {
    if v.is_empty() {
        return 0;
    }
    v.sort_unstable();
    v[((v.len() - 1) as f64 * p).round() as usize]
}

pub async fn run(a: Args) -> bool {
    let start = Instant::now();
    let end = start + Duration::from_secs(a.secs);
    let mut samples = Vec::new();
    while Instant::now() < end {
        match sample(a.pid, start.elapsed().as_secs_f64()) {
            Some(s) => samples.push(s),
            None => {
                eprintln!("resources: process {} is gone", a.pid);
                break;
            }
        }
        tokio::time::sleep(Duration::from_millis(a.interval_ms)).await;
    }
    if let Some(path) = &a.csv {
        let mut out = String::from("secs,rss_kib,fds,threads\n");
        for s in &samples {
            out.push_str(&format!(
                "{:.1},{},{},{}\n",
                s.t, s.rss_kib, s.fds, s.threads
            ));
        }
        let _ = std::fs::write(path, out);
    }
    if samples.len() < 8 {
        eprintln!("resources: too few samples ({})", samples.len());
        return false;
    }
    let warm = ((samples.len() as f64) * a.warmup_pct / 100.0) as usize;
    let mid = warm + (samples.len() - warm) / 2;
    let (early, late) = (&samples[warm..mid], &samples[mid..]);
    let rss0 = quantile(early.iter().map(|s| s.rss_kib).collect(), 0.1);
    let rss1 = quantile(late.iter().map(|s| s.rss_kib).collect(), 0.1);
    let fd0 = quantile(early.iter().map(|s| s.fds).collect(), 0.5);
    let fd1 = quantile(late.iter().map(|s| s.fds).collect(), 0.5);
    let rss_growth = if rss0 > 0 {
        (rss1 - rss0) as f64 / rss0 as f64 * 100.0
    } else {
        0.0
    };
    let mut failures = Vec::new();
    if rss_growth > a.max_rss_growth_pct {
        failures.push(format!(
            "resident memory's low point grew {rss_growth:.1}% after warm-up ({rss0} → {rss1} KiB)"
        ));
    }
    if fd1 - fd0 > a.max_fd_growth {
        failures.push(format!("open file descriptors grew {fd0} → {fd1}"));
    }
    let summary = json!({
        "pid": a.pid,
        "samples": samples.len(),
        "rss_kib_low_first_half": rss0,
        "rss_kib_low_second_half": rss1,
        "rss_kib_peak": samples.iter().map(|s| s.rss_kib).max(),
        "rss_growth_pct": rss_growth,
        "fds_after_warmup": fd0,
        "fds_end": fd1,
        "threads_end": samples.last().map(|s| s.threads),
        "failures": failures,
        "pass": failures.is_empty(),
    });
    println!(
        "{}",
        serde_json::to_string_pretty(&summary).unwrap_or_default()
    );
    write_json(a.json.as_deref(), &summary);
    failures.is_empty()
}
