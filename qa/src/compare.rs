//! Compare benchmark results against a baseline.
//!
//! Both files are JSON arrays of `bench` results, matched by `name`. A
//! scenario regresses when its throughput drops by more than
//! `--max-throughput-drop` percent, or its p99 latency rises by more than
//! `--max-p99-rise` percent *and* by more than `--p99-floor-us` (tiny
//! latencies are noisy in relative terms). Scenarios missing from the
//! baseline are reported, not failed.

use std::path::PathBuf;

use serde_json::Value;

#[derive(clap::Args)]
pub struct Args {
    #[arg(long)]
    baseline: PathBuf,
    #[arg(long)]
    current: PathBuf,
    #[arg(long, default_value_t = 15.0)]
    max_throughput_drop: f64,
    #[arg(long, default_value_t = 50.0)]
    max_p99_rise: f64,
    #[arg(long, default_value_t = 500.0)]
    p99_floor_us: f64,
}

fn load(path: &PathBuf) -> Result<Vec<Value>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("{}: {e}", path.display()))?;
    match serde_json::from_str::<Value>(&text).map_err(|e| format!("{}: {e}", path.display()))? {
        Value::Array(v) => Ok(v),
        _ => Err(format!(
            "{}: expected a JSON array of results",
            path.display()
        )),
    }
}

pub fn run(a: Args) -> bool {
    let (base, cur) = match (load(&a.baseline), load(&a.current)) {
        (Ok(b), Ok(c)) => (b, c),
        (Err(e), _) | (_, Err(e)) => {
            eprintln!("compare: {e}");
            return false;
        }
    };
    let num = |v: &Value, k: &str| v.get(k).and_then(Value::as_f64).unwrap_or(0.0);
    let p99 = |v: &Value| {
        v.get("latency_us")
            .and_then(|l| l.get("p99"))
            .and_then(Value::as_f64)
            .unwrap_or(0.0)
    };
    println!(
        "{:<28} {:>12} {:>12} {:>8} {:>10} {:>10} {:>8}  verdict",
        "scenario", "base msg/s", "now msg/s", "Δ%", "base p99", "now p99", "Δ%"
    );
    let mut ok = true;
    for c in &cur {
        let name = c.get("name").and_then(Value::as_str).unwrap_or("?");
        let Some(b) = base
            .iter()
            .find(|b| b.get("name").and_then(Value::as_str) == Some(name))
        else {
            println!(
                "{name:<28} {:>12} {:>12.0}  (no baseline)",
                "-",
                num(c, "msgs_per_sec")
            );
            continue;
        };
        let (bt, ct) = (num(b, "msgs_per_sec"), num(c, "msgs_per_sec"));
        let (bp, cp) = (p99(b), p99(c));
        let dt = if bt > 0.0 {
            (ct - bt) / bt * 100.0
        } else {
            0.0
        };
        let dp = if bp > 0.0 {
            (cp - bp) / bp * 100.0
        } else {
            0.0
        };
        let mut verdict = Vec::new();
        if -dt > a.max_throughput_drop {
            verdict.push("THROUGHPUT REGRESSION");
        }
        if dp > a.max_p99_rise && cp - bp > a.p99_floor_us {
            verdict.push("LATENCY REGRESSION");
        }
        if !verdict.is_empty() {
            ok = false;
        }
        println!(
            "{name:<28} {bt:>12.0} {ct:>12.0} {dt:>+8.1} {bp:>10.0} {cp:>10.0} {dp:>+8.1}  {}",
            if verdict.is_empty() {
                "ok".to_string()
            } else {
                verdict.join(", ")
            }
        );
    }
    ok
}
