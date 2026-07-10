#!/usr/bin/env python3
"""Parse `latency`-bin output into median metrics; optionally diff a baseline.

Usage:
  bench_stats.py parse   <trial.log> [<trial.log> ...] > metrics.json
  bench_stats.py compare <metrics.json> <baseline.json>

`compare` prints a table and exits non-zero if any metric regressed beyond its
tolerance (latency worse if higher, throughput worse if lower). This is a manual
aid — it never runs in CI — so a non-zero exit just paints the drift red for a
human, it gates nothing.
"""
import json
import re
import statistics
import sys

# metric -> (regex group parser). Latency line then throughput then RSS.
LAT_RE = re.compile(
    r"p50\s+([\d.]+)\s+p90\s+([\d.]+)\s+p99\s+([\d.]+)\s+p99\.9\s+([\d.]+)\s+max\s+([\d.]+)"
)
THR_RE = re.compile(r"=\s*(\d+)\s*msg/s")
RSS_RE = re.compile(r"RSS:\s*([\d.]+)\s*MiB")

# metric -> ("lower" means lower-is-better) , tolerance fraction
DIRECTION = {
    "p50_us": ("lower", 0.25),
    "p90_us": ("lower", 0.25),
    "p99_us": ("lower", 0.30),
    "p999_us": ("lower", 0.40),
    "max_us": ("lower", 0.60),
    "throughput_msgs": ("higher", 0.20),
    "rss_mib": ("lower", 0.30),
}


def parse(logs):
    cols = {k: [] for k in DIRECTION}
    for path in logs:
        with open(path) as f:
            text = f.read()
        for m in LAT_RE.finditer(text):
            p50, p90, p99, p999, mx = (float(x) for x in m.groups())
            cols["p50_us"].append(p50)
            cols["p90_us"].append(p90)
            cols["p99_us"].append(p99)
            cols["p999_us"].append(p999)
            cols["max_us"].append(mx)
        for m in THR_RE.finditer(text):
            cols["throughput_msgs"].append(float(m.group(1)))
        for m in RSS_RE.finditer(text):
            cols["rss_mib"].append(float(m.group(1)))
    return {k: round(statistics.median(v), 1) for k, v in cols.items() if v}


def compare(cur, base):
    regressed = 0
    hdr = f"{'metric':<18}{'baseline':>12}{'current':>12}{'delta%':>10}  verdict"
    print(hdr)
    print("-" * len(hdr))
    for k in DIRECTION:
        if k not in cur or k not in base:
            continue
        direction, tol = DIRECTION[k]
        b, c = base[k], cur[k]
        if b == 0:
            continue
        delta = (c - b) / b * 100.0
        if direction == "lower":
            bad = c > b * (1 + tol)
        else:
            bad = c < b * (1 - tol)
        verdict = "REGRESSED" if bad else ("improved" if (
            (direction == "lower" and c < b * 0.9) or
            (direction == "higher" and c > b * 1.1)) else "ok")
        if bad:
            regressed += 1
        print(f"{k:<18}{b:>12.1f}{c:>12.1f}{delta:>+9.1f}%  {verdict}")
    return regressed


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(2)
    mode = sys.argv[1]
    if mode == "parse":
        print(json.dumps(parse(sys.argv[2:]), indent=2))
    elif mode == "compare":
        cur = json.load(open(sys.argv[2]))
        base = json.load(open(sys.argv[3]))
        n = compare(cur, base)
        if n:
            print(f"\n{n} metric(s) regressed beyond tolerance.")
            sys.exit(1)
        print("\nno regressions beyond tolerance.")
    else:
        print(__doc__)
        sys.exit(2)


if __name__ == "__main__":
    main()
