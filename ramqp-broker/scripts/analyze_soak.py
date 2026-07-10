#!/usr/bin/env python3
"""Summarize a soak run into KEY=VALUE lines the soak script evals.

Reads the RSS sample file (`<epoch>\\t<kib>` per line) and the loadgen log
(`... rate=<n> msg/s ...`), then reports early-vs-late medians so the caller can
assert on memory growth (leak) and throughput decay. Emits nothing but the
KEY=VALUE lines on stdout.
"""
import re
import statistics
import sys


def median(xs):
    return statistics.median(xs) if xs else 0


def window(xs, lo, hi):
    n = len(xs)
    if n == 0:
        return []
    a, b = int(n * lo), int(n * hi)
    return xs[a:max(b, a + 1)]


def main():
    rss_file, load_log = sys.argv[1], sys.argv[2]

    rss = []
    with open(rss_file) as f:
        for line in f:
            parts = line.split()
            if len(parts) == 2 and parts[1].isdigit():
                rss.append(int(parts[1]))

    rates = []
    with open(load_log) as f:
        for line in f:
            m = re.search(r"rate=(\d+)", line)
            if m:
                rates.append(int(m.group(1)))

    # Discard the first RSS decile (warm-up allocation ramp) before measuring.
    rss_early = median(window(rss, 0.10, 0.35))
    rss_late = median(window(rss, 0.75, 1.00))
    rate_early = median(window(rates, 0.00, 0.34))
    rate_late = median(window(rates, 0.66, 1.00))

    print(f"RSS_EARLY_KIB={int(rss_early)}")
    print(f"RSS_LATE_KIB={int(rss_late)}")
    print(f"RSS_LEAK_KIB={int(rss_late - rss_early)}")
    print(f"RSS_PEAK_KIB={int(max(rss) if rss else 0)}")
    print(f"RATE_EARLY={int(rate_early)}")
    print(f"RATE_LATE={int(rate_late)}")
    print(f"SAMPLES_RSS={len(rss)}")
    print(f"SAMPLES_RATE={len(rates)}")


if __name__ == "__main__":
    main()
