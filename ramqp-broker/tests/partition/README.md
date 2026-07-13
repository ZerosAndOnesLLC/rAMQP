# Process-level partition tests

Jepsen-style split-brain against real broker **processes** across real Linux
**network namespaces** — the process-level half of broker.md Phase 10's HA
fault-injection axis. The in-process leg (leader kills, rolling kills to the
availability boundary, follower-loss transparency) lives in
[`tests/cluster.rs`](../cluster.rs); this one adds true network partitions.

## What it asserts

A 3-node quorum cluster (one `ramqp-brokerd` per namespace, bridged on
`10.42.0.0/24`) is partitioned into a majority `{n1,n2}` and a minority `{n3}`
via `iptables` DROP rules inside the minority's namespace. Then:

- **majority stays available** — publishes to `n1` are still accepted (2/3 quorum),
- **minority refuses** — publishes to `n3` are refused/never accepted (the
  silent-loss guard: an accepted publish without quorum would fail the test),
- **no loss on heal** — after the partition clears, every committed message is
  still consumable.

## Running

```sh
ramqp-broker/tests/partition/run.sh
```

Run it as your normal user: it builds `ramqp-brokerd` + the `partition_probe`
example, then re-execs itself under `sudo` (it needs root for network
namespaces, veth pairs, and `iptables`). Everything is torn down on exit
(namespaces, bridge, broker processes) via an `EXIT` trap. CI runs the same
script in the `partition` job.

The client workload is driven by [`examples/partition_probe.rs`](../../examples/partition_probe.rs),
invoked per node; its exit code reports the outcome (`expect-accept` /
`expect-refused` / `consume`).

> Note: the clustered nodes run without on-disk Raft persistence (no
> `store-redb`/`data_dir`), so this exercises **partition availability**, not
> node-restart durability — restart-durability is covered by the `store-redb`
> durable suite.
