#!/usr/bin/env bash
# Process-level network-partition test for ramqp-broker's quorum queues — the
# Jepsen-style split-brain leg of broker.md Phase 10, run against real broker
# PROCESSES across real network namespaces (not the in-process fault injection
# in tests/cluster.rs).
#
# Topology: a Linux bridge (10.42.0.254/24) with three network namespaces
# ns1..ns3 (10.42.0.1..3), one ramqp-brokerd per namespace forming a 3-node
# quorum cluster. We then iptables-partition ns3 (the minority) away from ns1/ns2
# and assert CP behavior:
#   - majority {n1,n2} keeps ACCEPTING publishes (2/3 quorum),
#   - minority {n3} REFUSES publishes (never silently accepts — the loss guard),
#   - after healing, every committed message is still there (no loss).
#
# Needs root (network namespaces + iptables). Run it as your normal user; it
# builds the binaries, then re-execs itself under `sudo -E`:
#
#   ramqp-broker/tests/partition/run.sh
#
# CI runs the same script in the `partition` job.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../../.." && pwd)" # partition -> tests -> ramqp-broker -> root
BRK="$ROOT/target/debug/ramqp-brokerd"
PROBE="$ROOT/target/debug/examples/partition_probe"

# Phase 1 (unprivileged): build, then re-exec under sudo.
if [ "$(id -u)" -ne 0 ]; then
    echo ">> building brokerd + partition_probe ..."
    cargo build -p ramqp-broker --bin ramqp-brokerd --example partition_probe
    echo ">> re-executing under sudo for network-namespace setup ..."
    exec sudo "$0" "$@"
fi

# Phase 2 (root): the actual test.
SUBNET="10.42.0"
BRIDGE="br-ramqp"
QUEUE="/quorum/partition"
LOGDIR="$(mktemp -d)"
declare -a PIDS=()

log() { echo ">> $*"; }
fail() {
    echo "!! PARTITION TEST FAILED: $*" >&2
    exit 1
}

cleanup() {
    set +e
    for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null; done
    # brokerd runs as a child of `ip netns exec`; make sure none survive.
    pkill -f "$BRK" 2>/dev/null
    for i in 1 2 3; do
        ip netns pids "ns$i" 2>/dev/null | xargs -r kill 2>/dev/null
        ip netns del "ns$i" 2>/dev/null
        ip link del "veth$i-br" 2>/dev/null
    done
    ip link del "$BRIDGE" 2>/dev/null
    rm -rf "$LOGDIR"
}
trap cleanup EXIT

# --- topology ------------------------------------------------------------
log "creating bridge $BRIDGE and namespaces ns1..ns3 ..."
ip link add "$BRIDGE" type bridge
ip addr add "$SUBNET.254/24" dev "$BRIDGE"
ip link set "$BRIDGE" up
for i in 1 2 3; do
    ip netns add "ns$i"
    ip link add "veth$i" type veth peer name "veth$i-br"
    ip link set "veth$i" netns "ns$i"
    ip link set "veth$i-br" master "$BRIDGE"
    ip link set "veth$i-br" up
    ip netns exec "ns$i" ip addr add "$SUBNET.$i/24" dev "veth$i"
    ip netns exec "ns$i" ip link set "veth$i" up
    ip netns exec "ns$i" ip link set lo up
done

# --- start the cluster ---------------------------------------------------
SEEDS="1=$SUBNET.1:7472,2=$SUBNET.2:7472,3=$SUBNET.3:7472"
log "starting a ramqp-brokerd in each namespace ..."
for i in 1 2 3; do
    ip netns exec "ns$i" env \
        RAMQP_NODE_ID="$i" \
        RAMQP_LISTEN="$SUBNET.$i:5672" \
        RAMQP_CLUSTER_LISTEN="$SUBNET.$i:7472" \
        RAMQP_SEEDS="$SEEDS" \
        "$BRK" >"$LOGDIR/node$i.log" 2>&1 &
    PIDS+=($!)
done

# --- readiness: retry a seed publish until the cluster serves ------------
probe() { "$PROBE" "$@"; }
log "waiting for the cluster to form (seed publishes to n1) ..."
ready=0
for _ in $(seq 1 20); do
    if probe "amqp://$SUBNET.1:5672" "$QUEUE" expect-accept 5 >/dev/null 2>&1; then
        ready=1
        break
    fi
    sleep 1
done
[ "$ready" -eq 1 ] || { cat "$LOGDIR"/node*.log; fail "cluster never accepted the seed publishes"; }
log "cluster healthy — 5 messages committed."

# --- partition the minority (ns3) ---------------------------------------
log "partitioning ns3 away from ns1/ns2 (iptables DROP) ..."
for peer in 1 2; do
    ip netns exec ns3 iptables -A INPUT -s "$SUBNET.$peer" -j DROP
    ip netns exec ns3 iptables -A OUTPUT -d "$SUBNET.$peer" -j DROP
done

log "letting Raft notice the partition ..."
sleep 6

# --- majority must keep accepting ---------------------------------------
log "majority check: publishing to n1 (expect ACCEPT) ..."
probe "amqp://$SUBNET.1:5672" "$QUEUE" expect-accept 3 \
    || fail "majority {n1,n2} refused a publish it should have accepted"
log "majority accepted 3 more (8 committed total)."

# --- minority must refuse, never silently accept ------------------------
log "minority check: publishing to n3 (expect REFUSED, never accepted) ..."
probe "amqp://$SUBNET.3:5672" "$QUEUE" expect-refused 3 \
    || fail "minority {n3} accepted a publish without quorum — silent-loss hazard"
log "minority refused cleanly."

# --- heal and verify no committed message was lost ----------------------
log "healing the partition ..."
ip netns exec ns3 iptables -F
sleep 6

log "consuming from n1 to check for loss ..."
out="$(probe "amqp://$SUBNET.1:5672" "$QUEUE" consume 20)" || fail "consume probe failed"
got="$(echo "$out" | sed -n 's/^CONSUMED \([0-9]*\)$/\1/p')"
[ -n "$got" ] || fail "could not parse consume output: $out"
log "consumed $got messages (expected >= 8 committed)."
[ "$got" -ge 8 ] || fail "message loss: only $got of the 8 committed messages survived"

echo
echo "PARTITION TEST PASSED: majority available, minority refused, no committed-message loss."
