#!/usr/bin/env bash
# Stage 3 — chaos / fault injection against real broker PROCESSES (kill -9,
# cold restart from disk — stronger than the in-process suite's graceful stops).
#
# Part A: a 3-node quorum cluster (on-disk Raft via store-redb + --data-dir).
#   A verifying client is pinned to node 1 (kept alive) while nodes 2 and 3 are
#   killed and restarted one at a time (quorum 2/3 always held). Contract: every
#   ACCEPTED message is eventually delivered — zero loss across failovers.
# Part B: single-node durable crash recovery — produce N, SIGKILL, cold start on
#   the same data dir, and every durable message must still be there.
#
# Knobs: RAMQP_CHAOS_ROUNDS (4) RAMQP_CHAOS_N (20000) RAMQP_RECOVER_N (5000).

source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

section "stage 3: chaos / fault injection"

ROUNDS="${RAMQP_CHAOS_ROUNDS:-4}"
N="${RAMQP_CHAOS_N:-20000}"
RN="${RAMQP_RECOVER_N:-5000}"

build_brokerd store-redb >/dev/null # on-disk Raft is required to survive restart
info "building chaos + recover drivers"
cargo build --release -q -p ramqp-broker --features store-redb --example chaos --example recover
CHAOS="$ROOT/target/release/examples/chaos"
RECOVER="$ROOT/target/release/examples/recover"

# ---------------------------------------------------------------------------
# Part A: rolling kills against a 3-node quorum cluster.
# ---------------------------------------------------------------------------
section "part A: 3-node cluster, rolling leader/follower kills ($ROUNDS rounds)"

declare -A AMQP FAB DDIR PID
for i in 1 2 3; do
  AMQP[$i]="$(free_port)"
  FAB[$i]="$(free_port)"
  DDIR[$i]="$RAMQP_OUT/chaos-node$i"
  mkdir -p "${DDIR[$i]}"
done
SEEDS=(--seed "1=127.0.0.1:${FAB[1]}" --seed "2=127.0.0.1:${FAB[2]}" --seed "3=127.0.0.1:${FAB[3]}")

start_node() {
  local i="$1"
  spawn_brokerd "chaos-node$i" -- \
    --listen "127.0.0.1:${AMQP[$i]}" \
    --node-id "$i" --cluster-listen "127.0.0.1:${FAB[$i]}" \
    "${SEEDS[@]}" --data-dir "${DDIR[$i]}"
  PID[$i]="$BROKERD_PID"
}

for i in 1 2 3; do start_node "$i"; done
for i in 1 2 3; do
  if ! wait_port "${AMQP[$i]}" 30; then
    fail "chaos: node $i never came up"; finish; exit 1
  fi
done
ok "3-node cluster up (AMQP ${AMQP[1]},${AMQP[2]},${AMQP[3]})"
sleep 5 # allow cluster formation before the client declares the quorum queue

# Verifying client pinned to node 1 (never killed), running in the background.
CLIENTLOG="$RAMQP_OUT/chaos-client.log"
CHAOS_PRODUCER_URL="amqp://127.0.0.1:${AMQP[1]}" \
CHAOS_CONSUMER_URL="amqp://127.0.0.1:${AMQP[1]}" \
CHAOS_QUEUE="/quorum/chaos" CHAOS_N="$N" \
CHAOS_DEADLINE_SECS=$((ROUNDS * 30 + 150)) \
  "$CHAOS" >"$CLIENTLOG" 2>&1 &
chaos_pid=$!

# Rolling kill/restart of nodes 2 and 3 only (node 1 stays for the client;
# never more than one down at a time, so quorum is always held).
victims=(2 3)
for ((r = 0; r < ROUNDS; r++)); do
  v="${victims[$((r % 2))]}"
  info "round $r: SIGKILL node $v (pid ${PID[$v]})"
  kill -9 "${PID[$v]}" 2>/dev/null || true
  untrack_pid "${PID[$v]}"
  wait_port_down "${AMQP[$v]}" 15 || warn "node $v port still up after kill"
  sleep 4 # let the survivors re-elect / heal while the node is down
  info "round $r: cold-restart node $v"
  start_node "$v"
  wait_port "${AMQP[$v]}" 30 || warn "node $v did not rebind :${AMQP[$v]}"
  sleep 6 # let it rejoin + catch up before the next round touches the other node
  kill -0 "$chaos_pid" 2>/dev/null || { info "chaos client finished early"; break; }
done

info "kill rounds done; waiting for the verifying client to finish"
if wait "$chaos_pid"; then
  pass "chaos: zero accepted-message loss across $ROUNDS kill/restart rounds"
else
  rc=$?
  fail "chaos: verifying client reported failure (rc=$rc) — see chaos-client.log"
  tail -8 "$CLIENTLOG" | sed 's/^/    /' >&2 || true
fi
grep -E 'result:|PASS|FAIL' "$CLIENTLOG" | sed 's/^/    /' || true
check "chaos: no broker panics (cluster nodes)" no_panics chaos-node1 chaos-node2 chaos-node3

# Tear down the cluster before part B.
for i in 1 2 3; do kill "${PID[$i]}" 2>/dev/null || true; untrack_pid "${PID[$i]}"; done
sleep 1

# ---------------------------------------------------------------------------
# Part B: single-node durable crash recovery (kill -9 + cold start).
# ---------------------------------------------------------------------------
section "part B: durable crash recovery (produce → kill -9 → cold start → verify)"

BPORT="$(free_port)"
BDIR="$RAMQP_OUT/recover-node"
mkdir -p "$BDIR"

spawn_brokerd recover-broker -- --listen "127.0.0.1:$BPORT" --data-dir "$BDIR"
if ! wait_port "$BPORT" 30; then fail "recover: broker never came up"; finish; exit 1; fi

info "producing $RN durable messages"
if RECOVER_URL="amqp://127.0.0.1:$BPORT" RECOVER_ADDRESS="/durable/recovery" \
   RECOVER_N="$RN" RECOVER_PHASE=produce "$RECOVER"; then
  ok "durable produce confirmed"
else
  fail "recover: durable produce phase failed"; finish; exit 1
fi

info "SIGKILL the broker (simulated crash)"
kill -9 "$BROKERD_PID" 2>/dev/null || true
untrack_pid "$BROKERD_PID"
wait_port_down "$BPORT" 15 || warn "recover: port still up after kill"

info "cold-starting a fresh process on the same data dir"
spawn_brokerd recover-broker -- --listen "127.0.0.1:$BPORT" --data-dir "$BDIR"
if ! wait_port "$BPORT" 30; then fail "recover: broker did not restart"; finish; exit 1; fi

if RECOVER_URL="amqp://127.0.0.1:$BPORT" RECOVER_ADDRESS="/durable/recovery" \
   RECOVER_N="$RN" RECOVER_PHASE=consume "$RECOVER"; then
  pass "durable: all $RN messages survived kill -9 + cold start"
else
  fail "durable: crash recovery lost messages"
fi
check "durable: no broker panics on recovery" no_panics recover-broker

finish
