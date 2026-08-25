#!/usr/bin/env bash
# Runs the full benchmark suite against a freshly built cluster.
#
#   ./scripts/bench.sh
#
# Every number this prints goes on the resume, so the script starts from a
# clean state each time and reports what hardware it ran on.
set -euo pipefail

BIN=${BIN:-/tmp/kvnode}
LOAD=${LOAD:-/tmp/loadgen}
DATA=${DATA:-/tmp/raftkv-bench}
DUR=${DUR:-5s}

go build -o "$BIN" ./cmd/kvnode
go build -o "$LOAD" ./cmd/loadgen

cleanup() { pkill -f "$BIN --id=" 2>/dev/null || true; }
trap cleanup EXIT
cleanup

sleep_ready() {
  for _ in $(seq 1 400); do
    if curl -sf "$1/health" >/dev/null 2>&1; then return 0; fi
  done
  echo "node at $1 never became ready" >&2
  return 1
}

start_node() { # id, peers
  local id=$1 peers=$2
  "$BIN" --id="$id" --addr=":800$id" --raft-addr="localhost:900$id" \
         --data="$DATA/n$id" --peers="$peers" >/dev/null 2>"$DATA/n$id.log" &
}

find_leader() {
  for p in 8001 8002 8003; do
    if curl -s -m 1 "localhost:$p/status" 2>/dev/null | grep -q role=leader; then
      echo "$p"; return
    fi
  done
}

echo "raftkv benchmarks"
echo "hardware: $(sysctl -n machdep.cpu.brand_string 2>/dev/null || uname -m), $(uname -sr)"
echo "duration per test: $DUR"
echo

# ---------------------------------------------------------------- 1 node
rm -rf "$DATA"; mkdir -p "$DATA"
start_node 1 ""
sleep_ready http://localhost:8001
# Wait for the node to elect itself before measuring, or the first ~200ms of
# 503s (no leader yet) pollute the error count.
for _ in $(seq 1 400); do
  if [ -n "$(find_leader)" ]; then break; fi
done
"$LOAD" --mode=write --addr=http://localhost:8001 --workers=1  --duration="$DUR" --label="write 1-node w=1"
"$LOAD" --mode=write --addr=http://localhost:8001 --workers=16 --duration="$DUR" --label="write 1-node w=16"
cleanup

# ---------------------------------------------------------------- 3 nodes
rm -rf "$DATA"; mkdir -p "$DATA"
start_node 1 "2=localhost:9002,3=localhost:9003"
start_node 2 "1=localhost:9001,3=localhost:9003"
start_node 3 "1=localhost:9001,2=localhost:9002"
for p in 8001 8002 8003; do sleep_ready "http://localhost:$p"; done

# NOTE: with `set -e`, `[ -n "$L" ] && break` exits the script when the test
# is false, because the AND-list returns 1. Use an explicit if.
L=""
for _ in $(seq 1 400); do
  L=$(find_leader)
  if [ -n "$L" ]; then break; fi
done
if [ -z "$L" ]; then echo "no leader was elected" >&2; exit 1; fi

FOLLOWER=""
for p in 8001 8002 8003; do
  if [ "$p" != "$L" ]; then FOLLOWER=$p; break; fi
done
echo "leader on :$L, follower on :$FOLLOWER"
echo

"$LOAD" --mode=write --addr="http://localhost:$L" --workers=1  --duration="$DUR" --label="write 3-node w=1"
"$LOAD" --mode=write --addr="http://localhost:$L" --workers=16 --duration="$DUR" --label="write 3-node w=16"
"$LOAD" --mode=write --addr="http://localhost:$FOLLOWER" --workers=16 --duration="$DUR" --label="write via follower w=16"
echo
"$LOAD" --mode=read --addr="http://localhost:$L"        --consistency=strong   --workers=16 --duration="$DUR" --label="read leader strong"
"$LOAD" --mode=read --addr="http://localhost:$FOLLOWER" --consistency=strong   --workers=16 --duration="$DUR" --label="read follower strong"
"$LOAD" --mode=read --addr="http://localhost:$FOLLOWER" --consistency=eventual --workers=16 --duration="$DUR" --label="read follower eventual"
echo
"$LOAD" --mode=staleness --addr="http://localhost:$L" --follower="http://localhost:$FOLLOWER" --samples=300

# ------------------------------------------------------- failover under load
echo
echo "killing the leader 3s into a 10s write load..."
LID=$(curl -s "localhost:$L/status" | sed 's/id=\([0-9]*\).*/\1/')
( sleep 3; pkill -9 -f "$BIN --id=$LID " ) &
"$LOAD" --mode=availability --addr="http://localhost:$FOLLOWER" --workers=4 --duration=10s
