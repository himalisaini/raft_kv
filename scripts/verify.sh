#!/usr/bin/env bash
# End-to-end acceptance test: starts a real 3-node cluster, kills nodes, and
# checks the properties that define "correct" for this system.
#
#   ./scripts/verify.sh
#
# Every check prints PASS or FAIL. Exit code is non-zero if anything failed.
set -uo pipefail

BIN=${BIN:-/tmp/kvnode}
DATA=${DATA:-/tmp/raftkv-verify}
PASS=0; FAIL=0

green() { printf "\033[32m%s\033[0m" "$1"; }
red()   { printf "\033[31m%s\033[0m" "$1"; }

check() { # description, expected, actual
  if [ "$2" = "$3" ]; then
    printf "  [%s] %-52s %s\n" "$(green PASS)" "$1" "$3"; PASS=$((PASS+1))
  else
    printf "  [%s] %-52s got %-18s want %s\n" "$(red FAIL)" "$1" "$3" "$2"; FAIL=$((FAIL+1))
  fi
}

cleanup() { pkill -f "$BIN --id=" 2>/dev/null; sleep 0; }
trap cleanup EXIT

start_node() {
  local id=$1 peers=""
  for j in 1 2 3; do [ "$j" != "$id" ] && peers="$peers,$j=localhost:900$j"; done
  "$BIN" --id="$id" --addr=":800$id" --raft-addr="localhost:900$id" \
         --data="$DATA/n$id" --peers="${peers#,}" >/dev/null 2>>"$DATA/n$id.log" &
  # Detach the job so bash does not print "Killed: 9" when we kill -9 it.
  disown
}

ready() { for _ in $(seq 1 400); do if curl -sf "localhost:$1/health" >/dev/null 2>&1; then return 0; fi; done; return 1; }

leader_port() {
  for p in 8001 8002 8003; do
    if curl -s -m 1 "localhost:$p/status" 2>/dev/null | grep -q role=leader; then echo "$p"; return; fi
  done
}

count_leaders() {
  local n=0
  for p in 8001 8002 8003; do
    if curl -s -m 1 "localhost:$p/status" 2>/dev/null | grep -q role=leader; then n=$((n+1)); fi
  done
  echo "$n"
}

wait_leader() { # optional: a port that must NOT be the leader
  local avoid=${1:-} L
  for _ in $(seq 1 3000); do
    L=$(leader_port)
    if [ -n "$L" ] && [ "$L" != "$avoid" ]; then echo "$L"; return; fi
  done
}

code() { curl -s -o /dev/null -m 3 -w "%{http_code}" "$@"; }

go build -o "$BIN" ./cmd/kvnode || exit 1
cleanup; rm -rf "$DATA"; mkdir -p "$DATA"

echo
echo "1. ELECTION -- three nodes with no leader must agree on exactly one"
for i in 1 2 3; do start_node $i; done
for p in 8001 8002 8003; do ready $p || { echo "node on :$p never started"; exit 1; }; done
L=$(wait_leader)
check "a leader was elected" "yes" "$([ -n "$L" ] && echo yes || echo no)"
check "exactly one node claims leadership" "1" "$(count_leaders)"

F=$(for p in 8001 8002 8003; do if [ "$p" != "$L" ]; then echo "$p"; break; fi; done)
F2=$(for p in 8001 8002 8003; do if [ "$p" != "$L" ] && [ "$p" != "$F" ]; then echo "$p"; break; fi; done)
echo "   leader=:$L followers=:$F,:$F2"

echo
echo "2. REPLICATION -- one write must reach all three state machines"
check "PUT to leader returns 204" "204" "$(code -X PUT -d 'Delhi' "localhost:$L/kv/city")"
for p in $L $F $F2; do
  check "strong read on :$p sees it" "Delhi" "$(curl -s "localhost:$p/kv/city?consistency=strong")"
done

echo
echo "3. FORWARDING -- a client may write to any node"
check "PUT to follower returns 204" "204" "$(code -X PUT -d 'via-follower' "localhost:$F/kv/route")"
check "leader has the forwarded write" "via-follower" "$(curl -s "localhost:$L/kv/route?consistency=strong")"

echo
echo "4. LINEARIZABILITY -- a strong read never returns a superseded value"
BAD=0
for i in $(seq 1 40); do
  curl -s -o /dev/null -X PUT -d "v$i" "localhost:$L/kv/lin"
  GOT=$(curl -s "localhost:$F/kv/lin?consistency=strong")
  if [ "$GOT" != "v$i" ]; then BAD=$((BAD+1)); fi
done
check "40 write-then-strong-read cycles, stale reads" "0" "$BAD"

echo
echo "5. STALENESS -- eventual reads are allowed to lag (and visibly do)"
STALE=0
for i in $(seq 1 40); do
  curl -s -o /dev/null -X PUT -d "e$i" "localhost:$L/kv/ev"
  GOT=$(curl -s "localhost:$F/kv/ev?consistency=eventual")
  if [ "$GOT" != "e$i" ]; then STALE=$((STALE+1)); fi
done
echo "   $STALE of 40 eventual reads were behind (expected: some, this is the tradeoff)"

echo
echo "6. FAILOVER -- kill the leader; a new one must take over"
LID=$(curl -s "localhost:$L/status" | sed 's/id=\([0-9]*\).*/\1/')
pkill -9 -f "$BIN --id=$LID " 2>/dev/null
T0=$(python3 -c 'import time;print(time.time())')
NEW=$(wait_leader "$L")
T1=$(python3 -c 'import time;print(time.time())')
check "a new leader was elected" "yes" "$([ -n "$NEW" ] && echo yes || echo no)"
check "it is not the dead node" "yes" "$([ "$NEW" != "$L" ] && echo yes || echo no)"
check "exactly one leader after failover" "1" "$(count_leaders)"
printf "   failover took %s ms\n" "$(python3 -c "print(int(($T1-$T0)*1000))")"

echo
echo "7. DURABILITY ACROSS FAILOVER -- committed data must survive"
check "committed write survived" "Delhi" "$(curl -s "localhost:$NEW/kv/city?consistency=strong")"
check "cluster accepts writes again" "204" "$(code -X PUT -d 'yes' "localhost:$NEW/kv/after")"

echo
echo "8. SAFETY -- a minority must REFUSE writes rather than split-brain"
SURVIVOR=$(for p in 8001 8002 8003; do if [ "$p" != "$L" ] && [ "$p" != "$NEW" ]; then echo "$p"; break; fi; done)
SID=$(curl -s "localhost:$SURVIVOR/status" | sed 's/id=\([0-9]*\).*/\1/')
pkill -9 -f "$BIN --id=$SID " 2>/dev/null
sleep 1
W=$(code -X PUT -d 'nope' "localhost:$NEW/kv/mustfail")
check "write with 1 of 3 nodes is refused" "503" "$W"
check "strong read with no quorum is refused" "503" "$(code "localhost:$NEW/kv/city?consistency=strong")"
check "eventual read still served (AP side)" "Delhi" "$(curl -s "localhost:$NEW/kv/city?consistency=eventual")"

echo
echo "9. RESTART DURABILITY -- data must come back from disk"
pkill -9 -f "$BIN --id=" 2>/dev/null
sleep 1
for i in 1 2 3; do start_node $i; done
for p in 8001 8002 8003; do ready $p; done
L2=$(wait_leader)
check "cluster recovered from disk" "Delhi" "$(curl -s "localhost:$L2/kv/city?consistency=strong")"
check "post-failover write survived too" "yes" "$(curl -s "localhost:$L2/kv/after?consistency=strong")"

echo
echo "----------------------------------------------------------------"
printf "  %s passed, %s failed\n" "$(green $PASS)" "$([ $FAIL -eq 0 ] && green 0 || red $FAIL)"
[ $FAIL -eq 0 ] || exit 1
