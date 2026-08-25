#!/usr/bin/env bash
# The strongest guarantee this system makes:
#
#   if a write returned 204, that value is readable forever after,
#   even if the leader is destroyed the instant after it replied.
#
# We write a sequence of keys, record exactly which ones were acknowledged,
# kill -9 the leader mid-stream, then read every acknowledged key back.
# Any missing key is data loss.
set -uo pipefail

BIN=${BIN:-/tmp/kvnode}
DATA=${DATA:-/tmp/raftkv-lost}
N=${N:-120}          # writes to attempt
KILL_AT=${KILL_AT:-40}

cleanup() { pkill -f "$BIN --id=" 2>/dev/null; }
trap cleanup EXIT

start_node() {
  local id=$1 peers=""
  for j in 1 2 3; do [ "$j" != "$id" ] && peers="$peers,$j=localhost:900$j"; done
  "$BIN" --id="$id" --addr=":800$id" --raft-addr="localhost:900$id" \
         --data="$DATA/n$id" --peers="${peers#,}" >/dev/null 2>>"$DATA/n$id.log" &
  disown
}
ready() { for _ in $(seq 1 400); do if curl -sf "localhost:$1/health" >/dev/null 2>&1; then return 0; fi; done; return 1; }
leader_port() {
  for p in 8001 8002 8003; do
    if curl -s -m 1 "localhost:$p/status" 2>/dev/null | grep -q role=leader; then echo "$p"; return; fi
  done
}

go build -o "$BIN" ./cmd/kvnode || exit 1
cleanup; rm -rf "$DATA"; mkdir -p "$DATA"
for i in 1 2 3; do start_node $i; done
for p in 8001 8002 8003; do ready $p || exit 1; done
for _ in $(seq 1 3000); do L=$(leader_port); if [ -n "${L:-}" ]; then break; fi; done
echo "leader on :$L"

ACKED=""; REFUSED=0; UNKNOWN=0; KILLED=""
for i in $(seq 1 "$N"); do
  if [ "$i" = "$KILL_AT" ]; then
    LID=$(curl -s -m 1 "localhost:$L/status" | sed 's/id=\([0-9]*\).*/\1/')
    echo "  ...kill -9 node $LID (the leader) after $((i-1)) attempts"
    pkill -9 -f "$BIN --id=$LID " 2>/dev/null
    KILLED=$L
  fi

  # Always send to a node that is still alive, so the client keeps trying.
  TARGET=$(for p in 8001 8002 8003; do if [ "$p" != "${KILLED:-}" ]; then echo "$p"; break; fi; done)

  CODE=$(curl -s -o /dev/null -m 5 -w "%{http_code}" -X PUT -d "value-$i" "localhost:$TARGET/kv/k$i")
  case "$CODE" in
    204) ACKED="$ACKED $i" ;;
    503) REFUSED=$((REFUSED+1)) ;;
    *)   UNKNOWN=$((UNKNOWN+1)) ;;
  esac
done

NACK=$(echo $ACKED | wc -w | tr -d ' ')
echo
echo "  acknowledged (204) : $NACK"
echo "  refused      (503) : $REFUSED   <- honest refusals during the election"
echo "  other/timeout      : $UNKNOWN"

# Let the survivors settle, then verify from a node that is NOT the old leader.
for _ in $(seq 1 3000); do L2=$(leader_port); if [ -n "${L2:-}" ] && [ "$L2" != "$KILLED" ]; then break; fi; done
echo "  verifying against new leader :$L2"

LOST=""
for i in $ACKED; do
  GOT=$(curl -s -m 5 "localhost:$L2/kv/k$i?consistency=strong")
  if [ "$GOT" != "value-$i" ]; then LOST="$LOST $i"; fi
done

echo
if [ -z "$LOST" ]; then
  printf "  \033[32mPASS\033[0m  all %s acknowledged writes survived kill -9 of the leader\n" "$NACK"
else
  printf "  \033[31mFAIL\033[0m  LOST ACKNOWLEDGED WRITES:%s\n" "$LOST"
  exit 1
fi

# Every node must also agree, byte for byte, on the replicated log.
echo
echo "  log state on each node:"
for p in 8001 8002 8003; do
  S=$(curl -s -m 1 "localhost:$p/status" 2>/dev/null) && echo "    :$p $S" || echo "    :$p (dead)"
done
