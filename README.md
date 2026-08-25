# raftkv

A key-value store that runs on three machines and keeps working when one of
them dies. Go, no dependencies outside the standard library.

Writes go to a leader and are only acknowledged once a majority of nodes have
them on disk. Reads can be served by any node, and the client chooses per
request whether it wants a guaranteed fresh answer or a fast one.

## How it fits together

```
                    client
                      |
                      |  HTTP
                      v
                 +----------+
                 |  leader  |          all writes land here
                 +----------+
                   |      |
                   |      |   AppendEntries (HTTP + JSON)
             +-----+      +-----+
             v                  v
       +-----------+      +-----------+
       | follower1 |      | follower2 |   can serve reads
       +-----------+      +-----------+
```

Three separate processes, three separate log files, nothing shared. The only
thing crossing between them is messages.

Each node is built out of five pieces:

| package | what it does |
|---|---|
| `internal/store` | the map, plus the lock that keeps concurrent requests from corrupting it |
| `internal/wal` | append-only log file. Length-prefixed frames with a CRC, fsynced per write |
| `internal/raft` | the log, elections, replication, commit tracking, read barriers |
| `internal/engine` | applies committed entries to the map. Must be deterministic |
| `internal/api` | HTTP handlers, consistency modes, status endpoint |

## What happens on a write

```
PUT /kv/city  "Delhi"
        |
        1. leader appends the entry to its log and fsyncs it
        2. leader sends it to both followers
        3. leader waits for ONE of them to confirm  (2 of 3 is a majority)
        4. entry is committed, applied to the map
        5. 204 No Content
```

Step 1 is what makes the write survive a power cut. Step 3 is what makes it
survive the leader being destroyed a millisecond later. Neither alone is
enough.

If the leader dies, followers stop hearing heartbeats, one of them times out
after 150 to 300ms, and campaigns. Whoever collects two votes becomes the new
leader. Terms are just numbers that go up, and a higher term always wins, so
an old leader that wakes up from a network glitch gets told to stand down.

## Running it

```bash
go build -o kvnode ./cmd/kvnode

./kvnode --id=1 --addr=:8001 --raft-addr=localhost:9001 \
         --data=./data/n1 --peers=2=localhost:9002,3=localhost:9003
```

Three of those, matching ids and ports. They elect a leader themselves.

```bash
curl -X PUT -d 'Delhi' localhost:8001/kv/city     # write to any node
curl localhost:8002/kv/city                        # strong read (default)
curl 'localhost:8002/kv/city?consistency=eventual' # fast, maybe stale
curl localhost:8001/status                         # who is leader, log state
```

You can write to a follower. It forwards to the leader for you.

## The two read modes

| | guarantee | cost |
|---|---|---|
| `strong` (default) | sees every write acknowledged before this read started | one round trip to a majority |
| `eventual` | whatever this node has applied so far | nothing, it is a local map lookup |

Strong reads are the interesting part. Routing the read to the leader is not
enough, because a leader that has been partitioned away does not know it. It
still has the role, the term and a full log. So before answering, the leader
confirms with a majority that it is still in charge, and only then reads. If
it cannot confirm, the read fails with a 503 instead of quietly returning
stale data.

Followers can serve strong reads too. The follower asks the leader for a
commit index, waits until its own state machine reaches it, then answers
locally. The value never crosses the network twice.

## Numbers

Apple M4, three nodes as separate processes on localhost, Go 1.26. Every
write is fsynced individually. Reproduce with `./scripts/bench.sh`.

| | throughput | p50 latency |
|---|---|---|
| writes, 3 nodes | 78 /sec | 13 ms |
| writes, 1 node | 203 /sec | 5 ms |
| strong reads, leader | 52,000 /sec | 287 us |
| strong reads, follower | 32,000 /sec | 442 us |
| eventual reads, follower | 93,000 /sec | 157 us |

78 writes a second looks terrible, and it is not a Go problem. A 3-node write
costs two fsyncs in sequence, one on the leader and one on a follower, and
fsync on this machine is a real hardware barrier at about 5ms. That is roughly
95% of the 13ms. The network hop between them is 0.2ms. So replication is
cheap and durability is expensive, which is the opposite of what I expected.
Batching writes behind one fsync would move this by around 90x. I measured
that but did not build it, because one fsync per write is obviously correct.

Reads never touch a disk, which is the entire 700x gap.

Eventual reads lag by at most one heartbeat. Measured at p50 30ms, p99 49ms
against a 50ms heartbeat. Shorten the heartbeat and the window shrinks with
it, at the cost of more background chatter.

Failover is about 300ms after `kill -9`, measured as the gap in a continuous
write stream. Nearly all of it is the election timeout, randomised between 150
and 300ms so that nodes do not all campaign at once and split the vote.

## Tests

```bash
go test -race ./...        # 55 tests, including a 3-node cluster over real HTTP
./scripts/verify.sh        # 19 acceptance checks against a live cluster
./scripts/nolostwrites.sh  # kills the leader mid-write, checks nothing was lost
```

The cluster tests run over a `Transport` interface, so there is a version that
drops 20% of messages, duplicates replication RPCs, and delays everything
randomly. `TestSustainedChaos` runs that while killing and reviving a node
every quarter second. Typical run: 203 writes attempted, 168 acknowledged, 13
node kills, 36 Raft terms, and at the end all three logs agree entry for entry
with zero acknowledged writes lost.

The refusals matter as much as the successes. During an election the cluster
returns 503 rather than accepting a write it cannot commit.

## Things that broke, and what fixed them

Worth writing down because none of these showed up in unit tests. They only
appeared under load.

Strong reads were causing elections. The read barrier returns as soon as a
majority replies, then cancelled the context, which aborted the other peer's
in-flight request. An aborted request cannot go back in the idle connection
pool, so its TCP connection got torn down. At a few thousand barriers a second
that churned enough connections to overflow the peer's listen backlog, and it
started refusing the leader's heartbeats too. Letting the straggler finish
before cancelling took errors from 42,000 to 4.

Batching the read barrier was worth 7x. Sixteen concurrent strong reads were
each running their own quorum round trip. Sharing one round between everyone
waiting took it from 7,400/sec to 51,600/sec. The subtle part: a reader may
only join a round that has not started yet. Joining one already in flight
would be faster and wrong, since that round captured its commit index before
the reader arrived.

A leadership probe nearly un-committed data. The probe sends an empty
AppendEntries anchored at index 0, so a follower computed its commit index as
`min(leaderCommit, 0)` and moved it backwards. Commit indexes must never
decrease. One guard fixed it.

Most failed writes had actually succeeded. One chaos run acknowledged 168
writes, but 200 keys were committed. So 32 of the 35 writes that returned an
error to the client had in fact been committed, usually by the next leader
after the original one died mid-replication. An error means unknown, not
failed. These operations are idempotent so retrying is safe, but a production
version would need client request ids to deduplicate.

## Not implemented

- **Log compaction and snapshots.** The log grows forever and restart time
  grows with it. Measured: 5 million entries against a 100 key working set is
  198MB and 356ms of replay. It is a tuning problem rather than a correctness
  one, so I spent the time on election safety instead.
- **PreVote.** A partitioned node keeps incrementing its term while alone, and
  disrupts the cluster when it rejoins by forcing an election.
- **Authentication between peers.** Anything that can reach the peer port can
  claim to be the leader. Real clusters use mutual TLS.
- **Pipelined fsync and replication.** The leader could send entries while its
  own fsync is in flight. That is the largest remaining win.
