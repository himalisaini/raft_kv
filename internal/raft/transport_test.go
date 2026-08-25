package raft

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/himalisaini/raftkv/internal/wal"
)

// testNode starts a real HTTP server for one node and returns the node plus
// the Peer struct another node would use to reach it.
func testNode(t *testing.T, id NodeID, peerIDs ...NodeID) (*Node, Peer) {
	t.Helper()

	log, err := OpenLog(filepath.Join(t.TempDir(), "raft.log"))
	if err != nil {
		t.Fatalf("open log: %v", err)
	}

	cfg := Config{ID: id, Addr: "unused"}
	for _, p := range peerIDs {
		cfg.Peers = append(cfg.Peers, Peer{ID: p, Addr: "unused"})
	}

	n, err := NewNode(Options{Config: cfg, Log: log}, PersistentState{})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	srv := httptest.NewServer(PeerHandler(n))
	t.Cleanup(func() {
		srv.Close()
		log.Close()
	})

	// httptest gives us "http://127.0.0.1:PORT"; the transport adds the
	// scheme itself, so hand back just host:port.
	return n, Peer{ID: id, Addr: strings.TrimPrefix(srv.URL, "http://")}
}

// TestReplicationOverHTTP: an entry created on one node ends up on another,
// through a real socket.
func TestReplicationOverHTTP(t *testing.T) {
	follower, followerPeer := testNode(t, "2", "1", "3")
	tr := NewHTTPTransport(2 * time.Second)
	ctx := context.Background()

	resp, err := tr.AppendEntries(ctx, followerPeer, AppendEntriesRequest{
		Term:         1,
		LeaderID:     "1",
		PrevLogIndex: 0, // anchored at the sentinel: the follower is empty
		PrevLogTerm:  0,
		Entries: []wal.Record{
			{Term: 1, Index: 1, Op: wal.OpSet, Key: "city", Value: "Delhi"},
			{Term: 1, Index: 2, Op: wal.OpSet, Key: "lang", Value: "Go"},
		},
		LeaderCommit: 0,
	})
	if err != nil {
		t.Fatalf("rpc: %v", err)
	}

	if !resp.Success {
		t.Fatalf("follower rejected: %+v", resp)
	}
	if resp.Term != 1 {
		t.Fatalf("follower term = %d, want 1 (it should have adopted the leader's)", resp.Term)
	}
	if resp.LastLogIndex != 2 {
		t.Fatalf("follower last index = %d, want 2", resp.LastLogIndex)
	}
	if got := render(follower.Log()); got != "[1:city][1:lang]" {
		t.Fatalf("follower log = %s", got)
	}
}

// TestStaleLeaderIsRejected: a leader from an old term must be told to stop.
func TestStaleLeaderIsRejected(t *testing.T) {
	follower, followerPeer := testNode(t, "2", "1", "3")
	tr := NewHTTPTransport(2 * time.Second)
	ctx := context.Background()

	// A term-5 leader replicates successfully.
	if _, err := tr.AppendEntries(ctx, followerPeer, AppendEntriesRequest{
		Term: 5, LeaderID: "3", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []wal.Record{{Term: 5, Index: 1, Op: wal.OpSet, Key: "a"}},
	}); err != nil {
		t.Fatalf("rpc: %v", err)
	}

	// Now a leader from term 2 wakes up from a partition and tries to write.
	resp, err := tr.AppendEntries(ctx, followerPeer, AppendEntriesRequest{
		Term: 2, LeaderID: "1", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries: []wal.Record{{Term: 2, Index: 1, Op: wal.OpSet, Key: "stale"}},
	})
	if err != nil {
		t.Fatalf("rpc: %v", err)
	}

	if resp.Success {
		t.Fatal("follower accepted a write from a stale leader")
	}
	if resp.Term != 5 {
		t.Fatalf("reply term = %d, want 5 so the old leader learns it is deposed", resp.Term)
	}
	if got := render(follower.Log()); got != "[5:a]" {
		t.Fatalf("follower log = %s, want [5:a] untouched", got)
	}
}

// TestCommitIndexNeverOutrunsTheLog: a follower that is behind must not claim
// to have committed entries it has not received.
func TestCommitIndexNeverOutrunsTheLog(t *testing.T) {
	follower, followerPeer := testNode(t, "2", "1", "3")
	tr := NewHTTPTransport(2 * time.Second)

	// The leader has committed up to index 9, but only sends us entry 1.
	resp, err := tr.AppendEntries(context.Background(), followerPeer, AppendEntriesRequest{
		Term: 1, LeaderID: "1", PrevLogIndex: 0, PrevLogTerm: 0,
		Entries:      []wal.Record{{Term: 1, Index: 1, Op: wal.OpSet, Key: "a"}},
		LeaderCommit: 9,
	})
	if err != nil || !resp.Success {
		t.Fatalf("rpc: %v %+v", err, resp)
	}

	if got := follower.CommitIndex(); got != 1 {
		t.Fatalf("commit index = %d, want 1 -- it must not exceed our own log", got)
	}
}

// TestUnreachablePeerIsAnError: a down node produces an error, not a panic
// and not a false "success".
func TestUnreachablePeerIsAnError(t *testing.T) {
	tr := NewHTTPTransport(200 * time.Millisecond)

	_, err := tr.AppendEntries(context.Background(),
		Peer{ID: "9", Addr: "127.0.0.1:1"}, // nothing listens on port 1
		AppendEntriesRequest{Term: 1, LeaderID: "1"})

	if err == nil {
		t.Fatal("expected an error calling a dead peer")
	}
}

// TestWalkbackOverHTTP repeats the 2.1 convergence demo, but every round
// trip is a real request over a real socket between two real HTTP servers.
func TestWalkbackOverHTTP(t *testing.T) {
	// The leader's log, held locally. In 2.4 this lives inside a Node.
	leader := NewLog()
	mustAppend(t, leader, 1, cmd("a", "1"))
	mustAppend(t, leader, 1, cmd("b", "2"))
	mustAppend(t, leader, 4, cmd("c", "3"))
	mustAppend(t, leader, 4, cmd("d", "4"))

	follower, followerPeer := testNode(t, "2", "1", "3")
	mustAppend(t, follower.Log(), 1, cmd("a", "1"))
	mustAppend(t, follower.Log(), 1, cmd("b", "2"))
	mustAppend(t, follower.Log(), 2, cmd("x", "stale"))
	mustAppend(t, follower.Log(), 3, cmd("y", "stale"))

	t.Logf("leader   : %s", render(leader))
	t.Logf("follower : %s", render(follower.Log()))
	t.Log("")

	tr := NewHTTPTransport(2 * time.Second)
	nextIndex := leader.LastIndex() + 1

	for attempt := 1; ; attempt++ {
		prevIndex := nextIndex - 1
		prevEntry, _ := leader.At(prevIndex)

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		resp, err := tr.AppendEntries(ctx, followerPeer, AppendEntriesRequest{
			Term:         4,
			LeaderID:     "1",
			PrevLogIndex: prevIndex,
			PrevLogTerm:  prevEntry.Term,
			Entries:      leader.EntriesFrom(nextIndex),
			LeaderCommit: 0,
		})
		cancel()
		if err != nil {
			t.Fatalf("rpc: %v", err)
		}

		t.Logf("attempt %d: prevIndex=%d prevTerm=%d entries=%d -> success=%v (follower has %d)",
			attempt, prevIndex, prevEntry.Term, len(leader.EntriesFrom(nextIndex)),
			resp.Success, resp.LastLogIndex)

		if resp.Success {
			break
		}
		if attempt > 10 {
			t.Fatal("failed to converge")
		}
		nextIndex--
	}

	t.Log("")
	t.Logf("follower : %s   <- converged over the network", render(follower.Log()))

	if render(follower.Log()) != render(leader) {
		t.Fatalf("logs differ:\n leader   %s\n follower %s", render(leader), render(follower.Log()))
	}
}
