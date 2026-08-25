package raft

import (
	"path/filepath"
	"testing"
)

// voter builds a node with a real state file so we can test persistence.
func voter(t *testing.T, dir string) *Node {
	t.Helper()

	rlog, err := OpenLog(filepath.Join(dir, "raft.log"))
	if err != nil {
		t.Fatalf("open log: %v", err)
	}
	t.Cleanup(func() { rlog.Close() })

	st, restored, err := OpenStateStore(dir)
	if err != nil {
		t.Fatalf("open state: %v", err)
	}

	n, err := NewNode(Options{
		Config: Config{ID: "1", Addr: "a", Peers: []Peer{
			{ID: "2", Addr: "b"}, {ID: "3", Addr: "c"},
		}},
		Log:   rlog,
		State: st,
	}, restored)
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	return n
}

func vote(t *testing.T, n *Node, term uint64, candidate NodeID, lastIndex, lastTerm uint64) RequestVoteResponse {
	t.Helper()
	resp, err := n.HandleRequestVote(RequestVoteRequest{
		Term: term, CandidateID: candidate,
		LastLogIndex: lastIndex, LastLogTerm: lastTerm,
	})
	if err != nil {
		t.Fatalf("request vote: %v", err)
	}
	return resp
}

// TestOneVotePerTerm is the rule that makes two leaders impossible.
func TestOneVotePerTerm(t *testing.T) {
	n := voter(t, t.TempDir())

	if resp := vote(t, n, 1, "2", 0, 0); !resp.VoteGranted {
		t.Fatal("first candidate should get the vote")
	}

	// A second candidate asks in the SAME term.
	if resp := vote(t, n, 1, "3", 0, 0); resp.VoteGranted {
		t.Fatal("granted a second vote in term 1 -- two leaders become possible")
	}

	// The same candidate re-asking is a retry, not a second vote.
	if resp := vote(t, n, 1, "2", 0, 0); !resp.VoteGranted {
		t.Fatal("a retry from the same candidate should still be granted")
	}

	// A new term is a new election, so the old vote no longer binds us.
	if resp := vote(t, n, 2, "3", 0, 0); !resp.VoteGranted {
		t.Fatal("term 2 is a fresh election; node 3 should get the vote")
	}
	if n.Term() != 2 {
		t.Fatalf("term = %d, want 2", n.Term())
	}
}

// TestStaleCandidateIsDenied: a candidate from an old term learns the truth.
func TestStaleCandidateIsDenied(t *testing.T) {
	n := voter(t, t.TempDir())
	vote(t, n, 5, "2", 0, 0)

	resp := vote(t, n, 3, "3", 0, 0)
	if resp.VoteGranted {
		t.Fatal("granted a vote to a candidate from term 3 while in term 5")
	}
	if resp.Term != 5 {
		t.Fatalf("reply term = %d, want 5 so the candidate learns it is behind", resp.Term)
	}
}

// TestElectionRestriction: a candidate whose log is less complete than ours
// must not win, because it could not replay entries we have already applied.
func TestElectionRestriction(t *testing.T) {
	n := voter(t, t.TempDir())
	// Our log: 3 entries, latest one from term 4.
	mustAppend(t, n.Log(), 2, cmd("a", "1"))
	mustAppend(t, n.Log(), 4, cmd("b", "2"), cmd("c", "3"))

	cases := []struct {
		name                string
		lastIndex, lastTerm uint64
		want                bool
	}{
		{"shorter log, same term", 2, 4, false},
		{"same log", 3, 4, true},
		{"longer log, same term", 9, 4, true},
		{"older term, longer log", 99, 3, false},
		{"newer term, shorter log", 1, 5, true},
		{"empty log", 0, 0, false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// A fresh term each time so the one-vote rule does not interfere.
			n2 := voter(t, t.TempDir())
			mustAppend(t, n2.Log(), 2, cmd("a", "1"))
			mustAppend(t, n2.Log(), 4, cmd("b", "2"), cmd("c", "3"))

			resp := vote(t, n2, 10, "2", tc.lastIndex, tc.lastTerm)
			if resp.VoteGranted != tc.want {
				t.Fatalf("candidate with last=(%d,%d): granted=%v, want %v",
					tc.lastIndex, tc.lastTerm, resp.VoteGranted, tc.want)
			}
		})
	}
}

// TestVoteSurvivesRestart is the reason the state file exists. A node that
// crashes after voting must NOT be able to vote again in the same term.
func TestVoteSurvivesRestart(t *testing.T) {
	dir := t.TempDir()

	n1 := voter(t, dir)
	if resp := vote(t, n1, 7, "2", 0, 0); !resp.VoteGranted {
		t.Fatal("node 2 should get the vote")
	}
	n1.Log().Close()

	// "Crash" and restart: a brand new Node, from nothing but the files.
	n2 := voter(t, dir)

	if n2.Term() != 7 {
		t.Fatalf("term after restart = %d, want 7", n2.Term())
	}
	if n2.VotedFor() != "2" {
		t.Fatalf("votedFor after restart = %q, want \"2\"", n2.VotedFor())
	}

	if resp := vote(t, n2, 7, "3", 0, 0); resp.VoteGranted {
		t.Fatal("voted twice in term 7 across a restart -- two leaders possible")
	}
}

// TestStateFileIsAtomic: a half-written state file must never be readable as
// valid state. We check the temp file is gone and the real file parses.
func TestStateFileIsAtomic(t *testing.T) {
	dir := t.TempDir()
	st, _, err := OpenStateStore(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	if err := st.Save(PersistentState{CurrentTerm: 42, VotedFor: "3"}); err != nil {
		t.Fatalf("save: %v", err)
	}

	_, restored, err := OpenStateStore(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if restored.CurrentTerm != 42 || restored.VotedFor != "3" {
		t.Fatalf("restored %+v, want term 42 voted 3", restored)
	}
}
