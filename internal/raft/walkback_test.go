package raft

import (
	"testing"
)

// TestLeaderWalksBackToFindMatchPoint shows the whole repair loop.
//
// The leader does not know where the follower diverged. It optimistically
// assumes the follower is fully caught up, gets rejected, decrements its
// guess, and tries again -- until a Match succeeds. Then one append fixes
// everything after that point.
func TestLeaderWalksBackToFindMatchPoint(t *testing.T) {
	// The true log, held by the new leader (term 4).
	leader := NewLog()
	mustAppend(t, leader, 1, cmd("a", "1"))
	mustAppend(t, leader, 1, cmd("b", "2"))
	mustAppend(t, leader, 4, cmd("c", "3"))
	mustAppend(t, leader, 4, cmd("d", "4"))

	// A follower that spent terms 2 and 3 listening to a leader that was
	// partitioned away, and never got the real entries.
	follower := NewLog()
	mustAppend(t, follower, 1, cmd("a", "1"))
	mustAppend(t, follower, 1, cmd("b", "2"))
	mustAppend(t, follower, 2, cmd("x", "stale"))
	mustAppend(t, follower, 3, cmd("y", "stale"))
	mustAppend(t, follower, 3, cmd("z", "stale"))

	t.Logf("leader   : %s", render(leader))
	t.Logf("follower : %s", render(follower))
	t.Log("")

	// nextIndex is the leader's guess at where to start sending. Raft
	// initialises it optimistically to "one past my last entry".
	nextIndex := leader.LastIndex() + 1

	attempts := 0
	for {
		attempts++
		prevIndex := nextIndex - 1
		prevEntry, _ := leader.At(prevIndex)
		prevTerm := prevEntry.Term // index 0 gives the zero value: term 0

		entries := leader.EntriesFrom(nextIndex)
		ok := mustAppendAfter(t, follower, prevIndex, prevTerm, entries)

		t.Logf("attempt %d: send prevIndex=%d prevTerm=%d with %d entries -> %v",
			attempts, prevIndex, prevTerm, len(entries), map[bool]string{true: "ACCEPTED", false: "rejected"}[ok])

		if ok {
			break
		}

		nextIndex-- // back up one and try an earlier anchor
		if nextIndex == 0 {
			t.Fatal("walked back past the sentinel, which should be impossible")
		}
	}

	t.Log("")
	t.Logf("follower : %s   <- now identical to the leader", render(follower))

	if render(follower) != render(leader) {
		t.Fatalf("logs still differ:\n leader   %s\n follower %s",
			render(leader), render(follower))
	}

	// Sanity: the stale entries are really gone, not just hidden.
	if e, ok := follower.At(3); !ok || e.Key != "c" {
		t.Fatalf("index 3 = %+v, want the leader's entry c", e)
	}
	if follower.LastIndex() != 4 {
		t.Fatalf("follower last index = %d, want 4", follower.LastIndex())
	}
}
