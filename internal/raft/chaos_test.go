package raft

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// lossy is the network these tests run on: one message in five never
// arrives, one AppendEntries in ten arrives twice, and every message is
// delayed by a random amount comparable to a heartbeat interval.
var lossy = netConditions{
	dropRate: 0.20,
	dupRate:  0.10,
	maxDelay: 8 * time.Millisecond,
}

// TestElectsALeaderUnderPacketLoss: elections must still converge when votes
// and heartbeats go missing. Randomised timeouts are what make this work --
// a fixed timeout would split the vote and retry forever.
func TestElectsALeaderUnderPacketLoss(t *testing.T) {
	c := newClusterWith(t, 3, lossy)
	c.start(t)

	start := time.Now()
	leader := c.waitForLeader(t, 10*time.Second)
	t.Logf("elected %s in %v despite packet loss", leader.node.ID(),
		time.Since(start).Round(time.Millisecond))

	c.waitForAgreement(t, leader.node.ID(), 10*time.Second)
	c.netStats(t)
}

// TestReplicatesUnderPacketLoss: every node must converge on the same log
// even though a fifth of the replication messages are lost and a tenth are
// delivered twice.
func TestReplicatesUnderPacketLoss(t *testing.T) {
	c := newClusterWith(t, 3, lossy)
	c.start(t)

	leader := c.waitForLeader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	acked := 0
	for i := 0; i < 30; i++ {
		if err := leader.node.Propose(ctx, set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))); err == nil {
			acked++
		}
		// Leadership can move under this much loss; follow it.
		if !leader.node.IsLeader() {
			leader = c.waitForLeader(t, 10*time.Second)
		}
	}
	t.Logf("%d of 30 writes were acknowledged", acked)

	if acked == 0 {
		t.Fatal("no writes succeeded at all")
	}

	// Every acknowledged write must eventually be visible everywhere.
	for i := 0; i < acked; i++ {
		for _, id := range c.ids {
			waitForValue(t, c.members[id], fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 15*time.Second)
		}
	}

	// And the logs themselves must agree, entry for entry.
	assertLogsMatch(t, c)
	c.netStats(t)
}

// TestNoAcknowledgedWriteIsLostUnderChaos is the strongest property: if
// Propose returned nil, that value survives -- lossy network, duplicated
// messages, and a leader being destroyed partway through.
func TestNoAcknowledgedWriteIsLostUnderChaos(t *testing.T) {
	c := newClusterWith(t, 3, lossy)
	c.start(t)

	leader := c.waitForLeader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var acked []int
	for i := 0; i < 40; i++ {
		if i == 15 {
			t.Logf("killing leader %s mid-stream", leader.node.ID())
			leader.up.Store(false)
			leader = c.waitForLeader(t, 10*time.Second)
			t.Logf("new leader is %s", leader.node.ID())
		}

		if !leader.node.IsLeader() || !leader.up.Load() {
			leader = c.waitForLeader(t, 10*time.Second)
		}

		if err := leader.node.Propose(ctx, set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))); err == nil {
			acked = append(acked, i)
		}
	}
	t.Logf("%d of 40 writes acknowledged (the rest were honestly refused)", len(acked))

	// Read every acknowledged key back from a node that is still alive.
	for _, i := range acked {
		waitForValue(t, leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 15*time.Second)
	}
	t.Logf("all %d acknowledged writes survived", len(acked))
	c.netStats(t)
}

// TestDuplicatesDoNotCorruptTheLog isolates the duplicate case: every single
// AppendEntries is delivered twice.
func TestDuplicatesDoNotCorruptTheLog(t *testing.T) {
	c := newClusterWith(t, 3, netConditions{dupRate: 1.0})
	c.start(t)

	leader := c.waitForLeader(t, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for i := 0; i < 20; i++ {
		if err := leader.node.Propose(ctx, set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}

	for i := 0; i < 20; i++ {
		for _, id := range c.ids {
			waitForValue(t, c.members[id], fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 10*time.Second)
		}
	}

	assertLogsMatch(t, c)
	c.netStats(t)
}

// assertLogsMatch checks that every live node holds an identical log up to
// the shortest one -- the Log Matching property, asserted directly.
func assertLogsMatch(t *testing.T, c *cluster) {
	t.Helper()

	shortest := ^uint64(0)
	for _, id := range c.ids {
		if m := c.members[id]; m.up.Load() {
			if n := m.node.Log().LastIndex(); n < shortest {
				shortest = n
			}
		}
	}
	if shortest == ^uint64(0) || shortest == 0 {
		t.Fatal("no live node has any entries")
	}

	for i := uint64(1); i <= shortest; i++ {
		var (
			refKey  string
			refTerm uint64
			refID   NodeID
			first   = true
		)
		for _, id := range c.ids {
			m := c.members[id]
			if !m.up.Load() {
				continue
			}
			e, ok := m.node.Log().At(i)
			if !ok {
				t.Fatalf("node %s is missing entry %d", id, i)
			}
			if first {
				refKey, refTerm, refID, first = e.Key, e.Term, id, false
				continue
			}
			if e.Key != refKey || e.Term != refTerm {
				t.Fatalf("logs diverge at index %d: node %s has (%d,%q), node %s has (%d,%q)",
					i, refID, refTerm, refKey, id, e.Term, e.Key)
			}
		}
	}
	t.Logf("all live nodes agree on every one of %d log entries", shortest)
}

// tryFindLeader returns the current leader, or nil. Unlike waitForLeader it
// never fails the test, so it is safe to call from a loop that expects the
// cluster to be in flux.
func (c *cluster) tryFindLeader(within time.Duration) *member {
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ls := c.leaders(); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(time.Millisecond)
	}
	return nil
}

// TestSustainedChaos is the closest thing here to a real fault-injection run.
//
// For several seconds: a fifth of all messages are dropped, a tenth of
// replication messages are duplicated, every message is delayed randomly,
// AND a randomly chosen node is repeatedly killed and revived -- while a
// client writes continuously.
//
// One node at a time goes down, so a majority always survives. The cluster
// is therefore required to keep making progress, not merely to avoid
// corruption. At the end we check the two things that must never break:
//
//  1. every acknowledged write is still there
//  2. every node's log agrees, entry for entry
func TestSustainedChaos(t *testing.T) {
	if testing.Short() {
		t.Skip("chaos test takes several seconds")
	}

	c := newClusterWith(t, 3, lossy)
	c.start(t)

	if c.tryFindLeader(10*time.Second) == nil {
		t.Fatal("no initial leader")
	}

	stop := make(chan struct{})
	done := make(chan struct{})

	// The chaos monkey: kill one node, wait, revive it, repeat.
	var kills int
	go func() {
		defer close(done)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			case <-time.After(250 * time.Millisecond):
			}

			victim := c.members[c.ids[i%len(c.ids)]]
			victim.up.Store(false)
			kills++

			select {
			case <-stop:
				victim.up.Store(true)
				return
			case <-time.After(200 * time.Millisecond):
			}
			victim.up.Store(true)
		}
	}()

	// The client: write as fast as it can, recording only what was acked.
	var acked []int
	attempts := 0
	deadline := time.Now().Add(6 * time.Second)

	for i := 0; time.Now().Before(deadline); i++ {
		attempts++

		leader := c.tryFindLeader(500 * time.Millisecond)
		if leader == nil {
			continue // mid-election; a client would retry, so do that
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		err := leader.node.Propose(ctx, set(fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i)))
		cancel()

		if err == nil {
			acked = append(acked, i)
		}
	}

	close(stop)
	<-done
	for _, id := range c.ids {
		c.members[id].up.Store(true)
	}

	t.Logf("%d writes attempted, %d acknowledged, across %d node kills",
		attempts, len(acked), kills)
	if len(acked) == 0 {
		t.Fatal("no writes were acknowledged at all -- the cluster made no progress")
	}

	// Let the cluster settle now that the network is whole again.
	leader := c.tryFindLeader(15 * time.Second)
	if leader == nil {
		t.Fatal("cluster never stabilised after chaos stopped")
	}

	// 1. Every acknowledged write must still be readable.
	for _, i := range acked {
		waitForValue(t, leader, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 20*time.Second)
	}
	t.Logf("all %d acknowledged writes survived", len(acked))

	// 2. Every node must agree on the log.
	for _, id := range c.ids {
		m := c.members[id]
		deadline := time.Now().Add(15 * time.Second)
		for m.node.Log().LastIndex() < leader.node.Log().LastIndex() && time.Now().Before(deadline) {
			time.Sleep(5 * time.Millisecond)
		}
	}
	assertLogsMatch(t, c)

	for _, id := range c.ids {
		m := c.members[id]
		t.Logf("  node %s: term=%d log=%d commit=%d applied=%d keys=%d",
			id, m.node.Term(), m.node.Log().LastIndex(),
			m.node.CommitIndex(), m.node.LastApplied(), m.engine.Len())
	}
	c.netStats(t)
}
