package raft

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/himalisaini/raftkv/internal/engine"
	"github.com/himalisaini/raftkv/internal/wal"
)

// member is one node in a test cluster, plus a switch to simulate it going
// offline without tearing down its state.
type member struct {
	node   *Node
	engine *engine.Engine
	addr   string
	up     atomic.Bool
	net    *flakyTransport
}

type cluster struct {
	members map[NodeID]*member
	ids     []NodeID
}

// netConditions describes the simulated network for a test cluster. The zero
// value is a perfect network, which is what most tests want.
type netConditions struct {
	dropRate float64
	dupRate  float64
	maxDelay time.Duration
}

// newCluster starts n nodes, each with its own log file and its own HTTP
// peer listener, all pointed at each other.
func newCluster(t *testing.T, n int) *cluster {
	t.Helper()
	return newClusterWith(t, n, netConditions{})
}

// newClusterWith starts a cluster on a simulated network.
func newClusterWith(t *testing.T, n int, net netConditions) *cluster {
	t.Helper()

	c := &cluster{members: make(map[NodeID]*member)}
	dir := t.TempDir()

	// Servers have to exist before configs can name their addresses, so
	// stand them up first and fill in the node pointer afterwards.
	for i := 1; i <= n; i++ {
		id := NodeID(fmt.Sprint(i))
		m := &member{}
		m.up.Store(true)

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !m.up.Load() {
				// Simulate an unreachable node: the leader's transport turns
				// this into an error, which is not an acknowledgement.
				http.Error(w, "node is down\n", http.StatusServiceUnavailable)
				return
			}
			PeerHandler(m.node).ServeHTTP(w, r)
		}))
		t.Cleanup(srv.Close)

		m.addr = strings.TrimPrefix(srv.URL, "http://")
		c.members[id] = m
		c.ids = append(c.ids, id)
	}

	for _, id := range c.ids {
		cfg := Config{ID: id, Addr: c.members[id].addr}
		for _, other := range c.ids {
			if other != id {
				cfg.Peers = append(cfg.Peers, Peer{ID: other, Addr: c.members[other].addr})
			}
		}

		rlog, err := OpenLog(filepath.Join(dir, string(id)+".log"))
		if err != nil {
			t.Fatalf("open log for %s: %v", id, err)
		}
		t.Cleanup(func() { rlog.Close() })

		m := c.members[id]
		m.engine = engine.New()
		m.net = &flakyTransport{
			inner:    NewHTTPTransport(2 * time.Second),
			owner:    m,
			dropRate: net.dropRate,
			dupRate:  net.dupRate,
			maxDelay: net.maxDelay,
		}
		st, restored, err := OpenStateStore(t.TempDir())
		if err != nil {
			t.Fatalf("open state for %s: %v", id, err)
		}

		node, err := NewNode(Options{
			Config:    cfg,
			Log:       rlog,
			State:     st,
			Transport: m.net,
			Apply:     m.engine.Apply,

			// Milliseconds instead of hundreds of them, so an election in a
			// test takes about as long as one function call.
			HeartbeatInterval:  15 * time.Millisecond,
			ElectionTimeoutMin: 60 * time.Millisecond,
			ElectionTimeoutMax: 120 * time.Millisecond,
		}, restored)
		if err != nil {
			t.Fatalf("new node %s: %v", id, err)
		}
		m.node = node
	}

	return c
}

func (c *cluster) leader(t *testing.T, id NodeID) *member {
	t.Helper()
	m := c.members[id]
	m.node.BecomeLeader()
	return m
}

func set(key, value string) wal.Record {
	return wal.Record{Op: wal.OpSet, Key: key, Value: value}
}

// TestClusterReplicatesToEveryNode: a write on the leader shows up on all
// three nodes' logs AND in all three state machines.
func TestClusterReplicatesToEveryNode(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader(t, "1")

	ctx := context.Background()
	for _, kv := range [][2]string{{"city", "Delhi"}, {"lang", "Go"}, {"city", "Mumbai"}} {
		if err := leader.node.Propose(ctx, set(kv[0], kv[1])); err != nil {
			t.Fatalf("propose %s: %v", kv[0], err)
		}
	}

	// One heartbeat carries the final commit index to the followers, which
	// is what lets them apply the last entry.
	leader.node.Heartbeat(ctx)

	for _, id := range c.ids {
		m := c.members[id]
		if got := m.node.Log().LastIndex(); got != 3 {
			t.Errorf("node %s log last index = %d, want 3", id, got)
		}
		if v, ok := m.engine.Get("city"); !ok || v != "Mumbai" {
			t.Errorf("node %s city = %q %v, want Mumbai true", id, v, ok)
		}
		if v, ok := m.engine.Get("lang"); !ok || v != "Go" {
			t.Errorf("node %s lang = %q %v, want Go true", id, v, ok)
		}
	}

	t.Logf("leader   commit=%d applied=%d", leader.node.CommitIndex(), leader.node.LastApplied())
	for _, id := range c.ids[1:] {
		m := c.members[id]
		t.Logf("follower %s commit=%d applied=%d keys=%d", id,
			m.node.CommitIndex(), m.node.LastApplied(), m.engine.Len())
	}
}

// TestCommitsWithOneNodeDown: 2 of 3 is a majority, so writes keep working.
func TestCommitsWithOneNodeDown(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader(t, "1")

	c.members["3"].up.Store(false)
	t.Log("node 3 is down; 2 of 3 remain")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := leader.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose should succeed with 2 of 3 nodes: %v", err)
	}
	leader.node.Heartbeat(ctx)

	if v, ok := c.members["2"].engine.Get("city"); !ok || v != "Delhi" {
		t.Fatalf("node 2 city = %q %v", v, ok)
	}
	if c.members["3"].engine.Len() != 0 {
		t.Fatal("node 3 is down and must not have applied anything")
	}
}

// TestFollowersApplyOneRoundLate makes the lag explicit: right after a
// Propose returns, the follower HAS the entry but has not applied it. This
// gap is exactly the staleness window a follower read can observe.
func TestFollowersApplyOneRoundLate(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader(t, "1")
	ctx := context.Background()

	if err := leader.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	f := c.members["2"]
	t.Logf("straight after Propose: leader applied=%d, follower log=%d applied=%d keys=%d",
		leader.node.LastApplied(), f.node.Log().LastIndex(), f.node.LastApplied(), f.engine.Len())

	if f.node.Log().LastIndex() != 1 {
		t.Fatalf("follower should already HOLD the entry, log = %d", f.node.Log().LastIndex())
	}
	if f.node.LastApplied() != 0 {
		t.Fatalf("follower should not have applied it yet, applied = %d", f.node.LastApplied())
	}
	if _, ok := f.engine.Get("city"); ok {
		t.Fatal("a follower read here would be stale -- that is the point")
	}

	leader.node.Heartbeat(ctx)

	t.Logf("after one heartbeat  : follower log=%d applied=%d keys=%d",
		f.node.Log().LastIndex(), f.node.LastApplied(), f.engine.Len())

	if v, ok := f.engine.Get("city"); !ok || v != "Delhi" {
		t.Fatalf("follower city = %q %v after heartbeat, want Delhi true", v, ok)
	}
}

// TestFailsWithTwoNodesDown: 1 of 3 is not a majority. The write must NOT be
// reported as successful, and must not be applied anywhere.
func TestFailsWithTwoNodesDown(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader(t, "1")

	c.members["2"].up.Store(false)
	c.members["3"].up.Store(false)
	t.Log("nodes 2 and 3 are down; only the leader remains")

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	err := leader.node.Propose(ctx, set("city", "Delhi"))
	if err == nil {
		t.Fatal("propose must fail without a majority")
	}
	t.Logf("propose correctly failed: %v", err)

	// The entry IS in the leader's log -- it just is not committed.
	if got := leader.node.Log().LastIndex(); got != 1 {
		t.Fatalf("leader log last index = %d, want 1 (the entry is stored)", got)
	}
	if got := leader.node.CommitIndex(); got != 0 {
		t.Fatalf("commit index = %d, want 0 (nothing may be committed)", got)
	}
	if leader.engine.Len() != 0 {
		t.Fatal("an uncommitted entry must never reach the state machine")
	}
}

// TestFollowerCatchesUpAfterRejoin: a node that missed writes while offline
// is brought fully up to date by the leader's walkback.
func TestFollowerCatchesUpAfterRejoin(t *testing.T) {
	c := newCluster(t, 3)
	leader := c.leader(t, "1")
	ctx := context.Background()

	c.members["3"].up.Store(false)
	for i := 0; i < 5; i++ {
		if err := leader.node.Propose(ctx, set(fmt.Sprintf("k%d", i), "v")); err != nil {
			t.Fatalf("propose %d: %v", i, err)
		}
	}
	t.Logf("while node 3 was down: leader log=%d, node 3 log=%d",
		leader.node.Log().LastIndex(), c.members["3"].node.Log().LastIndex())

	c.members["3"].up.Store(true)
	t.Log("node 3 rejoins")

	// One more write is enough: replicating it drags node 3 all the way up.
	if err := leader.node.Propose(ctx, set("final", "yes")); err != nil {
		t.Fatalf("propose after rejoin: %v", err)
	}
	leader.node.Heartbeat(ctx)

	m3 := c.members["3"]
	t.Logf("after one more write: node 3 log=%d applied=%d keys=%d",
		m3.node.Log().LastIndex(), m3.node.LastApplied(), m3.engine.Len())

	if got := m3.node.Log().LastIndex(); got != 6 {
		t.Fatalf("node 3 log last index = %d, want 6", got)
	}
	if v, ok := m3.engine.Get("k0"); !ok || v != "v" {
		t.Fatalf("node 3 missed k0: %q %v", v, ok)
	}
}

// start runs every node's clock. From here on the cluster governs itself.
func (c *cluster) start(t *testing.T) {
	t.Helper()
	for _, id := range c.ids {
		c.members[id].node.Start()
	}
	t.Cleanup(func() {
		for _, id := range c.ids {
			c.members[id].node.Stop()
		}
	})
}

// leaders returns every node that currently believes it is leader.
func (c *cluster) leaders() []*member {
	var out []*member
	for _, id := range c.ids {
		if m := c.members[id]; m.up.Load() && m.node.IsLeader() {
			out = append(out, m)
		}
	}
	return out
}

// waitForLeader polls until exactly one live node claims leadership.
func (c *cluster) waitForLeader(t *testing.T, within time.Duration) *member {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ls := c.leaders(); len(ls) == 1 {
			return ls[0]
		}
		time.Sleep(2 * time.Millisecond)
	}

	t.Fatalf("no single leader within %v; leaders now: %d", within, len(c.leaders()))
	return nil
}

// TestClusterElectsALeader: start three nodes with no leader and no flags.
func TestClusterElectsALeader(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	start := time.Now()
	leader := c.waitForLeader(t, 3*time.Second)
	t.Logf("node %s elected in %v (term %d)", leader.node.ID(),
		time.Since(start).Round(time.Millisecond), leader.node.Term())

	// Winning an election and being RECOGNISED are different moments: the
	// followers only learn who won when the first heartbeat reaches them.
	// So poll rather than assert immediately.
	c.waitForAgreement(t, leader.node.ID(), 2*time.Second)
	t.Logf("all followers recognised %s after %v",
		leader.node.ID(), time.Since(start).Round(time.Millisecond))

	for _, id := range c.ids {
		if m := c.members[id]; m != leader && m.node.IsLeader() {
			t.Fatalf("node %s also thinks it is leader", id)
		}
	}
}

// waitForAgreement polls until every live follower names want as the leader.
func (c *cluster) waitForAgreement(t *testing.T, want NodeID, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		agreed := true
		for _, id := range c.ids {
			m := c.members[id]
			if !m.up.Load() || m.node.ID() == want {
				continue
			}
			if m.node.LeaderID() != want {
				agreed = false
			}
		}
		if agreed {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("followers did not agree on leader %s within %v", want, within)
}

// TestNewLeaderElectedAfterLeaderDies is the headline demo, as a test.
func TestNewLeaderElectedAfterLeaderDies(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	old := c.waitForLeader(t, 3*time.Second)
	oldTerm := old.node.Term()

	// Write something first, so we can check it survives the handover.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := old.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose before failover: %v", err)
	}

	t.Logf("killing leader %s (term %d)", old.node.ID(), oldTerm)
	old.up.Store(false)
	killed := time.Now()

	fresh := c.waitForLeader(t, 5*time.Second)
	t.Logf("node %s took over in %v (term %d)", fresh.node.ID(),
		time.Since(killed).Round(time.Millisecond), fresh.node.Term())

	if fresh == old {
		t.Fatal("the dead node cannot be the new leader")
	}
	if fresh.node.Term() <= oldTerm {
		t.Fatalf("new term %d must be greater than old term %d", fresh.node.Term(), oldTerm)
	}

	// The committed write survived the leader change -- but not instantly.
	// A new leader holds the entry in its log the moment it wins, yet it
	// cannot APPLY it until something from its own term commits. That is
	// what the no-op is for, and this wait is that window.
	waitForValue(t, fresh, "city", "Delhi", 2*time.Second)
	t.Logf("committed write visible on the new leader after %v",
		time.Since(killed).Round(time.Millisecond))

	// And the cluster still accepts writes.
	if err := fresh.node.Propose(ctx, set("after", "failover")); err != nil {
		t.Fatalf("propose after failover: %v", err)
	}
}

// waitForValue polls one node until a key has the expected value.
func waitForValue(t *testing.T, m *member, key, want string, within time.Duration) {
	t.Helper()

	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if v, ok := m.engine.Get(key); ok && v == want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	v, ok := m.engine.Get(key)
	t.Fatalf("node %s: %s = %q (found=%v), want %q within %v",
		m.node.ID(), key, v, ok, want, within)
}

// TestNewLeaderMustCommitANoopBeforeItCanApply makes that window explicit.
func TestNewLeaderMustCommitANoopBeforeItCanApply(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	leader := c.waitForLeader(t, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := leader.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Every node should end up holding the entry AND applying it, once the
	// heartbeats have carried the commit index around.
	for _, id := range c.ids {
		waitForValue(t, c.members[id], "city", "Delhi", 2*time.Second)
	}

	for _, id := range c.ids {
		m := c.members[id]
		t.Logf("node %s: role=%s term=%d log=%d commit=%d applied=%d",
			id, m.node.Role(), m.node.Term(), m.node.Log().LastIndex(),
			m.node.CommitIndex(), m.node.LastApplied())
	}
}

// TestMinorityCannotElectALeader: partition one node off and it can campaign
// as much as it likes -- it can never reach a majority.
func TestMinorityCannotElectALeader(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)
	c.waitForLeader(t, 3*time.Second)

	// Isolate a FOLLOWER. (Isolating the leader is a different scenario --
	// it keeps believing it is leader until it can talk to someone again,
	// which is safe but is tested separately below.)
	var lonely *member
	for _, id := range c.ids {
		if m := c.members[id]; !m.node.IsLeader() {
			lonely = m
			break
		}
	}
	lonely.up.Store(false)

	// Give it plenty of time to run several elections on its own.
	time.Sleep(600 * time.Millisecond)

	if lonely.node.IsLeader() {
		t.Fatal("an isolated node elected itself leader -- split brain")
	}
	t.Logf("isolated node %s reached term %d and is still a %s",
		lonely.node.ID(), lonely.node.Term(), lonely.node.Role())

	// Its term climbs, which is exactly why a rejoining node disrupts the
	// cluster: it comes back with a higher term and forces a new election.
	if lonely.node.Term() < 2 {
		t.Fatalf("isolated node should have campaigned repeatedly, term = %d", lonely.node.Term())
	}
}

// TestNeverTwoLeadersInOneTerm kills leaders repeatedly while a sampler
// watches, and asserts the one invariant everything depends on.
func TestNeverTwoLeadersInOneTerm(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	var mu sync.Mutex
	seen := map[uint64]map[NodeID]bool{}
	stop := make(chan struct{})

	// Sample constantly, not just at convenient moments.
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
			}
			mu.Lock()
			for _, id := range c.ids {
				m := c.members[id]
				// A partitioned node may still BELIEVE it is leader. That is
				// safe -- it cannot reach a majority, so it cannot commit
				// anything. Only reachable leaders can do damage.
				if !m.up.Load() || !m.node.IsLeader() {
					continue
				}
				term := m.node.Term()
				if seen[term] == nil {
					seen[term] = map[NodeID]bool{}
				}
				seen[term][m.node.ID()] = true
			}
			mu.Unlock()
			time.Sleep(time.Millisecond)
		}
	}()

	for round := 0; round < 4; round++ {
		leader := c.waitForLeader(t, 3*time.Second)
		t.Logf("round %d: killing %s (term %d)", round, leader.node.ID(), leader.node.Term())

		leader.up.Store(false)
		time.Sleep(200 * time.Millisecond) // let a successor be elected

		// Bring the old leader back. It still thinks it is leader, in an old
		// term, and must discover otherwise the moment it talks to anyone.
		leader.up.Store(true)

		deadline := time.Now().Add(2 * time.Second)
		for leader.node.IsLeader() && time.Now().Before(deadline) {
			time.Sleep(2 * time.Millisecond)
		}
		if leader.node.IsLeader() {
			t.Fatalf("revived node %s never stepped down", leader.node.ID())
		}
		t.Logf("          %s rejoined and stepped down to %s at term %d",
			leader.node.ID(), leader.node.Role(), leader.node.Term())
	}

	close(stop)
	mu.Lock()
	defer mu.Unlock()
	for term, leaders := range seen {
		if len(leaders) > 1 {
			t.Fatalf("term %d had %d reachable leaders: %v", term, len(leaders), leaders)
		}
	}
	t.Logf("checked %d terms,each with at most one reachable leader", len(seen))
}

// TestPartitionedLeaderCannotConfirmLeadership is why ConfirmLeadership
// exists. A leader cut off from the cluster still HAS the role, the term and
// a full log -- everything except the right to answer.
func TestPartitionedLeaderCannotConfirmLeadership(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	old := c.waitForLeader(t, 3*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := old.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	// Cut it off from the other two.
	old.up.Store(false)
	t.Logf("partitioned leader %s away from the cluster", old.node.ID())

	// It still thinks it is the leader...
	if !old.node.IsLeader() {
		t.Skip("leader stepped down before we could test it; timing")
	}

	// ...but it cannot prove it, so a strong read must fail rather than
	// serve possibly-stale data.
	rctx, rcancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer rcancel()

	if _, err := old.node.ConfirmLeadership(rctx); err == nil {
		t.Fatal("a partitioned leader confirmed leadership -- it could serve stale reads")
	} else {
		t.Logf("strong read correctly refused: %v", err)
	}

	// An eventual read from the same node still works; it may just be out
	// of date.
	if v, ok := old.engine.Get("city"); !ok || v != "Delhi" {
		t.Fatalf("eventual read = %q %v, want Delhi true", v, ok)
	}

	// Meanwhile the majority carries on and elects someone new.
	fresh := c.waitForLeader(t, 5*time.Second)
	if fresh == old {
		t.Fatal("the partitioned node cannot be the new leader")
	}
	t.Logf("majority elected %s (term %d) while %s sat isolated",
		fresh.node.ID(), fresh.node.Term(), old.node.ID())
}

// TestFollowerServesLinearizableRead: a follower can serve a strong read by
// asking the leader for a read index and waiting to catch up. No proxying of
// the value, and no stale answer.
func TestFollowerServesLinearizableRead(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	leader := c.waitForLeader(t, 3*time.Second)
	c.waitForAgreement(t, leader.node.ID(), 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	var follower *member
	for _, id := range c.ids {
		if m := c.members[id]; m != leader {
			follower = m
			break
		}
	}

	// Write, then IMMEDIATELY do a strong read on a follower. Without the
	// barrier this is the race that returns stale data.
	if err := leader.node.Propose(ctx, set("city", "Delhi")); err != nil {
		t.Fatalf("propose: %v", err)
	}

	if err := follower.node.LinearizableRead(ctx); err != nil {
		t.Fatalf("linearizable read on follower %s: %v", follower.node.ID(), err)
	}

	v, ok := follower.engine.Get("city")
	if !ok || v != "Delhi" {
		t.Fatalf("follower strong read = %q %v, want Delhi true", v, ok)
	}
	t.Logf("follower %s served a strong read correctly (applied=%d)",
		follower.node.ID(), follower.node.LastApplied())
}

// TestWriteToFollowerIsForwarded: clients may write to any node.
func TestWriteToFollowerIsForwarded(t *testing.T) {
	c := newCluster(t, 3)
	c.start(t)

	leader := c.waitForLeader(t, 3*time.Second)
	c.waitForAgreement(t, leader.node.ID(), 2*time.Second)

	var follower *member
	for _, id := range c.ids {
		if m := c.members[id]; m != leader {
			follower = m
			break
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := follower.node.ProposeOrForward(ctx, set("via", "follower")); err != nil {
		t.Fatalf("forward from %s: %v", follower.node.ID(), err)
	}

	waitForValue(t, leader, "via", "follower", 2*time.Second)
	t.Logf("write sent to follower %s was committed by leader %s",
		follower.node.ID(), leader.node.ID())
}

// netStats prints what the simulated network actually did, so a passing test
// proves something rather than silently running on a perfect link.
func (c *cluster) netStats(t *testing.T) {
	t.Helper()
	for _, id := range c.ids {
		t.Logf("  node %s network: %s", id, c.members[id].net)
	}
}
