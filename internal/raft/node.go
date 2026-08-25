package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/himalisaini/raftkv/internal/wal"
)

// Role is what this node currently believes it is.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

func (r Role) String() string {
	switch r {
	case Leader:
		return "leader"
	case Candidate:
		return "candidate"
	default:
		return "follower"
	}
}

// ErrNotLeader is returned when a write arrives at a node that cannot commit
// it. The caller should retry against the leader.
var ErrNotLeader = errors.New("raft: not the leader")

// maxWalkbackAttempts bounds how far one replication call will back up
// looking for a match point, so a pathological peer cannot spin forever.
const maxWalkbackAttempts = 64

// ApplyFunc is how committed entries reach the state machine.
type ApplyFunc func(wal.Record)

// Options collects everything a Node needs. A struct rather than positional
// parameters, because five arguments at a call site is unreadable and the
// list is still growing.
type Options struct {
	Config    Config
	Log       *Log
	State     *StateStore // nil means state is not persisted (tests only)
	Transport Transport
	Apply     ApplyFunc

	// DisableReadBatching makes every strong read run its own leadership
	// confirmation round instead of sharing one. It exists so the batching
	// optimisation can be measured against the unbatched path rather than
	// asserted; nothing should turn it on in production.
	DisableReadBatching bool

	// Timings. Zero values fall back to the defaults in election.go.
	// Tests shrink these so an election takes milliseconds, not seconds.
	HeartbeatInterval  time.Duration
	ElectionTimeoutMin time.Duration
	ElectionTimeoutMax time.Duration
}

// Node is one member of the cluster.
type Node struct {
	cfg   Config
	log   *Log
	tr    Transport
	apply ApplyFunc
	state *StateStore

	mu sync.Mutex

	// currentTerm and votedFor are the persistent pair: both are written to
	// disk before this node replies to any RPC.
	currentTerm uint64
	votedFor    NodeID

	role     Role
	leaderID NodeID

	// lastHeard is when we last heard from a current leader or granted a
	// vote. The election timeout is measured from here.
	lastHeard time.Time

	heartbeatInterval  time.Duration
	electionTimeoutMin time.Duration
	electionTimeoutMax time.Duration

	cancel context.CancelFunc
	wg     sync.WaitGroup

	// Batching for leadership confirmation; see ConfirmLeadership.
	noReadBatching bool
	confirmMu      sync.Mutex
	confirmNext    *confirmBatch
	confirmRunning bool

	// commitIndex: the highest entry known to be stored on a majority.
	// lastApplied: the highest entry handed to the state machine.
	// lastApplied always trails commitIndex, never leads it.
	commitIndex uint64
	lastApplied uint64

	// applyWait is closed (and replaced) every time lastApplied advances.
	// It is Go's idiomatic broadcast: a waiter grabs the current channel
	// under the lock, then selects on it. Closing wakes every waiter at
	// once, and replacing it arms the next round.
	applyWait chan struct{}

	// Leader-only bookkeeping, one entry per peer.
	//   nextIndex  = the next entry we will TRY to send (a guess)
	//   matchIndex = the highest entry we KNOW they have (a fact)
	nextIndex  map[NodeID]uint64
	matchIndex map[NodeID]uint64

	// applyMu serializes the apply loop so entries reach the state machine
	// strictly in index order, even with concurrent proposals.
	applyMu sync.Mutex
}

// NewNode builds a node from its options and its recovered persistent state.
func NewNode(opts Options, restored PersistentState) (*Node, error) {
	if err := opts.Config.Validate(); err != nil {
		return nil, err
	}
	apply := opts.Apply
	if apply == nil {
		apply = func(wal.Record) {}
	}

	n := &Node{
		cfg:   opts.Config,
		log:   opts.Log,
		tr:    opts.Transport,
		apply: apply,
		state: opts.State,

		noReadBatching:     opts.DisableReadBatching,
		heartbeatInterval:  orDefault(opts.HeartbeatInterval, defaultHeartbeatInterval),
		electionTimeoutMin: orDefault(opts.ElectionTimeoutMin, defaultElectionTimeoutMin),
		electionTimeoutMax: orDefault(opts.ElectionTimeoutMax, defaultElectionTimeoutMax),
		lastHeard:          time.Now(),

		// Come back exactly where we left off, not at term 0.
		currentTerm: restored.CurrentTerm,
		votedFor:    restored.VotedFor,

		nextIndex:  make(map[NodeID]uint64),
		matchIndex: make(map[NodeID]uint64),
		applyWait:  make(chan struct{}),
	}

	if n.electionTimeoutMax <= n.electionTimeoutMin {
		return nil, fmt.Errorf("raft: election timeout max (%v) must exceed min (%v)",
			n.electionTimeoutMax, n.electionTimeoutMin)
	}
	return n, nil
}

func orDefault(v, fallback time.Duration) time.Duration {
	if v <= 0 {
		return fallback
	}
	return v
}

// VotedFor is who this node voted for in the current term, or "".
func (n *Node) VotedFor() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.votedFor
}

// persistLocked writes currentTerm and votedFor to disk.
func (n *Node) persistLocked() error {
	if n.state == nil {
		return nil
	}
	return n.state.Save(PersistentState{
		CurrentTerm: n.currentTerm,
		VotedFor:    n.votedFor,
	})
}

// setTermLocked moves to a new, higher term. Entering a new term always
// clears the vote: one vote per node per term, and this is a new term.
func (n *Node) setTermLocked(term uint64) error {
	n.currentTerm = term
	n.votedFor = ""
	n.leaderID = ""
	n.role = Follower
	return n.persistLocked()
}

// logUpToDateLocked implements the Election Restriction: is the candidate's
// log at least as complete as ours?
//
// A later term always wins. Within the same term, the longer log wins. A
// voter that refuses candidates less complete than itself guarantees that
// any node able to win an election already holds every committed entry.
func (n *Node) logUpToDateLocked(lastIndex, lastTerm uint64) bool {
	ourIndex, ourTerm := n.log.LastIndex(), n.log.LastTerm()

	if lastTerm != ourTerm {
		return lastTerm > ourTerm
	}
	return lastIndex >= ourIndex
}

// HandleRequestVote is called when a candidate asks us for our vote.
func (n *Node) HandleRequestVote(req RequestVoteRequest) (RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	deny := func() RequestVoteResponse {
		return RequestVoteResponse{Term: n.currentTerm, VoteGranted: false}
	}

	// Rule 1: never help a candidate from a term older than ours.
	if req.Term < n.currentTerm {
		return deny(), nil
	}

	// Rule 2: a higher term means a new election. Adopt it and clear our
	// old vote, because that vote belonged to a term that is now history.
	if req.Term > n.currentTerm {
		if err := n.setTermLocked(req.Term); err != nil {
			return RequestVoteResponse{}, err
		}
	}

	// Rule 3: one vote per term. If we already voted for someone else in
	// this term, we cannot vote again -- this is what makes two leaders in
	// one term impossible. Re-asking the SAME candidate is fine, so a
	// retried request is not treated as a second vote.
	if n.votedFor != "" && n.votedFor != req.CandidateID {
		return deny(), nil
	}

	// Rule 4: the Election Restriction.
	if !n.logUpToDateLocked(req.LastLogIndex, req.LastLogTerm) {
		return deny(), nil
	}

	// Grant it -- and get it on disk BEFORE replying. If we replied first
	// and crashed, we could wake up with no memory of this vote and grant a
	// second one in the same term.
	n.votedFor = req.CandidateID
	if err := n.persistLocked(); err != nil {
		return RequestVoteResponse{}, fmt.Errorf("raft: persist vote: %w", err)
	}

	// We just helped someone else's election, so give them time to win it
	// before starting one of our own.
	n.noteContactLocked()

	return RequestVoteResponse{Term: n.currentTerm, VoteGranted: true}, nil
}

func (n *Node) ID() NodeID { return n.cfg.ID }
func (n *Node) Log() *Log  { return n.log }

func (n *Node) Term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.currentTerm
}

func (n *Node) Role() Role {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.role
}

func (n *Node) IsLeader() bool { return n.Role() == Leader }

// LeaderID is who we currently believe the leader is, or "" if unknown.
func (n *Node) LeaderID() NodeID {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.leaderID
}

func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.commitIndex
}

func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.lastApplied
}

// BecomeLeader promotes this node without running an election.
//
// Normal operation never calls this -- leadership comes from winning a vote.
// It exists so tests can pin leadership to a known node and exercise
// replication without waiting on election timing.
func (n *Node) BecomeLeader() {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.currentTerm++
	n.votedFor = n.cfg.ID // a leader has, in effect, voted for itself
	_ = n.persistLocked()
	n.becomeLeaderLocked()
}

// ---------------------------------------------------------------- leader side

// Propose replicates one command and returns once it is committed and
// applied. It returns ErrNotLeader if this node cannot commit.
func (n *Node) Propose(ctx context.Context, cmd wal.Record) error {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.currentTerm
	n.mu.Unlock()

	// 1. Durable on the leader first. This entry is now a candidate for
	//    commitment, but is NOT yet committed.
	entries, err := n.log.Append(term, cmd)
	if err != nil {
		return fmt.Errorf("raft: append to leader log: %w", err)
	}
	index := entries[0].Index

	// 2. Send to every peer at once. The leader's own copy counts as the
	//    first acknowledgement, which is why a 1-node cluster commits
	//    immediately and a 3-node cluster needs only one follower.
	acks := make(chan bool, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		go func(p Peer) { acks <- n.replicateTo(ctx, p) }(p)
	}

	got := 1 // the leader's own copy
	need := n.cfg.Majority()
	replies := 0

	for got < need {
		if replies == len(n.cfg.Peers) {
			// Every peer has answered and we are still short. Waiting longer
			// cannot help, so fail now rather than at the context deadline.
			return fmt.Errorf("raft: index %d not committed: %d acknowledgements, need %d of %d",
				index, got, need, n.cfg.ClusterSize())
		}

		select {
		case ok := <-acks:
			replies++
			if ok {
				got++
			}
		case <-ctx.Done():
			return fmt.Errorf("raft: index %d not committed: %w", index, ctx.Err())
		}
	}

	// 3. A majority has it. Work out how far that lets us commit, then feed
	//    the newly committed entries to the state machine.
	n.advanceCommit()
	n.applyCommitted()

	if n.CommitIndex() < index {
		return fmt.Errorf("raft: index %d acknowledged but not committed", index)
	}
	return nil
}

// Heartbeat sends one round of AppendEntries to every peer and then
// re-evaluates what is committed.
//
// It exists because of a timing detail worth understanding: a follower only
// learns the leader's commit index from a message. When Propose returns, the
// entry is committed on the LEADER, but the followers were told the old
// commit index -- they hold the entry in their logs without having applied
// it. The next message closes that gap.
//
// The leader loop calls this on a fixed ticker, which is what bounds how
// stale a follower read can be.
func (n *Node) Heartbeat(ctx context.Context) {
	if !n.IsLeader() {
		return
	}

	var wg sync.WaitGroup
	for _, p := range n.cfg.Peers {
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			n.replicateTo(ctx, p)
		}(p)
	}
	wg.Wait()

	n.advanceCommit()
	n.applyCommitted()
}

// replicateTo brings one peer up to date, backing up through the log until it
// finds a matching entry. Returns true if the peer now has our entries.
func (n *Node) replicateTo(ctx context.Context, p Peer) bool {
	for attempt := 0; attempt < maxWalkbackAttempts; attempt++ {
		n.mu.Lock()
		if n.role != Leader {
			n.mu.Unlock()
			return false
		}
		req := AppendEntriesRequest{
			Term:         n.currentTerm,
			LeaderID:     n.cfg.ID,
			PrevLogIndex: n.nextIndex[p.ID] - 1,
			LeaderCommit: n.commitIndex,
		}
		n.mu.Unlock()

		prev, _ := n.log.At(req.PrevLogIndex) // index 0 gives term 0: the sentinel
		req.PrevLogTerm = prev.Term
		req.Entries = n.log.EntriesFrom(req.PrevLogIndex + 1)

		resp, err := n.tr.AppendEntries(ctx, p, req)
		if err != nil {
			return false // peer unreachable: not an ack, not a rejection
		}

		n.mu.Lock()
		switch {
		case resp.Term > n.currentTerm:
			// Someone else has been elected. We are not the leader any more.
			if err := n.setTermLocked(resp.Term); err != nil {
				n.mu.Unlock()
				return false
			}
			n.mu.Unlock()
			return false

		case resp.Success:
			n.matchIndex[p.ID] = req.PrevLogIndex + uint64(len(req.Entries))
			n.nextIndex[p.ID] = n.matchIndex[p.ID] + 1
			n.mu.Unlock()
			return true

		default:
			// Rejected. If the follower told us its log is shorter than our
			// guess, jump straight there instead of crawling back one index
			// per round trip. Otherwise back up by one.
			if resp.LastLogIndex+1 < n.nextIndex[p.ID] {
				n.nextIndex[p.ID] = resp.LastLogIndex + 1
			} else if n.nextIndex[p.ID] > 1 {
				n.nextIndex[p.ID]--
			}
			n.mu.Unlock()
		}
	}
	return false
}

// advanceCommit finds the highest index stored on a majority and commits it.
func (n *Node) advanceCommit() {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.role != Leader {
		return
	}

	for N := n.log.LastIndex(); N > n.commitIndex; N-- {
		e, ok := n.log.At(N)
		if !ok {
			continue
		}

		// A leader may only commit by counting replicas for entries from its
		// OWN term. Entries from earlier terms get committed indirectly, once
		// a current-term entry above them commits. Skipping this rule allows
		// a committed entry to be overwritten later (Figure 8 in the paper).
		if e.Term != n.currentTerm {
			return
		}

		count := 1 // the leader has it
		for _, p := range n.cfg.Peers {
			if n.matchIndex[p.ID] >= N {
				count++
			}
		}

		if count >= n.cfg.Majority() {
			n.commitIndex = N
			return
		}
	}
}

// ---------------------------------------------------------------- both sides

// applyCommitted hands every newly committed entry to the state machine, in
// index order, exactly once.
func (n *Node) applyCommitted() {
	n.applyMu.Lock()
	defer n.applyMu.Unlock()

	for {
		n.mu.Lock()
		next := n.lastApplied + 1
		if next > n.commitIndex {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()

		entry, ok := n.log.At(next)
		if !ok {
			return // committed but not stored: cannot happen, but do not spin
		}

		n.apply(entry)

		n.mu.Lock()
		n.lastApplied = next
		close(n.applyWait)                // wake everyone waiting on progress
		n.applyWait = make(chan struct{}) // and arm the next wait
		n.mu.Unlock()
	}
}

// -------------------------------------------------------------- follower side

// HandleAppendEntries is called when a leader sends us entries.
func (n *Node) HandleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	resp, err := n.handleAppendEntries(req)
	if err != nil {
		return resp, err
	}

	// Apply outside the node lock: the state machine may be slow, and
	// holding n.mu across it would block every other RPC.
	n.applyCommitted()
	return resp, nil
}

func (n *Node) handleAppendEntries(req AppendEntriesRequest) (AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Rule 1: reject anything from a leader older than us.
	if req.Term < n.currentTerm {
		return n.replyLocked(false), nil
	}

	// Rule 2: a term at least as high as ours is authoritative.
	if req.Term > n.currentTerm {
		if err := n.setTermLocked(req.Term); err != nil {
			return AppendEntriesResponse{}, fmt.Errorf("raft: persist term: %w", err)
		}
	}
	// Whoever sent this is the leader for this term, so step down to follower
	// and restart the election countdown.
	n.role = Follower
	n.leaderID = req.LeaderID
	n.noteContactLocked()

	// Rule 3: log matching plus conflict repair.
	ok, err := n.log.AppendAfter(req.PrevLogIndex, req.PrevLogTerm, req.Entries)
	if err != nil {
		return AppendEntriesResponse{}, fmt.Errorf("raft: append: %w", err)
	}
	if !ok {
		return n.replyLocked(false), nil
	}

	// Rule 4: adopt the leader's commit index, capped by our own log.
	if req.LeaderCommit > n.commitIndex {
		lastNew := req.PrevLogIndex + uint64(len(req.Entries))
		n.commitIndex = min(req.LeaderCommit, lastNew)
	}

	return n.replyLocked(true), nil
}

func (n *Node) replyLocked(success bool) AppendEntriesResponse {
	return AppendEntriesResponse{
		Term:         n.currentTerm,
		Success:      success,
		LastLogIndex: n.log.LastIndex(),
	}
}
