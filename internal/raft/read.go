package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/himalisaini/raftkv/internal/wal"
)

// ErrNoLeader means this node does not currently know who the leader is,
// usually because an election is in progress.
var ErrNoLeader = errors.New("raft: no known leader")

// ErrLeadershipLost means we believed we were leader but could not prove it
// to a majority. Almost always: this node has been partitioned off.
var ErrLeadershipLost = errors.New("raft: could not confirm leadership")

// ReadIndex returns an index such that a node which has applied up to it can
// serve a linearizable read -- one that reflects every write acknowledged
// before the read began.
//
// On a leader it confirms leadership first. On a follower it asks the leader.
// Either way the caller then waits for its own state machine to catch up.
func (n *Node) ReadIndex(ctx context.Context) (uint64, error) {
	if n.IsLeader() {
		return n.ConfirmLeadership(ctx)
	}

	leader, ok := n.leaderPeer()
	if !ok {
		return 0, ErrNoLeader
	}

	resp, err := n.tr.ReadIndex(ctx, leader, ReadIndexRequest{
		Term:       n.Term(),
		FromNodeID: n.cfg.ID,
	})
	if err != nil {
		return 0, fmt.Errorf("raft: read index from %s: %w", leader.ID, err)
	}
	if !resp.IsLeader {
		return 0, ErrNoLeader
	}
	return resp.ReadIndex, nil
}

// ConfirmLeadership proves this node is still the leader, right now.
//
// Being the leader is not the same as being able to prove it. A leader that
// has been partitioned away still has the role, the term and a full log --
// nothing has told it otherwise. Reads served from that state can return
// data a newer leader has already superseded.
//
// The proof is simple: exchange a heartbeat with a majority. A majority that
// still accepts our term cannot simultaneously have elected someone else.
// confirmBatch is one leadership-confirmation round that any number of
// concurrent readers can share the result of.
type confirmBatch struct {
	done  chan struct{}
	index uint64
	err   error
}

// ConfirmLeadership proves this node is still the leader, right now, and
// shares one round trip between every read waiting for one.
//
// Without batching, 16 concurrent strong reads mean 16 confirmation rounds,
// each hitting every peer. With it they collapse into one -- so strong-read
// throughput stops being bounded by how fast the leader can talk to its
// followers.
func (n *Node) ConfirmLeadership(ctx context.Context) (uint64, error) {
	if !n.IsLeader() {
		return 0, ErrNotLeader
	}

	// Benchmark path: every reader runs its own round.
	if n.noReadBatching {
		rctx, cancel := context.WithTimeout(ctx, n.heartbeatInterval*10)
		return n.confirmRound(rctx, cancel)
	}

	n.confirmMu.Lock()
	if n.confirmNext == nil {
		n.confirmNext = &confirmBatch{done: make(chan struct{})}
	}
	batch := n.confirmNext

	// Join the batch that has NOT started yet, never the one in flight. A
	// running round captured its commit index before we arrived, so its
	// answer may predate our read -- which would not be linearizable.
	startIt := !n.confirmRunning
	if startIt {
		n.confirmRunning = true
		n.confirmNext = nil
	}
	n.confirmMu.Unlock()

	if startIt {
		go n.runConfirm(batch)
	}

	select {
	case <-batch.done:
		return batch.index, batch.err
	case <-ctx.Done():
		return 0, fmt.Errorf("%w: %v", ErrLeadershipLost, ctx.Err())
	}
}

// runConfirm executes rounds back to back for as long as readers keep
// queueing up, then stops.
//
// This is a LOOP, not recursion, and that is load-bearing. Go does not
// optimise tail calls, so recursing into the next batch grows this
// goroutine's stack by one frame per round. Under sustained strong-read
// load the next batch is always non-empty, so the stack grows without
// bound -- and every time Go doubles a stack it copies the whole thing,
// which stalls the goroutine long enough for followers to miss heartbeats
// and start elections. Measured: ~35k reads/sec with 36k errors and
// leadership churning through three terms in twelve seconds.
func (n *Node) runConfirm(batch *confirmBatch) {
	for batch != nil {
		ctx, cancel := context.WithTimeout(context.Background(), n.heartbeatInterval*10)
		batch.index, batch.err = n.confirmRound(ctx, cancel)
		close(batch.done)

		n.confirmMu.Lock()
		next := n.confirmNext
		if next != nil {
			n.confirmNext = nil // this round takes the queued batch
		} else {
			n.confirmRunning = false
		}
		n.confirmMu.Unlock()

		batch = next
	}
}

// confirmRound is the actual proof: capture the commit index, then exchange
// a heartbeat with a majority.
func (n *Node) confirmRound(ctx context.Context, cancelRound context.CancelFunc) (uint64, error) {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return 0, ErrNotLeader
	}
	// Capture the commit index BEFORE the round trip. Anything committed
	// later did not exist when the read began, so it need not be visible.
	readIndex := n.commitIndex
	n.mu.Unlock()

	// A cluster of one is its own majority; there is nobody to ask.
	if len(n.cfg.Peers) == 0 {
		return readIndex, nil
	}

	// The channel is buffered, so a probe never blocks on send even after
	// we stop reading.
	acks := make(chan bool, len(n.cfg.Peers))

	var wg sync.WaitGroup
	for _, p := range n.cfg.Peers {
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			acks <- n.probe(ctx, p)
		}(p)
	}

	// We return as soon as a majority answers, which leaves the remaining
	// probes in flight. Cancelling the context at that moment would abort
	// those requests mid-response, and an aborted HTTP request cannot be
	// returned to the idle connection pool -- so its TCP connection is torn
	// down. At thousands of rounds per second that churns thousands of
	// connections per second, overflows the peer's listen backlog, and
	// starts refusing the leader's HEARTBEATS. Followers then time out and
	// call elections in the middle of a pure read workload.
	//
	// So: let the stragglers finish, and cancel only once they have.
	go func() {
		wg.Wait()
		cancelRound()
	}()

	got := 1 // ourselves
	replies := 0
	need := n.cfg.Majority()

	for got < need {
		if replies == len(n.cfg.Peers) {
			return 0, ErrLeadershipLost
		}
		select {
		case ok := <-acks:
			replies++
			if ok {
				got++
			}
		case <-ctx.Done():
			return 0, fmt.Errorf("%w: %v", ErrLeadershipLost, ctx.Err())
		}
	}

	// We may have been deposed during the round trip.
	if !n.IsLeader() {
		return 0, ErrNotLeader
	}
	return readIndex, nil
}

// WaitForApplied blocks until this node's state machine has applied index.
func (n *Node) WaitForApplied(ctx context.Context, index uint64) error {
	for {
		n.mu.Lock()
		if n.lastApplied >= index {
			n.mu.Unlock()
			return nil
		}
		// Grab the current notification channel while we hold the lock, so
		// we cannot miss a wake-up that happens between here and the select.
		wait := n.applyWait
		n.mu.Unlock()

		select {
		case <-wait:
			// lastApplied moved; loop round and check again.
		case <-ctx.Done():
			return fmt.Errorf("raft: waiting to apply index %d: %w", index, ctx.Err())
		}
	}
}

// LinearizableRead performs the full barrier: get a read index, then wait
// until this node has applied it. After it returns, a local read is safe.
func (n *Node) LinearizableRead(ctx context.Context) error {
	index, err := n.ReadIndex(ctx)
	if err != nil {
		return err
	}
	return n.WaitForApplied(ctx, index)
}

// HandleReadIndex answers another node's ReadIndex request.
func (n *Node) HandleReadIndex(ctx context.Context, req ReadIndexRequest) (ReadIndexResponse, error) {
	n.mu.Lock()
	if req.Term > n.currentTerm {
		if err := n.setTermLocked(req.Term); err != nil {
			n.mu.Unlock()
			return ReadIndexResponse{}, err
		}
	}
	term := n.currentTerm
	n.mu.Unlock()

	index, err := n.ConfirmLeadership(ctx)
	if err != nil {
		return ReadIndexResponse{Term: term, IsLeader: false}, nil
	}
	return ReadIndexResponse{Term: term, IsLeader: true, ReadIndex: index}, nil
}

// ProposeOrForward runs a write here if we are the leader, and hands it to
// the leader if we are not, so a client can write to any node.
func (n *Node) ProposeOrForward(ctx context.Context, cmd wal.Record) error {
	if n.IsLeader() {
		return n.Propose(ctx, cmd)
	}

	leader, ok := n.leaderPeer()
	if !ok {
		return ErrNoLeader
	}

	resp, err := n.tr.Forward(ctx, leader, ForwardRequest{Command: cmd})
	if err != nil {
		return fmt.Errorf("raft: forward to %s: %w", leader.ID, err)
	}
	if !resp.Success {
		if resp.Error != "" {
			return fmt.Errorf("raft: leader %s refused the write: %s", leader.ID, resp.Error)
		}
		return ErrNoLeader
	}
	return nil
}

// HandleForward is the leader side of a forwarded write.
func (n *Node) HandleForward(ctx context.Context, req ForwardRequest) ForwardResponse {
	if err := n.Propose(ctx, req.Command); err != nil {
		return ForwardResponse{Success: false, LeaderID: n.LeaderID(), Error: err.Error()}
	}
	return ForwardResponse{Success: true, LeaderID: n.cfg.ID}
}

// leaderPeer looks up the current leader in our peer list.
func (n *Node) leaderPeer() (Peer, bool) {
	id := n.LeaderID()
	if id == "" || id == n.cfg.ID {
		return Peer{}, false
	}
	return n.cfg.PeerByID(id)
}

// probe asks one peer "do you still accept me as leader in this term?" and
// nothing else.
//
// It deliberately does NOT reuse replicateTo. Replication carries entries and
// mutates nextIndex/matchIndex, so concurrent strong reads would send the
// same entries repeatedly and fight over that bookkeeping. A probe is
// anchored at the sentinel (index 0, term 0), which every log matches, and
// carries no entries -- so it costs one small round trip and changes nothing.
func (n *Node) probe(ctx context.Context, p Peer) bool {
	n.mu.Lock()
	if n.role != Leader {
		n.mu.Unlock()
		return false
	}
	req := AppendEntriesRequest{
		Term:         n.currentTerm,
		LeaderID:     n.cfg.ID,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		LeaderCommit: n.commitIndex,
	}
	n.mu.Unlock()

	resp, err := n.tr.AppendEntries(ctx, p, req)
	if err != nil {
		return false
	}

	if resp.Term > n.Term() {
		n.mu.Lock()
		_ = n.setTermLocked(resp.Term)
		n.mu.Unlock()
		return false
	}
	return resp.Success
}
