package raft

import (
	"context"
	"log"
	"math/rand"
	"time"

	"github.com/himalisaini/raftkv/internal/wal"
)

// Default timings. The rule that matters is
//
//	heartbeat  <<  electionTimeoutMin  <<  mean time between failures
//
// Heartbeats must be frequent enough that a healthy leader always resets
// every follower's timer with room to spare, or followers will start
// elections against a leader that is perfectly alive.
const (
	defaultHeartbeatInterval  = 50 * time.Millisecond
	defaultElectionTimeoutMin = 150 * time.Millisecond
	defaultElectionTimeoutMax = 300 * time.Millisecond
)

// Start runs the node's own clock: heartbeats when leader, election timeouts
// when not. It returns immediately; call Stop to shut it down.
func (n *Node) Start() {
	ctx, cancel := context.WithCancel(context.Background())

	n.mu.Lock()
	n.cancel = cancel
	n.lastHeard = time.Now()
	n.mu.Unlock()

	n.wg.Add(1)
	go func() {
		defer n.wg.Done()
		n.run(ctx)
	}()
}

// Stop halts the loop and waits for it to finish.
func (n *Node) Stop() {
	n.mu.Lock()
	cancel := n.cancel
	n.cancel = nil
	n.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	n.wg.Wait()
}

// run switches between the two behaviours a node can have. Each inner loop
// returns as soon as the role changes, and this loop picks the other one.
func (n *Node) run(ctx context.Context) {
	for ctx.Err() == nil {
		if n.IsLeader() {
			n.runLeader(ctx)
		} else {
			n.runFollower(ctx)
		}
	}
}

// runLeader heartbeats on a fixed interval. The heartbeat is what stops
// followers from timing out, and what carries the commit index to them.
func (n *Node) runLeader(ctx context.Context) {
	ticker := time.NewTicker(n.heartbeatInterval)
	defer ticker.Stop()

	// Send one immediately rather than waiting a full interval, so a brand
	// new leader suppresses competing elections as fast as possible.
	n.beat(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if !n.IsLeader() {
				return // deposed: let run() switch us to the follower loop
			}
			n.beat(ctx)
		}
	}
}

func (n *Node) beat(ctx context.Context) {
	hctx, cancel := context.WithTimeout(ctx, n.heartbeatInterval*3)
	defer cancel()
	n.Heartbeat(hctx)
}

// runFollower waits for a heartbeat. If none arrives before the timeout
// expires, it assumes the leader is gone and stands for election.
func (n *Node) runFollower(ctx context.Context) {
	for {
		timeout := n.randomElectionTimeout()

		timer := time.NewTimer(timeout)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}

		if n.IsLeader() {
			return
		}

		n.mu.Lock()
		idle := time.Since(n.lastHeard)
		n.mu.Unlock()

		if idle < timeout {
			continue // a heartbeat arrived while we were waiting; keep waiting
		}

		n.campaign(ctx)
		return // role may have changed; let run() re-decide
	}
}

// randomElectionTimeout is deliberately random.
//
// If every follower used the same timeout they would all campaign at the same
// instant, split the vote three ways, all fail, and repeat -- potentially
// forever. Randomising means one node almost always times out first and wins
// before the others even start.
func (n *Node) randomElectionTimeout() time.Duration {
	spread := n.electionTimeoutMax - n.electionTimeoutMin
	return n.electionTimeoutMin + time.Duration(rand.Int63n(int64(spread)))
}

// campaign runs one election with this node as the candidate.
func (n *Node) campaign(ctx context.Context) {
	n.mu.Lock()

	// Step 1: a new term, and vote for yourself. Both are persisted before
	// a single vote request goes out.
	n.currentTerm++
	n.votedFor = n.cfg.ID
	n.role = Candidate
	n.leaderID = ""
	n.lastHeard = time.Now()

	term := n.currentTerm
	req := RequestVoteRequest{
		Term:         term,
		CandidateID:  n.cfg.ID,
		LastLogIndex: n.log.LastIndex(),
		LastLogTerm:  n.log.LastTerm(),
	}

	if err := n.persistLocked(); err != nil {
		n.mu.Unlock()
		log.Printf("raft: node %s cannot persist its candidacy: %v", n.cfg.ID, err)
		return
	}
	n.mu.Unlock()

	// Step 2: ask everyone at once.
	votes := make(chan bool, len(n.cfg.Peers))
	for _, p := range n.cfg.Peers {
		go func(p Peer) {
			resp, err := n.tr.RequestVote(ctx, p, req)
			if err != nil {
				votes <- false
				return
			}

			// A higher term in the reply means this election is already
			// obsolete. Step down rather than keep counting.
			n.mu.Lock()
			if resp.Term > n.currentTerm {
				_ = n.setTermLocked(resp.Term)
				n.mu.Unlock()
				votes <- false
				return
			}
			n.mu.Unlock()

			votes <- resp.VoteGranted
		}(p)
	}

	// Step 3: count. We already voted for ourselves, so start at one.
	granted := 1
	replies := 0
	need := n.cfg.Majority()

	for granted < need {
		if replies == len(n.cfg.Peers) {
			return // every peer answered and we fell short: election lost
		}
		select {
		case ok := <-votes:
			replies++
			if ok {
				granted++
			}
		case <-ctx.Done():
			return
		}
	}

	n.winElection(term)
}

// winElection promotes this node, but only if the election it just won is
// still the current one.
func (n *Node) winElection(term uint64) {
	n.mu.Lock()

	// While we were counting votes we may have seen a higher term and
	// stepped down. Winning a stale election must not resurrect us.
	if n.role != Candidate || n.currentTerm != term {
		n.mu.Unlock()
		return
	}

	n.becomeLeaderLocked()
	n.mu.Unlock()

	log.Printf("raft: node %s elected leader for term %d", n.cfg.ID, term)

	// A new leader appends a no-op from its own term. Without it, entries
	// left over from previous terms could never be committed on an idle
	// cluster, because a leader may only commit by counting replicas for
	// entries from its current term.
	ctx, cancel := context.WithTimeout(context.Background(), n.heartbeatInterval*10)
	defer cancel()
	if err := n.Propose(ctx, wal.Record{Op: wal.OpNoop}); err != nil {
		log.Printf("raft: node %s could not commit its no-op: %v", n.cfg.ID, err)
	}
}

// becomeLeaderLocked sets up the leader-only bookkeeping.
func (n *Node) becomeLeaderLocked() {
	n.role = Leader
	n.leaderID = n.cfg.ID

	for _, p := range n.cfg.Peers {
		// Optimistic: assume every follower is caught up. Rejections tell
		// us otherwise, and replicateTo walks back from there.
		n.nextIndex[p.ID] = n.log.LastIndex() + 1
		n.matchIndex[p.ID] = 0
	}
}

// noteContactLocked records that we heard from a current leader or granted a
// vote. Both reset the election timer: the cluster is making progress, so
// there is no reason for us to disrupt it.
func (n *Node) noteContactLocked() {
	n.lastHeard = time.Now()
}
