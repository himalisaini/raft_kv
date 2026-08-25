package raft

import "github.com/himalisaini/raftkv/internal/wal"

// AppendEntriesRequest is the message a leader sends to a follower. It is
// both the replication mechanism and, when Entries is empty, the heartbeat.
type AppendEntriesRequest struct {
	// Term is the leader's term. A follower uses it to decide whether this
	// leader is current or stale.
	Term     uint64 `json:"term"`
	LeaderID NodeID `json:"leader_id"`

	// PrevLogIndex and PrevLogTerm identify the entry immediately before
	// Entries. The follower must already have exactly that entry, or it
	// rejects the whole message. This is the Log Matching check from 2.1.
	PrevLogIndex uint64 `json:"prev_log_index"`
	PrevLogTerm  uint64 `json:"prev_log_term"`

	Entries []wal.Record `json:"entries,omitempty"`

	// LeaderCommit tells the follower how far the leader has committed, so
	// the follower can apply those entries to its own state machine.
	LeaderCommit uint64 `json:"leader_commit"`
}

// AppendEntriesResponse is the follower's reply.
type AppendEntriesResponse struct {
	// Term is the follower's term. If it is higher than the leader's, the
	// leader has been deposed and must step down.
	Term uint64 `json:"term"`

	// Success is false when the follower could not match PrevLogIndex/Term.
	Success bool `json:"success"`

	// LastLogIndex is a hint: how far the follower's log actually goes. It
	// lets a leader skip straight past the "your log is too short" case
	// instead of decrementing one index per round trip.
	LastLogIndex uint64 `json:"last_log_index"`
}

// RequestVoteRequest is what a candidate sends when it wants to be leader.
type RequestVoteRequest struct {
	Term        uint64 `json:"term"`
	CandidateID NodeID `json:"candidate_id"`

	// LastLogIndex and LastLogTerm describe how complete the candidate's log
	// is. A voter refuses anyone whose log is less complete than its own.
	// This is the Election Restriction, and it is what guarantees a
	// committed entry can never be lost in a leader change.
	LastLogIndex uint64 `json:"last_log_index"`
	LastLogTerm  uint64 `json:"last_log_term"`
}

// RequestVoteResponse is the voter's reply.
type RequestVoteResponse struct {
	Term        uint64 `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
}

// ReadIndexRequest asks the leader two things at once: are you still really
// the leader, and how far have you committed?
type ReadIndexRequest struct {
	Term       uint64 `json:"term"`
	FromNodeID NodeID `json:"from_node_id"`
}

// ReadIndexResponse is the leader's answer.
type ReadIndexResponse struct {
	Term uint64 `json:"term"`

	// IsLeader is false if we are not the leader, or could not prove it.
	IsLeader bool `json:"is_leader"`

	// ReadIndex is the commit index at the moment leadership was confirmed.
	// A node that has applied up to here can serve a linearizable read.
	ReadIndex uint64 `json:"read_index"`
}

// ForwardRequest carries a client write from a follower to the leader.
type ForwardRequest struct {
	Command wal.Record `json:"command"`
}

// ForwardResponse reports what the leader did with it.
type ForwardResponse struct {
	Success  bool   `json:"success"`
	LeaderID NodeID `json:"leader_id"`
	Error    string `json:"error,omitempty"`
}
