// Package raft implements the replication half of Raft: an indexed log that
// two nodes can compare, disagree about, and repair.
package raft

import (
	"fmt"
	"sync"

	"github.com/himalisaini/raftkv/internal/wal"
)

// Log is one node's replicated log.
//
// Indices are 1-based. entries[0] is a sentinel with Term 0 and Index 0 that
// represents "before the beginning". It is never replicated and never
// applied; it exists so that the very first real entry has something to
// point back at, which removes every special case from the matching rules.
type Log struct {
	mu      sync.RWMutex
	entries []wal.Record

	// offsets[i] is the byte offset in the file where entries[i] starts.
	// This is how we turn "truncate from log index 3" into a byte offset
	// the file layer can act on. offsets[0] belongs to the sentinel and is
	// never used.
	offsets []int64

	// w is the file this log is persisted to. It is nil for an in-memory
	// log, which is what the unit tests use.
	w *wal.WAL
}

// NewLog returns an empty in-memory log holding only the sentinel. Nothing
// it stores survives a restart; use OpenLog for a real node.
func NewLog() *Log {
	return &Log{
		entries: []wal.Record{{Term: 0, Index: 0}},
		offsets: []int64{0},
	}
}

// OpenLog recovers a log from disk, repairing a torn tail if the node
// crashed mid-append.
func OpenLog(path string) (*Log, error) {
	res, err := wal.ReadAll(path)
	if err != nil {
		return nil, fmt.Errorf("raft: read log: %w", err)
	}
	if res.Truncated {
		if err := wal.TruncateTo(path, res.ValidBytes); err != nil {
			return nil, fmt.Errorf("raft: repair log: %w", err)
		}
	}

	w, err := wal.Open(path)
	if err != nil {
		return nil, fmt.Errorf("raft: open log: %w", err)
	}

	l := &Log{
		entries: []wal.Record{{Term: 0, Index: 0}},
		offsets: []int64{0},
		w:       w,
	}

	for i, r := range res.Records {
		// The index stored inside the record must equal its position. If it
		// does not, the file is not a log we wrote, and guessing would be
		// worse than refusing.
		if want := uint64(i + 1); r.Index != want {
			w.Close()
			return nil, fmt.Errorf("raft: log entry %d claims index %d", want, r.Index)
		}
		l.entries = append(l.entries, r)
		l.offsets = append(l.offsets, res.Offsets[i])
	}

	return l, nil
}

// Close releases the underlying file.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.w == nil {
		return nil
	}
	return l.w.Close()
}

// LastIndex is the index of the newest entry, or 0 for an empty log.
func (l *Log) LastIndex() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.lastIndexLocked()
}

// LastTerm is the term of the newest entry, or 0 for an empty log.
func (l *Log) LastTerm() uint64 {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.entries[len(l.entries)-1].Term
}

func (l *Log) lastIndexLocked() uint64 {
	return l.entries[len(l.entries)-1].Index
}

// At returns the entry stored at index, if we have it.
func (l *Log) At(index uint64) (wal.Record, bool) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 || index >= uint64(len(l.entries)) {
		return wal.Record{}, false
	}
	return l.entries[index], true
}

// Append is the LEADER side: stamp each command with the current term and the
// next free index, put it on disk, then in memory. Returns the entries as
// stored.
func (l *Log) Append(term uint64, cmds ...wal.Record) ([]wal.Record, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	appended := make([]wal.Record, 0, len(cmds))
	for _, c := range cmds {
		c.Term = term
		c.Index = l.lastIndexLocked() + 1

		if err := l.storeLocked(c); err != nil {
			// Entries before this one are already durable and in memory, so
			// disk and memory still agree. We just stopped early.
			return appended, err
		}
		appended = append(appended, c)
	}
	return appended, nil
}

// storeLocked writes one entry to disk (if we have a file) and then to
// memory. Disk first, always: if the fsync fails, memory must not claim to
// hold an entry the disk does not have.
func (l *Log) storeLocked(e wal.Record) error {
	offset := int64(0)
	if l.w != nil {
		var err error
		offset, err = l.w.Append(e)
		if err != nil {
			return fmt.Errorf("raft: persist entry %d: %w", e.Index, err)
		}
	}

	l.entries = append(l.entries, e)
	l.offsets = append(l.offsets, offset)
	return nil
}

// truncateLocked drops every entry from index onwards, on disk and in memory.
func (l *Log) truncateLocked(index uint64) error {
	if l.w != nil {
		if err := l.w.Truncate(l.offsets[index]); err != nil {
			return fmt.Errorf("raft: truncate at index %d: %w", index, err)
		}
	}

	l.entries = l.entries[:index]
	l.offsets = l.offsets[:index]
	return nil
}

// EntriesFrom returns every entry from index onwards. This is what a leader
// ships to a follower that it believes is behind.
func (l *Log) EntriesFrom(index uint64) []wal.Record {
	l.mu.RLock()
	defer l.mu.RUnlock()

	if index == 0 {
		index = 1 // never send the sentinel
	}
	if index >= uint64(len(l.entries)) {
		return nil
	}

	out := make([]wal.Record, len(l.entries)-int(index))
	copy(out, l.entries[index:])
	return out
}

// Match reports whether this log has an entry at prevIndex whose term is
// exactly prevTerm.
//
// This is the Log Matching check. Raft guarantees that if two logs agree on
// the index AND term of one entry, then every entry before it is identical.
// So this single comparison verifies an entire prefix without sending it.
func (l *Log) Match(prevIndex, prevTerm uint64) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.matchLocked(prevIndex, prevTerm)
}

func (l *Log) matchLocked(prevIndex, prevTerm uint64) bool {
	if prevIndex >= uint64(len(l.entries)) {
		return false // we are missing entries: our log is too short
	}
	return l.entries[prevIndex].Term == prevTerm
}

// AppendAfter is the FOLLOWER side of replication. The leader says: "the
// entry before these had index prevIndex and term prevTerm -- if you agree,
// here is what comes next."
//
// It returns false if we do not agree, which tells the leader to back up and
// try again from an earlier point.
func (l *Log) AppendAfter(prevIndex, prevTerm uint64, entries []wal.Record) (bool, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	// 1. Refuse anything we cannot anchor to a matching entry.
	if !l.matchLocked(prevIndex, prevTerm) {
		return false, nil
	}

	// 2. Walk the incoming entries against what we already have.
	for i, e := range entries {
		idx := prevIndex + 1 + uint64(i)

		if idx < uint64(len(l.entries)) {
			if l.entries[idx].Term == e.Term {
				// Same index, same term => same entry. This is a duplicate,
				// usually a retried message. Skip it. Crucially we must NOT
				// truncate here: a delayed duplicate would otherwise chop off
				// newer entries we have already committed.
				continue
			}
			// Same index, different term => a conflict. Everything from here
			// on came from a leader that has been deposed. Drop it.
			if err := l.truncateLocked(idx); err != nil {
				return false, err
			}
		}

		if err := l.storeLocked(e); err != nil {
			return false, err
		}
	}

	return true, nil
}

// Len is the number of real entries, excluding the sentinel.
func (l *Log) Len() int {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return len(l.entries) - 1
}
