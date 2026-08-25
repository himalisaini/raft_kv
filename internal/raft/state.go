package raft

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/himalisaini/raftkv/internal/wal"
)

// PersistentState is the small amount of Raft state that MUST survive a
// crash. It is two fields, and forgetting either one breaks safety:
//
//   - CurrentTerm: forget it and a restarted node rejoins in an old term,
//     accepting writes from a leader that has already been replaced.
//   - VotedFor: forget it and a node that crashes after voting can vote
//     AGAIN for a different candidate in the same term -- which is how you
//     get two leaders in one term, and two leaders means lost writes.
//
// The Raft paper is explicit that both must be on stable storage before the
// node replies to any RPC. Not after. Before.
type PersistentState struct {
	CurrentTerm uint64 `json:"current_term"`
	VotedFor    NodeID `json:"voted_for"`
}

// StateStore persists PersistentState to a single small file.
type StateStore struct {
	path string
}

// OpenStateStore loads the state file from dir, creating it if absent.
// A missing file is a brand new node: term 0, no vote cast.
func OpenStateStore(dir string) (*StateStore, PersistentState, error) {
	s := &StateStore{path: filepath.Join(dir, "state.json")}

	data, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return s, PersistentState{}, nil
	}
	if err != nil {
		return nil, PersistentState{}, fmt.Errorf("raft: read state: %w", err)
	}

	var st PersistentState
	if err := json.Unmarshal(data, &st); err != nil {
		// Refusing to start beats guessing. A node that guesses its term
		// wrong can violate the one-leader-per-term guarantee.
		return nil, PersistentState{}, fmt.Errorf("raft: parse state file %s: %w", s.path, err)
	}
	return s, st, nil
}

// Save writes the state durably, using the write-temp-then-rename dance.
//
// Writing in place would be a disaster: a crash halfway through leaves a
// half-written file that parses as garbage, and the node cannot start. On
// every POSIX filesystem rename is atomic -- after a crash you have either
// the complete old file or the complete new one, never a mixture.
func (s *StateStore) Save(st PersistentState) error {
	data, err := json.Marshal(st)
	if err != nil {
		return fmt.Errorf("raft: encode state: %w", err)
	}

	tmp := s.path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("raft: create temp state: %w", err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("raft: write temp state: %w", err)
	}
	// Sync the contents before the rename, or the rename can land first and
	// point at a file whose bytes never reached the disk.
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("raft: sync temp state: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("raft: close temp state: %w", err)
	}

	if err := os.Rename(tmp, s.path); err != nil {
		return fmt.Errorf("raft: rename state: %w", err)
	}

	// The rename itself is a directory change, and needs its own fsync.
	return wal.SyncDir(filepath.Dir(s.path))
}
