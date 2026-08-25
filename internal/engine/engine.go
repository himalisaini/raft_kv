// Package engine is the state machine: the thing a committed log entry gets
// applied to.
//
// It holds no durable state of its own. The Raft log is the write-ahead log,
// so durability lives in raft.Log and this package is only "given a committed
// command, update the state".
package engine

import (
	"github.com/himalisaini/raftkv/internal/store"
	"github.com/himalisaini/raftkv/internal/wal"
)

// Engine holds the current state of the key-value store.
type Engine struct {
	state *store.Store
}

// New returns an empty state machine.
func New() *Engine {
	return &Engine{state: store.New()}
}

// Get reads the current value. Reads never touch the disk.
func (e *Engine) Get(key string) (string, bool) {
	return e.state.Get(key)
}

// Len is how many keys are currently stored.
func (e *Engine) Len() int {
	return e.state.Len()
}

// Apply performs one committed log entry against the state.
//
// It must stay deterministic: the same log applied in the same order has to
// produce the same state on every node. Every guarantee in Raft rests on it.
func (e *Engine) Apply(r wal.Record) {
	switch r.Op {
	case wal.OpSet:
		e.state.Set(r.Key, r.Value)
	case wal.OpDelete:
		e.state.Delete(r.Key)
	}
}
