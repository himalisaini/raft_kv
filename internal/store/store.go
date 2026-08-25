// Package store holds the in-memory key-value data for a single node.
// It knows nothing about HTTP, disks, or Raft -- it is just a safe map.
package store

import "sync"

// Store is a key-value map that is safe to use from many goroutines at once.
type Store struct {
	mu   sync.RWMutex
	data map[string]string
}

// New returns an empty Store that is ready to use.
func New() *Store {
	return &Store{
		data: make(map[string]string),
	}
}

// Get returns the value for key. The second return value is false if the key
// was never set (or was deleted).
func (s *Store) Get(key string) (string, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.data[key]
	return value, ok
}

// Set stores value under key, overwriting any previous value.
func (s *Store) Set(key, value string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.data[key] = value
}

// Delete removes key. Deleting a key that does not exist is not an error.
func (s *Store) Delete(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.data, key)
}

// Len returns how many keys are currently stored. Useful in tests.
func (s *Store) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.data)
}
