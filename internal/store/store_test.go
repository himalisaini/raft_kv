package store

import (
	"fmt"
	"sync"
	"testing"
)

// TestBasicOperations walks through the everyday path: set, get, overwrite, delete.
func TestBasicOperations(t *testing.T) {
	s := New()

	// A key we never set must report ok == false.
	if _, ok := s.Get("city"); ok {
		t.Fatal("expected 'city' to be missing from a fresh store")
	}

	s.Set("city", "Delhi")
	value, ok := s.Get("city")
	if !ok || value != "Delhi" {
		t.Fatalf("got (%q, %v), want (\"Delhi\", true)", value, ok)
	}

	// Setting the same key again overwrites, it does not append.
	s.Set("city", "Mumbai")
	value, _ = s.Get("city")
	if value != "Mumbai" {
		t.Fatalf("got %q after overwrite, want \"Mumbai\"", value)
	}

	s.Delete("city")
	if _, ok := s.Get("city"); ok {
		t.Fatal("expected 'city' to be gone after Delete")
	}

	// Deleting something that is not there must not panic.
	s.Delete("does-not-exist")
}

// TestConcurrentWrites hammers the store from 100 goroutines at once.
// Run it with `go test -race` -- without the mutex in Set this test panics
// with "concurrent map writes" or is flagged by the race detector.
func TestConcurrentWrites(t *testing.T) {
	s := New()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			s.Set(fmt.Sprintf("key-%d", n), "value")
		}(i)
	}
	wg.Wait()

	if got := s.Len(); got != 100 {
		t.Fatalf("got %d keys, want 100", got)
	}
}
