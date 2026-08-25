package engine

import (
	"testing"

	"github.com/himalisaini/raftkv/internal/wal"
)

// TestApplyIsDeterministic: the same log in the same order must always give
// the same state. Two independent engines, one log, identical results.
func TestApplyIsDeterministic(t *testing.T) {
	log := []wal.Record{
		{Op: wal.OpSet, Key: "city", Value: "Delhi"},
		{Op: wal.OpSet, Key: "lang", Value: "Go"},
		{Op: wal.OpSet, Key: "city", Value: "Mumbai"},
		{Op: wal.OpDelete, Key: "lang"},
	}

	a, b := New(), New()
	for _, r := range log {
		a.Apply(r)
		b.Apply(r)
	}

	for _, key := range []string{"city", "lang"} {
		av, aok := a.Get(key)
		bv, bok := b.Get(key)
		if av != bv || aok != bok {
			t.Fatalf("engines disagree on %q: (%q,%v) vs (%q,%v)", key, av, aok, bv, bok)
		}
	}

	if v, ok := a.Get("city"); !ok || v != "Mumbai" {
		t.Fatalf("city = %q %v, want Mumbai true", v, ok)
	}
	if _, ok := a.Get("lang"); ok {
		t.Fatal("lang should have been deleted")
	}
	if a.Len() != 1 {
		t.Fatalf("len = %d, want 1", a.Len())
	}
}

// TestApplyIgnoresUnknownOps: a record we do not understand must not panic.
func TestApplyIgnoresUnknownOps(t *testing.T) {
	e := New()
	e.Apply(wal.Record{Op: "franchise", Key: "x", Value: "y"})
	if e.Len() != 0 {
		t.Fatal("unknown op should have changed nothing")
	}
}
