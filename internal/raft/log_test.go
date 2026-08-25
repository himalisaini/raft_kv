package raft

import (
	"fmt"
	"strings"
	"testing"

	"github.com/himalisaini/raftkv/internal/wal"
)

func cmd(key, value string) wal.Record {
	return wal.Record{Op: wal.OpSet, Key: key, Value: value}
}

// mustAppend fails the test if persisting an entry errors, so the tests can
// stay focused on log semantics rather than on I/O plumbing.
func mustAppend(t *testing.T, l *Log, term uint64, cmds ...wal.Record) []wal.Record {
	t.Helper()
	got, err := l.Append(term, cmds...)
	if err != nil {
		t.Fatalf("append: %v", err)
	}
	return got
}

func mustAppendAfter(t *testing.T, l *Log, prevIndex, prevTerm uint64, entries []wal.Record) bool {
	t.Helper()
	ok, err := l.AppendAfter(prevIndex, prevTerm, entries)
	if err != nil {
		t.Fatalf("append after: %v", err)
	}
	return ok
}

// render prints a log as "term:key" per index, so tests can show divergence.
func render(l *Log) string {
	var b strings.Builder
	for i := uint64(1); i <= l.LastIndex(); i++ {
		e, _ := l.At(i)
		fmt.Fprintf(&b, "[%d:%s]", e.Term, e.Key)
	}
	if b.Len() == 0 {
		return "(empty)"
	}
	return b.String()
}

func TestAppendAssignsTermAndIndex(t *testing.T) {
	l := NewLog()

	if l.LastIndex() != 0 || l.Len() != 0 {
		t.Fatalf("fresh log should be empty, got index %d len %d", l.LastIndex(), l.Len())
	}

	got := mustAppend(t, l, 1, cmd("a", "1"), cmd("b", "2"))
	if len(got) != 2 {
		t.Fatalf("appended %d entries, want 2", len(got))
	}
	if got[0].Index != 1 || got[0].Term != 1 {
		t.Fatalf("first entry = index %d term %d, want 1/1", got[0].Index, got[0].Term)
	}
	if got[1].Index != 2 {
		t.Fatalf("second entry index = %d, want 2", got[1].Index)
	}

	// A later term keeps the index sequence going.
	mustAppend(t, l, 3, cmd("c", "3"))
	if l.LastIndex() != 3 || l.LastTerm() != 3 {
		t.Fatalf("last = index %d term %d, want 3/3", l.LastIndex(), l.LastTerm())
	}
}

// TestMatchAgainstSentinel: an empty log must accept entries starting at 1.
// That is exactly what the sentinel is for.
func TestMatchAgainstSentinel(t *testing.T) {
	l := NewLog()

	if !l.Match(0, 0) {
		t.Fatal("empty log should match (prevIndex=0, prevTerm=0)")
	}
	if l.Match(1, 1) {
		t.Fatal("empty log should not match an entry it does not have")
	}
}

func TestAppendAfterRejectsGapsAndWrongTerms(t *testing.T) {
	l := NewLog()
	mustAppend(t, l, 1, cmd("a", "1"), cmd("b", "2")) // [1:a][1:b]

	// The leader claims the previous entry is index 5. We only have 2.
	if mustAppendAfter(t, l, 5, 1, []wal.Record{{Term: 1, Index: 6}}) {
		t.Fatal("should reject: our log is too short")
	}

	// Right index, wrong term.
	if mustAppendAfter(t, l, 2, 9, []wal.Record{{Term: 9, Index: 3}}) {
		t.Fatal("should reject: term at index 2 does not match")
	}

	// Correct anchor: accepted.
	if !mustAppendAfter(t, l, 2, 1, []wal.Record{{Term: 1, Index: 3, Op: wal.OpSet, Key: "c"}}) {
		t.Fatal("should accept: index 2 term 1 matches")
	}
	if l.LastIndex() != 3 {
		t.Fatalf("last index = %d, want 3", l.LastIndex())
	}
}

// TestConflictingTailIsTruncated is the repair mechanism: a follower that
// followed a deposed leader must drop those entries.
func TestConflictingTailIsTruncated(t *testing.T) {
	follower := NewLog()
	mustAppend(t, follower, 1, cmd("a", "1"))
	mustAppend(t, follower, 2, cmd("x", "bad"), cmd("y", "bad")) // from a leader that died

	t.Logf("follower before : %s", render(follower))

	// The real leader's log is [1:a][3:b][3:c]. It anchors at index 1.
	ok := mustAppendAfter(t, follower, 1, 1, []wal.Record{
		{Term: 3, Index: 2, Op: wal.OpSet, Key: "b"},
		{Term: 3, Index: 3, Op: wal.OpSet, Key: "c"},
	})
	if !ok {
		t.Fatal("should accept: index 1 term 1 matches")
	}

	t.Logf("follower after  : %s", render(follower))

	if got := render(follower); got != "[1:a][3:b][3:c]" {
		t.Fatalf("follower = %s, want [1:a][3:b][3:c]", got)
	}
}

// TestDuplicateDeliveryDoesNotTruncate is the subtle one. A retried message
// that arrives late must not chop off newer entries.
func TestDuplicateDeliveryDoesNotTruncate(t *testing.T) {
	l := NewLog()
	mustAppend(t, l, 1, cmd("a", "1"), cmd("b", "2"), cmd("c", "3")) // [1:a][1:b][1:c]

	// An old message arrives again: "after index 1, here is entry 2".
	// We already have entry 2 AND entry 3.
	ok := mustAppendAfter(t, l, 1, 1, []wal.Record{
		{Term: 1, Index: 2, Op: wal.OpSet, Key: "b"},
	})
	if !ok {
		t.Fatal("duplicate should be accepted, not rejected")
	}

	if got := render(l); got != "[1:a][1:b][1:c]" {
		t.Fatalf("log = %s, want [1:a][1:b][1:c] -- entry 3 was wrongly dropped", got)
	}
}

func TestEntriesFrom(t *testing.T) {
	l := NewLog()
	mustAppend(t, l, 1, cmd("a", "1"), cmd("b", "2"), cmd("c", "3"))

	if got := l.EntriesFrom(2); len(got) != 2 || got[0].Key != "b" {
		t.Fatalf("EntriesFrom(2) = %v, want entries b and c", got)
	}
	if got := l.EntriesFrom(4); got != nil {
		t.Fatalf("EntriesFrom(4) = %v, want nil", got)
	}
	// Index 0 is the sentinel and must never be sent.
	if got := l.EntriesFrom(0); len(got) != 3 {
		t.Fatalf("EntriesFrom(0) = %d entries, want 3 (sentinel excluded)", len(got))
	}
}
