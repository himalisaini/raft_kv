package raft

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/himalisaini/raftkv/internal/wal"
)

func TestLogSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	l1, err := OpenLog(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustAppend(t, l1, 1, cmd("a", "1"), cmd("b", "2"))
	mustAppend(t, l1, 4, cmd("c", "3"))
	before := render(l1)
	l1.Close()

	l2, err := OpenLog(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer l2.Close()

	if after := render(l2); after != before {
		t.Fatalf("after restart log = %s, want %s", after, before)
	}
	if l2.LastIndex() != 3 || l2.LastTerm() != 4 {
		t.Fatalf("last = %d/%d, want 3/4", l2.LastIndex(), l2.LastTerm())
	}

	// And appending after recovery must continue the index sequence.
	got := mustAppend(t, l2, 4, cmd("d", "4"))
	if got[0].Index != 4 {
		t.Fatalf("next index = %d, want 4", got[0].Index)
	}
}

// TestTruncationShrinksTheFile is the whole reason we track byte offsets.
// Discarding entries must actually reclaim the bytes, not just hide them.
func TestTruncationShrinksTheFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	l, err := OpenLog(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	mustAppend(t, l, 1, cmd("a", "1"))
	mustAppend(t, l, 2, cmd("x", "stale"), cmd("y", "stale"))

	full, _ := os.Stat(path)

	// A new leader in term 3 overwrites everything after index 1.
	ok := mustAppendAfter(t, l, 1, 1, []wal.Record{
		{Term: 3, Index: 2, Op: wal.OpSet, Key: "b"},
	})
	if !ok {
		t.Fatal("append should have been accepted")
	}
	l.Close()

	shrunk, _ := os.Stat(path)
	if shrunk.Size() >= full.Size() {
		t.Fatalf("file is %d bytes, was %d -- truncation did not reclaim space",
			shrunk.Size(), full.Size())
	}

	// The decisive check: reopen from disk and confirm the stale entries are
	// really gone, not merely dropped from memory.
	l2, err := OpenLog(path)
	if err != nil {
		t.Fatalf("reopen after truncation: %v", err)
	}
	defer l2.Close()

	if got := render(l2); got != "[1:a][3:b]" {
		t.Fatalf("recovered log = %s, want [1:a][3:b]", got)
	}
}

// TestTornTailIsRepairedOnOpen: a crash mid-append must not stop the node
// from starting.
func TestTornTailIsRepairedOnOpen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	l, _ := OpenLog(path)
	mustAppend(t, l, 1, cmd("a", "1"), cmd("b", "2"), cmd("c", "3"))
	l.Close()

	full, _ := os.Stat(path)
	if err := os.Truncate(path, full.Size()-6); err != nil { // simulate a crash
		t.Fatalf("simulate crash: %v", err)
	}

	l2, err := OpenLog(path)
	if err != nil {
		t.Fatalf("open after crash: %v", err)
	}
	defer l2.Close()

	if got := render(l2); got != "[1:a][1:b]" {
		t.Fatalf("recovered log = %s, want [1:a][1:b]", got)
	}

	// Index 3 is free again, and the next append must claim it.
	got := mustAppend(t, l2, 1, cmd("c", "3"))
	if got[0].Index != 3 {
		t.Fatalf("next index = %d, want 3", got[0].Index)
	}
}

// TestRejectsLogWithWrongIndices: refuse to guess about a file we did not
// write, rather than silently mis-indexing everything.
func TestRejectsLogWithWrongIndices(t *testing.T) {
	path := filepath.Join(t.TempDir(), "raft.log")

	w, _ := wal.Open(path)
	w.Append(wal.Record{Term: 1, Index: 7, Op: wal.OpSet, Key: "a"}) // index 7 at position 1
	w.Close()

	if _, err := OpenLog(path); err == nil {
		t.Fatal("expected an error for a log whose indices do not match position")
	}
}
