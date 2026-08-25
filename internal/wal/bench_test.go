package wal

import (
	"os"
	"path/filepath"
	"testing"
)

var rec = Record{Op: OpSet, Key: "user:12345", Value: "session-token-abcdef"}

// The real thing: every append is fsynced before returning.
func BenchmarkAppend_Fsync(b *testing.B) {
	w, _ := Open(filepath.Join(b.TempDir(), "wal.log"))
	defer w.Close()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := w.Append(rec); err != nil {
			b.Fatal(err)
		}
	}
}

// Write only -- bytes reach the OS page cache, not the disk platter.
// Survives a process crash. Does NOT survive a power cut.
func BenchmarkAppend_NoFsync(b *testing.B) {
	f, _ := os.OpenFile(filepath.Join(b.TempDir(), "wal.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	defer f.Close()
	frame, _ := encode(rec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Write(frame)
	}
}

// Group commit: batch 100 writes, then one fsync for all of them.
func BenchmarkAppend_BatchedFsync100(b *testing.B) {
	f, _ := os.OpenFile(filepath.Join(b.TempDir(), "wal.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	defer f.Close()
	frame, _ := encode(rec)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.Write(frame)
		if i%100 == 99 {
			f.Sync()
		}
	}
}
