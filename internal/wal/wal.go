// Package wal implements a write-ahead log: an append-only file that records
// every change before it is applied to the in-memory store. If the process
// dies, the file is what lets us rebuild the map.
package wal

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// WAL is an open append-only log file.
type WAL struct {
	mu   sync.Mutex
	f    *os.File
	path string

	// size is the current length of the file in bytes, which is also the
	// offset the next record will be written at.
	size int64
}

// Open opens (or creates) the log file at path, ready for appending.
func Open(path string) (*WAL, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, fmt.Errorf("wal: create dir: %w", err)
	}

	// O_APPEND makes every write land at the end of the file, atomically,
	// so we never need to track or seek to an offset ourselves.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %s: %w", path, err)
	}

	// Creating a file is itself a change to the *directory*, and that change
	// is buffered too. Without this, a crash right after Open can leave a
	// filesystem where our file never existed at all.
	if err := syncDir(filepath.Dir(path)); err != nil {
		f.Close()
		return nil, err
	}

	// Where does the file currently end? That is where our next append lands.
	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("wal: stat %s: %w", path, err)
	}

	return &WAL{f: f, path: path, size: info.Size()}, nil
}

// Append writes one record to the end of the log and does not return until
// the operating system confirms it is physically on the disk. It returns the
// byte offset the record was written at, which is what lets a caller later
// truncate back to exactly this point.
func (w *WAL) Append(r Record) (int64, error) {
	frame, err := encode(r)
	if err != nil {
		return 0, fmt.Errorf("wal: encode: %w", err)
	}

	w.mu.Lock()
	defer w.mu.Unlock()

	offset := w.size

	// Write hands the bytes to the OS page cache -- fast, but still in RAM.
	if _, err := w.f.Write(frame); err != nil {
		return 0, fmt.Errorf("wal: write: %w", err)
	}

	// Sync forces those bytes out of the page cache onto the physical disk.
	// This is the line that makes the write survive a power cut.
	if err := w.f.Sync(); err != nil {
		return 0, fmt.Errorf("wal: sync: %w", err)
	}

	w.size += int64(len(frame))
	return offset, nil
}

// Truncate cuts the log back to size bytes and makes the cut durable. Raft
// calls this when a follower has to discard entries from a deposed leader.
//
// No seek is needed afterwards: the file was opened with O_APPEND, so the
// kernel sends every subsequent write to the (now shorter) end of file.
func (w *WAL) Truncate(size int64) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if size > w.size {
		return fmt.Errorf("wal: truncate to %d but file is only %d bytes", size, w.size)
	}

	if err := w.f.Truncate(size); err != nil {
		return fmt.Errorf("wal: truncate: %w", err)
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate: %w", err)
	}
	if err := syncDir(filepath.Dir(w.path)); err != nil {
		return err
	}

	w.size = size
	return nil
}

// Size is the current length of the log file in bytes.
func (w *WAL) Size() int64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.size
}

// Close flushes and closes the underlying file.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.f.Close()
}

// SyncDir fsyncs a directory, which is how you make a file *creation* or
// *rename* durable. Exported because raft needs it for its state file.
func SyncDir(dir string) error {
	return syncDir(dir)
}

// syncDir fsyncs a directory, which is how you make a file *creation* durable.
func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("wal: open dir %s: %w", dir, err)
	}
	defer d.Close()

	if err := d.Sync(); err != nil {
		return fmt.Errorf("wal: sync dir %s: %w", dir, err)
	}
	return nil
}
