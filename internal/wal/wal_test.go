package wal

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

// TestEncodeDecodeRoundTrip proves the bytes we write can be read back.
func TestEncodeDecodeRoundTrip(t *testing.T) {
	original := Record{Op: OpSet, Key: "city", Value: "Delhi"}

	frame, err := encode(original)
	if err != nil {
		t.Fatalf("encode: %v", err)
	}

	length := binary.BigEndian.Uint32(frame[0:4])
	crc := binary.BigEndian.Uint32(frame[4:8])
	payload := frame[headerSize:]

	if int(length) != len(payload) {
		t.Fatalf("header says %d bytes, payload is %d", length, len(payload))
	}

	got, err := decodePayload(payload, crc)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got != original {
		t.Fatalf("got %+v, want %+v", got, original)
	}
}

// TestCorruptionIsDetected flips one bit and checks the CRC catches it.
func TestCorruptionIsDetected(t *testing.T) {
	frame, _ := encode(Record{Op: OpSet, Key: "city", Value: "Delhi"})
	crc := binary.BigEndian.Uint32(frame[4:8])
	payload := frame[headerSize:]

	payload[3] ^= 0x01 // corrupt a single bit, as a bad disk sector would

	if _, err := decodePayload(payload, crc); err != ErrCorrupt {
		t.Fatalf("got %v, want ErrCorrupt", err)
	}
}

// TestAppendWritesToDisk checks the file actually grows by the frame size.
func TestAppendWritesToDisk(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")

	w, err := Open(path)
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	records := []Record{
		{Op: OpSet, Key: "city", Value: "Delhi"},
		{Op: OpSet, Key: "city", Value: "Mumbai"},
		{Op: OpDelete, Key: "city"},
	}
	for _, r := range records {
		if _, err := w.Append(r); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	w.Close()

	// Work out how big the file should be, frame by frame.
	wantSize := 0
	for _, r := range records {
		frame, _ := encode(r)
		wantSize += len(frame)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Size() != int64(wantSize) {
		t.Fatalf("file is %d bytes, want %d", info.Size(), wantSize)
	}
}

// TestConcurrentAppends checks that 50 goroutines appending at once produce
// 50 intact frames -- not interleaved garbage.
func TestConcurrentAppends(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal.log")
	w, _ := Open(path)
	defer w.Close()

	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			if _, err := w.Append(Record{Op: OpSet, Key: "k", Value: "v"}); err != nil {
				t.Errorf("append: %v", err)
			}
		}(i)
	}
	wg.Wait()

	oneFrame, _ := encode(Record{Op: OpSet, Key: "k", Value: "v"})
	info, _ := os.Stat(path)
	if want := int64(len(oneFrame) * 50); info.Size() != want {
		t.Fatalf("file is %d bytes, want %d", info.Size(), want)
	}
}
