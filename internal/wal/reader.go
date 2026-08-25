package wal

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// maxRecordSize caps how big a single record may claim to be. A corrupted
// length header could otherwise say "4 billion bytes" and we would try to
// allocate it. Never trust a number you read off a disk.
const maxRecordSize = 8 << 20 // 8 MiB

// ReadResult is what we recovered from a log file.
type ReadResult struct {
	// Records are every intact record, in the order they were written.
	Records []Record

	// Offsets[i] is the byte offset where Records[i] begins. Raft needs this
	// to turn "truncate from log index N" into "truncate to byte offset X".
	Offsets []int64

	// ValidBytes is the offset just past the last intact record. Anything
	// after this is a torn or corrupt tail and must be thrown away.
	ValidBytes int64

	// Truncated is true if we stopped early instead of reaching a clean EOF.
	Truncated bool
}

// ReadAll reads a log file from the start, stopping at the first record that
// is incomplete or fails its checksum. A missing file is not an error -- that
// is simply a node booting for the very first time.
func ReadAll(path string) (ReadResult, error) {
	var res ReadResult

	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return res, nil
	}
	if err != nil {
		return res, fmt.Errorf("wal: open %s: %w", path, err)
	}
	defer f.Close()

	r := bufio.NewReader(f)
	header := make([]byte, headerSize)

	for {
		// Step 1: read the 8-byte header.
		if _, err := io.ReadFull(r, header); err != nil {
			if errors.Is(err, io.EOF) {
				return res, nil // clean end of file: every record was whole
			}
			if errors.Is(err, io.ErrUnexpectedEOF) {
				res.Truncated = true // died partway through a header
				return res, nil
			}
			return res, fmt.Errorf("wal: read header: %w", err)
		}

		length := binary.BigEndian.Uint32(header[0:4])
		crc := binary.BigEndian.Uint32(header[4:8])

		if length > maxRecordSize {
			res.Truncated = true // the header itself is nonsense
			return res, nil
		}

		// Step 2: read exactly `length` bytes of payload.
		payload := make([]byte, length)
		if _, err := io.ReadFull(r, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				res.Truncated = true // died partway through a payload
				return res, nil
			}
			return res, fmt.Errorf("wal: read payload: %w", err)
		}

		// Step 3: verify it, then keep it.
		rec, err := decodePayload(payload, crc)
		if err != nil {
			res.Truncated = true // checksum failed: the bytes are damaged
			return res, nil
		}

		res.Records = append(res.Records, rec)
		res.Offsets = append(res.Offsets, res.ValidBytes)
		res.ValidBytes += int64(headerSize) + int64(length)
	}
}

// TruncateTo cuts the file back to size, discarding a torn tail, and makes
// that repair durable before we start appending again.
func TruncateTo(path string, size int64) error {
	f, err := os.OpenFile(path, os.O_WRONLY, 0o644)
	if err != nil {
		return fmt.Errorf("wal: open for truncate: %w", err)
	}
	defer f.Close()

	if err := f.Truncate(size); err != nil {
		return fmt.Errorf("wal: truncate %s: %w", path, err)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("wal: sync after truncate: %w", err)
	}
	return syncDir(filepath.Dir(path))
}
