package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"hash/crc32"
)

// Op is the kind of change a Record describes.
type Op string

const (
	OpSet    Op = "set"
	OpDelete Op = "delete"

	// OpNoop changes nothing. A new leader appends one so that it has an
	// entry from its OWN term to commit; committing that entry commits
	// everything beneath it. See advanceCommit in the raft package.
	OpNoop Op = "noop"
)

// Record is one entry in the log: "someone did X to key K".
// A delete carries no Value, which is why Value is omitempty.
//
// Term and Index are the two fields Raft adds. On a single node they stay
// zero and nothing cares. In a cluster they are what lets two nodes compare
// their logs and find the exact point where they disagree.
type Record struct {
	Term  uint64 `json:"term,omitempty"`
	Index uint64 `json:"index,omitempty"`
	Op    Op     `json:"op"`
	Key   string `json:"key"`
	Value string `json:"value,omitempty"`
}

// Every record is written to disk inside a frame that looks like this:
//
//	┌──────────────┬──────────────┬─────────────────────┐
//	│ length (4B)  │ crc32 (4B)   │ payload (length B)  │
//	└──────────────┴──────────────┴─────────────────────┘
//
// length tells the reader where this record ends and the next begins.
// crc32 lets the reader detect a record that was only partially written
// (a crash mid-write) or silently corrupted on disk.
const headerSize = 8

// ErrCorrupt means the bytes on disk do not match their own checksum.
var ErrCorrupt = errors.New("wal: record failed checksum")

// encode turns a Record into the exact bytes we append to the file.
func encode(r Record) ([]byte, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	frame := make([]byte, headerSize+len(payload))
	binary.BigEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(frame[4:8], crc32.ChecksumIEEE(payload))
	copy(frame[headerSize:], payload)

	return frame, nil
}

// decodePayload verifies the checksum and turns the bytes back into a Record.
// The caller is responsible for having read a full frame off disk first.
func decodePayload(payload []byte, wantCRC uint32) (Record, error) {
	if crc32.ChecksumIEEE(payload) != wantCRC {
		return Record{}, ErrCorrupt
	}

	var r Record
	if err := json.Unmarshal(payload, &r); err != nil {
		return Record{}, err
	}
	return r, nil
}
