// Package api exposes the engine over HTTP. It is deliberately thin: it
// translates HTTP into engine calls and engine results into status codes,
// and contains no storage logic of its own.
package api

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/himalisaini/raftkv/internal/engine"
	"github.com/himalisaini/raftkv/internal/raft"
	"github.com/himalisaini/raftkv/internal/wal"
)

// maxValueBytes caps how large a single value may be. Without this, one
// client could stream gigabytes into memory and take the node down.
const maxValueBytes = 1 << 20 // 1 MiB

// Server holds the dependencies our handlers need.
//
// Reads come from the local engine. Writes go through Raft, because a write
// is only real once a majority of the cluster has it.
type Server struct {
	engine *engine.Engine
	node   *raft.Node
}

// NewServer wires up the routes and returns something http.Server can run.
func NewServer(node *raft.Node, e *engine.Engine) http.Handler {
	s := &Server{engine: e, node: node}

	mux := http.NewServeMux()
	// Since Go 1.22 the pattern can include the method and a {wildcard}.
	// A request that matches the path but not the method gets 405 for free.
	mux.HandleFunc("GET /kv/{key}", s.handleGet)
	mux.HandleFunc("PUT /kv/{key}", s.handlePut)
	mux.HandleFunc("DELETE /kv/{key}", s.handleDelete)
	mux.HandleFunc("GET /health", s.handleHealth)
	mux.HandleFunc("GET /status", s.handleStatus)

	return mux
}

// handleGet returns the value, or 404 if the key is not present.
//
// The ?consistency= parameter selects the guarantee:
//
//	strong   (default) -- reflects every write acknowledged before this read
//	                      started. Costs a round trip to a majority.
//	eventual           -- whatever this node has applied so far. No network
//	                      cost, but may be milliseconds behind.
func (s *Server) handleGet(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	mode := r.URL.Query().Get("consistency")
	if mode == "" {
		// Defaulting to strong is deliberate: a store that silently serves
		// stale data unless you opt out is a trap. Staleness should be a
		// choice the caller makes on purpose.
		mode = "strong"
	}

	switch mode {
	case "eventual":
		// Read straight out of the local state machine. On a follower this
		// is the read that can be stale.

	case "strong":
		// Barrier first: confirm who the leader is, learn how far it has
		// committed, and wait for our own state machine to catch up.
		if err := s.node.LinearizableRead(r.Context()); err != nil {
			s.readBarrierFailed(w, err)
			return
		}

	default:
		http.Error(w, "consistency must be strong or eventual\n", http.StatusBadRequest)
		return
	}

	w.Header().Set("X-Raft-Node", string(s.node.ID()))
	w.Header().Set("X-Raft-Role", s.node.Role().String())
	w.Header().Set("X-Raft-Consistency", mode)

	value, ok := s.engine.Get(key)
	if !ok {
		// This is the `ok bool` from store.Get, surfaced as a status code.
		http.Error(w, "key not found\n", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	io.WriteString(w, value)
}

// readBarrierFailed turns a failed barrier into an honest status code. Note
// what we do NOT do: fall back to a local read. Quietly downgrading a strong
// read to a stale one is exactly the lie the mode exists to prevent.
func (s *Server) readBarrierFailed(w http.ResponseWriter, err error) {
	if leader := s.node.LeaderID(); leader != "" {
		w.Header().Set("X-Raft-Leader", string(leader))
	}

	switch {
	case errors.Is(err, raft.ErrNoLeader):
		http.Error(w, "no leader available for a strong read\n", http.StatusServiceUnavailable)
	case errors.Is(err, raft.ErrLeadershipLost), errors.Is(err, raft.ErrNotLeader):
		http.Error(w, "leadership could not be confirmed\n", http.StatusServiceUnavailable)
	default:
		log.Printf("read barrier: %v", err)
		http.Error(w, "read barrier failed\n", http.StatusServiceUnavailable)
	}
}

// handlePut stores the request body as the value for key.
func (s *Server) handlePut(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	// MaxBytesReader makes reads fail past the limit instead of buffering
	// forever, and it tells the client we hit the cap.
	r.Body = http.MaxBytesReader(w, r.Body, maxValueBytes)

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "value too large\n", http.StatusRequestEntityTooLarge)
		return
	}

	s.propose(w, r, wal.Record{Op: wal.OpSet, Key: key, Value: string(value)})
}

// handleDelete removes a key. Deleting a missing key is a success: the caller
// asked for "this key should not exist", and afterwards it does not.
func (s *Server) handleDelete(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")

	s.propose(w, r, wal.Record{Op: wal.OpDelete, Key: key})
}

// propose sends one command through Raft and turns the outcome into a status
// code. It only returns 204 once the entry is committed on a majority AND
// applied here, so a 204 means "durable on a majority of the cluster".
func (s *Server) propose(w http.ResponseWriter, r *http.Request, cmd wal.Record) {
	// Any node accepts writes. A follower forwards to the leader over the
	// peer port rather than bouncing the client with a redirect.
	err := s.node.ProposeOrForward(r.Context(), cmd)

	switch {
	case err == nil:
		w.WriteHeader(http.StatusNoContent)

	case errors.Is(err, raft.ErrNotLeader), errors.Is(err, raft.ErrNoLeader):
		// Nobody is leading right now -- an election is probably running.
		if leader := s.node.LeaderID(); leader != "" {
			w.Header().Set("X-Raft-Leader", string(leader))
		}
		http.Error(w, "no leader available\n", http.StatusServiceUnavailable)

	default:
		// Careful wording: we do not know this failed. The entry is in our
		// log and a future leader may still commit it.
		log.Printf("propose %s %q: %v", cmd.Op, cmd.Key, err)
		http.Error(w, "write not confirmed\n", http.StatusServiceUnavailable)
	}
}

// handleStatus exposes what this node believes about itself. Invaluable when
// you have three terminals open and need to know who is leader.
func (s *Server) handleStatus(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "id=%s role=%s term=%d commit=%d applied=%d log=%d keys=%d\n",
		s.node.ID(), s.node.Role(), s.node.Term(),
		s.node.CommitIndex(), s.node.LastApplied(),
		s.node.Log().LastIndex(), s.engine.Len())
}

// handleHealth is what we will point a load balancer -- and later our own
// failure detection -- at.
func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "ok\n")
}
