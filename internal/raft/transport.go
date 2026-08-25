package raft

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

// Transport is how a node reaches its peers. It is an interface so that tests
// can replace the network with something that drops, delays, or reorders
// messages -- which is the only honest way to test a consensus protocol.
type Transport interface {
	AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error)
	RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (RequestVoteResponse, error)
	ReadIndex(ctx context.Context, peer Peer, req ReadIndexRequest) (ReadIndexResponse, error)
	Forward(ctx context.Context, peer Peer, req ForwardRequest) (ForwardResponse, error)
}

// The two endpoints peers post to.
const (
	appendEntriesPath = "/raft/append-entries"
	requestVotePath   = "/raft/request-vote"
	readIndexPath     = "/raft/read-index"
	forwardPath       = "/raft/forward"
)

// HTTPTransport sends RPCs as JSON over HTTP.
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport returns a transport with sane timeouts.
//
// The timeout matters more than it looks: a leader that blocks forever on one
// unreachable follower stops heartbeating everyone else, and the cluster
// elects a new leader out from under it.
func NewHTTPTransport(timeout time.Duration) *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				// Peer traffic is a small number of hosts with a lot of
				// requests. The default idle pool (2 per host) makes Go
				// tear down and re-dial constantly, which under load shows
				// up as timeouts that look like node failures.
				MaxIdleConns:        128,
				MaxIdleConnsPerHost: 64,
				IdleConnTimeout:     90 * time.Second,
			},
		},
	}
}

// AppendEntries posts one replication request to one peer.
func (t *HTTPTransport) AppendEntries(ctx context.Context, peer Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	var resp AppendEntriesResponse
	err := t.post(ctx, peer, appendEntriesPath, req, &resp)
	return resp, err
}

// RequestVote asks one peer for its vote.
func (t *HTTPTransport) RequestVote(ctx context.Context, peer Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
	var resp RequestVoteResponse
	err := t.post(ctx, peer, requestVotePath, req, &resp)
	return resp, err
}

// ReadIndex asks a peer (the leader) for a linearizable read barrier.
func (t *HTTPTransport) ReadIndex(ctx context.Context, peer Peer, req ReadIndexRequest) (ReadIndexResponse, error) {
	var resp ReadIndexResponse
	err := t.post(ctx, peer, readIndexPath, req, &resp)
	return resp, err
}

// Forward hands a client write to the leader.
func (t *HTTPTransport) Forward(ctx context.Context, peer Peer, req ForwardRequest) (ForwardResponse, error) {
	var resp ForwardResponse
	err := t.post(ctx, peer, forwardPath, req, &resp)
	return resp, err
}

// post is the shared plumbing: encode, send, check, decode.
func (t *HTTPTransport) post(ctx context.Context, peer Peer, path string, req, out any) error {
	body, err := json.Marshal(req)
	if err != nil {
		return fmt.Errorf("raft: encode request: %w", err)
	}

	url := "http://" + peer.Addr + path
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("raft: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	httpResp, err := t.client.Do(httpReq)
	if err != nil {
		// A peer being down is normal, not exceptional. The caller treats
		// this as "no reply" and tries again later.
		return fmt.Errorf("raft: call %s: %w", peer.ID, err)
	}
	defer httpResp.Body.Close()

	if httpResp.StatusCode != http.StatusOK {
		return fmt.Errorf("raft: peer %s returned %s", peer.ID, httpResp.Status)
	}

	if err := json.NewDecoder(httpResp.Body).Decode(out); err != nil {
		return fmt.Errorf("raft: decode response: %w", err)
	}
	return nil
}

// PeerHandler is the server side: the HTTP endpoint peers call. It listens on
// a separate port from client traffic, so peer RPCs and user requests can be
// firewalled, rate-limited and monitored independently.
func PeerHandler(n *Node) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("POST "+appendEntriesPath, func(w http.ResponseWriter, r *http.Request) {
		var req AppendEntriesRequest

		// Cap the body: a peer -- or something pretending to be one -- must
		// not be able to make us allocate without bound.
		r.Body = http.MaxBytesReader(w, r.Body, 8<<20)

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request\n", http.StatusBadRequest)
			return
		}

		resp, err := n.HandleAppendEntries(req)
		if err != nil {
			// A disk failure while appending. Do NOT reply "success: false",
			// which would mean "my log does not match" -- a lie that would
			// send the leader walking backwards for no reason.
			http.Error(w, "append failed\n", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("POST "+readIndexPath, func(w http.ResponseWriter, r *http.Request) {
		var req ReadIndexRequest
		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request\n", http.StatusBadRequest)
			return
		}

		resp, err := n.HandleReadIndex(r.Context(), req)
		if err != nil {
			http.Error(w, "read index failed\n", http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("POST "+forwardPath, func(w http.ResponseWriter, r *http.Request) {
		var req ForwardRequest
		r.Body = http.MaxBytesReader(w, r.Body, 8<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request\n", http.StatusBadRequest)
			return
		}

		resp := n.HandleForward(r.Context(), req)
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	mux.HandleFunc("POST "+requestVotePath, func(w http.ResponseWriter, r *http.Request) {
		var req RequestVoteRequest

		r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, "bad request\n", http.StatusBadRequest)
			return
		}

		resp, err := n.HandleRequestVote(req)
		if err != nil {
			// We could not persist the vote. Replying "denied" would be a
			// lie we might contradict after a restart, so say nothing
			// definitive: the candidate simply gets no vote from us.
			http.Error(w, "vote failed\n", http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(resp)
	})

	return mux
}
