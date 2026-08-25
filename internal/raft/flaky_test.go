package raft

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"
)

// flakyTransport is a Transport that behaves like a real network instead of
// like localhost: it drops messages, delays them by random amounts, and
// sometimes delivers them twice.
//
// This is why Transport is an interface. Every hard case in Raft -- a vote
// request that vanishes, an AppendEntries that arrives twice, a reply that
// overtakes an earlier one -- is unreachable over loopback HTTP, which
// essentially never loses or reorders anything. With zero rates it behaves
// exactly like the plain transport, so the same helper serves both.
type flakyTransport struct {
	inner Transport
	owner *member

	// dropRate is the fraction of messages that never arrive.
	dropRate float64
	// dupRate is the fraction of AppendEntries delivered twice. Duplicates
	// are the interesting case: they must be idempotent, or a delayed retry
	// can truncate committed entries.
	dupRate float64
	// maxDelay is the upper bound on added latency. Random delays on
	// concurrent calls also produce REORDERING for free.
	maxDelay time.Duration

	sent    atomic.Int64
	dropped atomic.Int64
	duped   atomic.Int64
	delayed atomic.Int64
}

var errDropped = errors.New("simulated packet loss")

// deliver decides what happens to one outbound message.
func (f *flakyTransport) deliver(ctx context.Context) error {
	if !f.owner.up.Load() {
		return errors.New("partitioned")
	}
	f.sent.Add(1)

	if f.dropRate > 0 && rand.Float64() < f.dropRate {
		f.dropped.Add(1)
		return errDropped
	}

	if f.maxDelay > 0 {
		f.delayed.Add(1)
		select {
		case <-time.After(time.Duration(rand.Int63n(int64(f.maxDelay)))):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (f *flakyTransport) AppendEntries(ctx context.Context, p Peer, req AppendEntriesRequest) (AppendEntriesResponse, error) {
	if err := f.deliver(ctx); err != nil {
		return AppendEntriesResponse{}, err
	}

	resp, err := f.inner.AppendEntries(ctx, p, req)

	// Deliver it a second time. The follower must treat the duplicate as a
	// no-op, not as a reason to truncate.
	if err == nil && f.dupRate > 0 && rand.Float64() < f.dupRate {
		f.duped.Add(1)
		if _, dupErr := f.inner.AppendEntries(ctx, p, req); dupErr != nil {
			_ = dupErr // a failed duplicate is just another dropped message
		}
	}
	return resp, err
}

func (f *flakyTransport) RequestVote(ctx context.Context, p Peer, req RequestVoteRequest) (RequestVoteResponse, error) {
	if err := f.deliver(ctx); err != nil {
		return RequestVoteResponse{}, err
	}
	return f.inner.RequestVote(ctx, p, req)
}

func (f *flakyTransport) ReadIndex(ctx context.Context, p Peer, req ReadIndexRequest) (ReadIndexResponse, error) {
	if err := f.deliver(ctx); err != nil {
		return ReadIndexResponse{}, err
	}
	return f.inner.ReadIndex(ctx, p, req)
}

func (f *flakyTransport) Forward(ctx context.Context, p Peer, req ForwardRequest) (ForwardResponse, error) {
	if err := f.deliver(ctx); err != nil {
		return ForwardResponse{}, err
	}
	return f.inner.Forward(ctx, p, req)
}

func (f *flakyTransport) String() string {
	return fmt.Sprintf("sent=%d dropped=%d (%.0f%%) duplicated=%d delayed=%d",
		f.sent.Load(), f.dropped.Load(),
		100*float64(f.dropped.Load())/float64(max(f.sent.Load(), 1)),
		f.duped.Load(), f.delayed.Load())
}
