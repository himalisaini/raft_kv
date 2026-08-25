package raft

import "testing"

func TestMajority(t *testing.T) {
	cases := []struct {
		size       int
		want       int
		canSurvive int // failures tolerated
	}{
		{1, 1, 0},
		{2, 2, 0},
		{3, 2, 1},
		{4, 3, 1},
		{5, 3, 2},
		{6, 4, 2},
		{7, 4, 3},
	}

	for _, tc := range cases {
		c := Config{ID: "self", Addr: "a"}
		for i := 1; i < tc.size; i++ {
			c.Peers = append(c.Peers, Peer{ID: NodeID(string(rune('a' + i))), Addr: "x"})
		}

		if got := c.Majority(); got != tc.want {
			t.Errorf("cluster of %d: majority = %d, want %d", tc.size, got, tc.want)
		}
		if got := tc.size - tc.want; got != tc.canSurvive {
			t.Errorf("cluster of %d: survives %d failures, want %d", tc.size, got, tc.canSurvive)
		}
	}
}

func TestValidate(t *testing.T) {
	good := Config{
		ID:   "1",
		Addr: "localhost:9001",
		Peers: []Peer{
			{ID: "2", Addr: "localhost:9002"},
			{ID: "3", Addr: "localhost:9003"},
		},
	}
	if err := good.Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}

	// A node listed as its own peer would vote for itself twice.
	selfPeer := good
	selfPeer.Peers = append([]Peer{{ID: "1", Addr: "localhost:9001"}}, good.Peers...)
	if err := selfPeer.Validate(); err == nil {
		t.Fatal("expected an error when a node lists itself as a peer")
	}

	missingID := Config{Addr: "localhost:9001"}
	if err := missingID.Validate(); err == nil {
		t.Fatal("expected an error for a missing node id")
	}

	evenCluster := Config{ID: "1", Addr: "a", Peers: []Peer{{ID: "2", Addr: "b"}}}
	if err := evenCluster.Validate(); err == nil {
		t.Fatal("expected a warning for an even cluster size")
	}
}

func TestParsePeers(t *testing.T) {
	peers, err := ParsePeers("3=localhost:9003, 2=localhost:9002")
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if len(peers) != 2 {
		t.Fatalf("got %d peers, want 2", len(peers))
	}
	// Sorted, so every node in the cluster agrees on peer order.
	if peers[0].ID != "2" || peers[1].ID != "3" {
		t.Fatalf("peers not sorted: %+v", peers)
	}
	if peers[0].Addr != "localhost:9002" {
		t.Fatalf("addr = %q", peers[0].Addr)
	}

	if _, err := ParsePeers("localhost:9002"); err == nil {
		t.Fatal("expected an error for a peer with no id=")
	}
	if p, err := ParsePeers(""); err != nil || p != nil {
		t.Fatalf("empty string should give no peers, got %v %v", p, err)
	}
}
