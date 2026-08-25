package raft

import (
	"fmt"
	"sort"
	"strings"
)

// NodeID names one node in the cluster. It must be stable across restarts:
// a node's identity is what its votes and its log are attributed to.
type NodeID string

// Peer is another node we can send messages to.
type Peer struct {
	ID   NodeID
	Addr string // host:port, e.g. "localhost:9002"
}

// Config describes this node's view of the cluster.
type Config struct {
	ID    NodeID // who am I
	Addr  string // where other nodes reach me
	Peers []Peer // everyone else -- this node is NOT in the list
}

// ClusterSize counts every node including this one.
func (c Config) ClusterSize() int {
	return len(c.Peers) + 1
}

// Majority is how many nodes must agree before anything is committed.
//
// For a cluster of n, that is n/2 + 1 using integer division:
//
//	1 -> 1    3 -> 2    5 -> 3
//	2 -> 2    4 -> 3    6 -> 4
//
// Note 3 and 4 both tolerate exactly one failure, which is why real clusters
// are odd-sized: the fourth machine costs money and buys nothing.
func (c Config) Majority() int {
	return c.ClusterSize()/2 + 1
}

// Validate catches misconfiguration at startup rather than at 3am.
func (c Config) Validate() error {
	if c.ID == "" {
		return fmt.Errorf("raft: node id is required")
	}
	if c.Addr == "" {
		return fmt.Errorf("raft: node address is required")
	}

	seen := map[NodeID]bool{c.ID: true}
	for _, p := range c.Peers {
		if p.ID == "" || p.Addr == "" {
			return fmt.Errorf("raft: peer %+v is missing an id or address", p)
		}
		if seen[p.ID] {
			// Either a duplicate peer, or this node listed as its own peer.
			// Both would corrupt vote counting: one node, two votes.
			return fmt.Errorf("raft: duplicate node id %q", p.ID)
		}
		seen[p.ID] = true
	}

	if c.ClusterSize()%2 == 0 {
		// Not fatal, but you are paying for a node that adds no fault
		// tolerance. Worth saying out loud.
		return fmt.Errorf("raft: cluster size %d is even; %d and %d tolerate the same single failure",
			c.ClusterSize(), c.ClusterSize()-1, c.ClusterSize())
	}

	return nil
}

// PeerByID looks up one peer.
func (c Config) PeerByID(id NodeID) (Peer, bool) {
	for _, p := range c.Peers {
		if p.ID == id {
			return p, true
		}
	}
	return Peer{}, false
}

// ParsePeers turns a flag value like "2=localhost:9002,3=localhost:9003"
// into a peer list, sorted by ID so every node builds the same view.
func ParsePeers(s string) ([]Peer, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return nil, nil
	}

	var peers []Peer
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}

		id, addr, found := strings.Cut(part, "=")
		if !found {
			return nil, fmt.Errorf("raft: peer %q must look like id=host:port", part)
		}

		id, addr = strings.TrimSpace(id), strings.TrimSpace(addr)
		if id == "" || addr == "" {
			return nil, fmt.Errorf("raft: peer %q must look like id=host:port", part)
		}

		peers = append(peers, Peer{ID: NodeID(id), Addr: addr})
	}

	sort.Slice(peers, func(i, j int) bool { return peers[i].ID < peers[j].ID })
	return peers, nil
}
