// Command kvnode runs one node of the key-value store. Run three of them,
// each pointed at the other two, to get a cluster:
//
//	kvnode --id=1 --addr=:8001 --raft-addr=localhost:9001 \
//	       --data=./data/node1 --peers=2=localhost:9002,3=localhost:9003
package main

import (
	"context"
	"errors"
	"flag"
	"log"
	"net/http"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/himalisaini/raftkv/internal/api"
	"github.com/himalisaini/raftkv/internal/engine"
	"github.com/himalisaini/raftkv/internal/raft"
)

func main() {
	addr := flag.String("addr", ":8001", "client-facing address to listen on")
	dataDir := flag.String("data", "./data/node1", "directory for this node's log")
	id := flag.String("id", "1", "this node's stable id")
	raftAddr := flag.String("raft-addr", "localhost:9001", "address peers reach this node on")
	noReadBatch := flag.Bool("no-read-batching", false,
		"benchmark only: give every strong read its own quorum round trip")
	peerList := flag.String("peers", "", "other nodes, e.g. 2=localhost:9002,3=localhost:9003")
	flag.Parse()

	// Work out who we are and who else is in the cluster. A bad cluster
	// definition should stop the node now, not surface as a split brain later.
	peers, err := raft.ParsePeers(*peerList)
	if err != nil {
		log.Fatalf("peers: %v", err)
	}
	cfg := raft.Config{ID: raft.NodeID(*id), Addr: *raftAddr, Peers: peers}
	if err := cfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}
	log.Printf("node %s: cluster of %d, majority is %d, peers %v",
		cfg.ID, cfg.ClusterSize(), cfg.Majority(), cfg.Peers)

	// 1. Recover from disk. This replays the WAL before we accept traffic,
	//    so a client can never read a half-recovered state.
	// The state machine. It holds no durable state of its own -- everything
	// it knows is rebuilt by replaying the Raft log.
	e := engine.New()

	rlog, err := raft.OpenLog(filepath.Join(*dataDir, "raft.log"))
	if err != nil {
		log.Fatalf("open raft log: %v", err)
	}
	defer rlog.Close()

	// currentTerm and votedFor live in their own small file, separate from
	// the log, because they are rewritten constantly and the log is not.
	stateStore, restored, err := raft.OpenStateStore(*dataDir)
	if err != nil {
		log.Fatalf("open state: %v", err)
	}

	node, err := raft.NewNode(raft.Options{
		Config:    cfg,
		Log:       rlog,
		State:     stateStore,
		Transport: raft.NewHTTPTransport(2 * time.Second),
		Apply:     e.Apply,

		DisableReadBatching: *noReadBatch,
	}, restored)
	if err != nil {
		log.Fatalf("new node: %v", err)
	}
	log.Printf("node %s: restored term=%d votedFor=%q",
		node.ID(), restored.CurrentTerm, restored.VotedFor)
	log.Printf("node %s: raft log recovered, %d entries, last index %d term %d",
		node.ID(), rlog.Len(), rlog.LastIndex(), rlog.LastTerm())

	// Start the node's own clock: election timeouts when it is a follower,
	// heartbeats when it is the leader. Nobody is appointed leader any
	// more -- the cluster works it out for itself.
	node.Start()
	defer node.Stop()

	// Peer traffic gets its own listener. Keeping it off the client port
	// means the two can be firewalled and monitored independently.
	peerSrv := &http.Server{
		Addr:              *raftAddr,
		Handler:           raft.PeerHandler(node),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		log.Printf("node %s: peer listener on %s", node.ID(), *raftAddr)
		if err := peerSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("peer serve: %v", err)
		}
	}()

	srv := &http.Server{
		Addr:    *addr,
		Handler: api.NewServer(node, e),

		// A client that opens a connection and sends headers one byte per
		// minute would otherwise tie up a goroutine forever (a "slowloris").
		ReadHeaderTimeout: 5 * time.Second,
		WriteTimeout:      10 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	// 2. Serve in the background so main can wait for a shutdown signal.
	go func() {
		log.Printf("kvnode listening on %s, data in %s", *addr, *dataDir)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("serve: %v", err)
		}
	}()

	// 3. Block until Ctrl-C or `kill`. NotifyContext cancels ctx on signal.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	<-ctx.Done()

	// 4. Graceful shutdown: stop accepting new connections, let in-flight
	//    requests finish, then give up after 5 seconds.
	log.Println("shutting down...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("forced shutdown: %v", err)
	}
	if err := peerSrv.Shutdown(shutdownCtx); err != nil {
		log.Printf("forced peer shutdown: %v", err)
	}
	log.Println("stopped")
}
