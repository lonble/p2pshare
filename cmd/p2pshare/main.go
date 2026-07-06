package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"p2pshare/internal/node"
	"p2pshare/internal/rpcapi"
)

func main() {
	addr := flag.String("addr", ":9000", "QUIC listen address")
	rpcAddr := flag.String("rpc", "127.0.0.1:8000", "HTTP JSON-RPC address")
	dataDir := flag.String("data", "./p2pdata", "Data directory")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n, err := node.StartNode(*addr, *dataDir, ctx)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	n.StartRepublish(ctx, 15*time.Minute)

	srv := &http.Server{Addr: *rpcAddr, Handler: rpcapi.New(n)}
	go func() {
		log.Printf("JSON-RPC listening on http://%s/", *rpcAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("rpc server: %v", err)
		}
	}()

	id := n.MyID()
	log.Printf("node started  id=%s  quic=%s", id.String(), *addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")
	cancel()
	srv.Close()
}
