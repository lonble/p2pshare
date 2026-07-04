package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"p2pshare/internal/node"
	"p2pshare/internal/rpcapi"
)

func main() {
	addr := flag.String("addr", ":9000", "QUIC 监听/通告地址")
	rpcAddr := flag.String("rpc", "127.0.0.1:8000", "HTTP JSON-RPC 地址")
	bootstrap := flag.String("bootstrap", "", "逗号分隔的引导节点地址")
	dataDir := flag.String("data", "./p2pdata", "数据目录")
	flag.Parse()

	n, err := node.New(*addr, *dataDir)
	if err != nil {
		log.Fatalf("create node: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	n.Start(ctx)
	n.StartRepublish(ctx, 15*time.Minute) // 新增

	if *bootstrap != "" {
		var addrs []string
		for _, a := range strings.Split(*bootstrap, ",") {
			if a = strings.TrimSpace(a); a != "" {
				addrs = append(addrs, a)
			}
		}
		go n.Bootstrap(ctx, addrs)
	}

	srv := &http.Server{Addr: *rpcAddr, Handler: rpcapi.New(n)}
	go func() {
		log.Printf("JSON-RPC listening on http://%s/", *rpcAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("rpc server: %v", err)
		}
	}()

	id := n.Myid()
	log.Printf("node started  id=%s  quic=%s", id.String(), *addr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	log.Println("shutting down...")
	cancel()
	_ = srv.Close()
}
