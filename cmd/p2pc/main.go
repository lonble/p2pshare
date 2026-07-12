package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand/v2"
	"net/http"
	"os"
	"strings"

	"p2pshare/internal/dht"
	"p2pshare/internal/rpcapi"
)

func main() {
	defaultAPI := os.Getenv("P2P_API")
	if defaultAPI == "" {
		defaultAPI = "127.0.0.1:8000"
	}
	apiFlag := flag.String("api", defaultAPI, "JSON-RPC server API address")
	flag.Parse()

	args := flag.Args()
	if len(args) < 1 {
		printUsage()
		os.Exit(1)
	}

	command := args[0]
	cmdArgs := args[1:]
	apiAddress := *apiFlag

	switch command {
	case rpcapi.MethodStatus:
		var res rpcapi.StatusResult
		callRPC(apiAddress, command, nil, &res)
		fmt.Println("Node Status")
		fmt.Printf("  ID: %s\n", res.ID)
		fmt.Printf("  Peers: %d\n", res.Peers)

	case rpcapi.MethodPeers:
		var res rpcapi.PeersResult
		callRPC(apiAddress, command, nil, &res)
		fmt.Printf("Peers (%d):\n", len(res))
		for _, p := range res {
			fmt.Printf(" - %v\n", p)
		}

	case rpcapi.MethodListFiles:
		var res rpcapi.ListFilesResult
		callRPC(apiAddress, command, nil, &res)
		fmt.Printf("%-64s | %-10s | %-10s | %-6s | %s\n", "ID", "SIZE", "CHUNKSIZE", "CHUNKS", "NAME")
		for _, f := range res {
			fmt.Printf("%-64v | %-10s | %-10s | %-6d | %s\n", f.ID, formatBytes(f.Size), formatBytes(f.ChunkSize), f.Chunks, f.Name)
		}

	case rpcapi.MethodPublish:
		if len(cmdArgs) != 1 {
			fatalf("Usage: p2pc publish <path>")
		}
		params := &rpcapi.PublishParams{Path: cmdArgs[0]}
		var res rpcapi.PublishResult
		callRPC(apiAddress, command, params, &res)
		fmt.Println("Publish")
		fmt.Printf("  ID: %v\n", res.ID)
		fmt.Printf("  Name: %s\n", res.Manifest.Name)
		fmt.Printf("  Size: %s\n", formatBytes(res.Manifest.Size))
		fmt.Printf("  Chunk Size: %s\n", formatBytes(res.Manifest.ChunkSize))
		fmt.Printf("  Chunks: %d\n", len(res.Manifest.Chunks))

	case rpcapi.MethodDownload:
		if len(cmdArgs) < 1 || len(cmdArgs) > 2 {
			fatalf("Usage: p2pc download <id> [outdir]")
		}
		id, err := dht.ParseID(cmdArgs[0])
		if err != nil {
			fatalf("Error: %s", err.Error())
		}
		params := &rpcapi.DownloadParams{
			ID: id,
		}
		if len(cmdArgs) == 2 {
			params.OutDir = cmdArgs[1]
		}
		var res rpcapi.DownloadResult
		callRPC(apiAddress, command, params, &res)
		if res.OK {
			fmt.Println("Download complete")
			fmt.Printf("  Saved to: %s\n", res.Output)
		}

	case rpcapi.MethodBootstrap:
		if len(cmdArgs) < 1 {
			fatalf("Usage: p2pc bootstrap <id,addr> [id,addr ...]")
		}
		var contacts rpcapi.BootstrapParams
		for _, arg := range cmdArgs {
			parts := strings.SplitN(arg, ",", 2)
			if len(parts) != 2 {
				fatalf("Invalid format '%s'. Expected 'id,addr'", arg)
			}
			id, err := dht.ParseID(parts[0])
			if err != nil {
				fatalf("Error: %s", err.Error())
			}
			contacts = append(contacts, dht.Contact{ID: id, Addr: parts[1]})
		}
		var res rpcapi.BootstrapResult
		callRPC(apiAddress, command, contacts, &res)
		if res.OK {
			fmt.Println("Bootstrap successfully")
		} else {
			fmt.Println("All bootstrap nodes are inaccessible")
		}

	default:
		fatalf("Unknown command: %s", command)
	}
}

func callRPC(addr string, method string, params any, result any) {
	url := "http://" + addr + "/"
	request := rpcapi.RpcRequest{
		JSONRPC: "2.0",
		Method:  method,
		ID:      rand.Int(),
	}

	if params != nil {
		b, err := json.Marshal(params)
		if err != nil {
			fatalf("Failed to encode params: %v", err)
		}
		request.Params = b
	}

	reqBytes, err := json.Marshal(request)
	if err != nil {
		fatalf("Failed to encode request: %v", err)
	}
	resp, err := http.Post(url, "application/json", bytes.NewReader(reqBytes))
	if err != nil {
		fatalf("HTTP request failed: %v", err)
	}
	defer resp.Body.Close()

	// 使用一个临时的结构体来延迟对 Result 字段的反序列化
	var response rpcapi.RpcResponse

	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		fatalf("Failed to decode response: %v", err)
	}

	if response.Error != nil {
		fatalf("RPC Error [%d]: %s", response.Error.Code, response.Error.Message)
	}

	if result != nil && len(response.Result) > 0 {
		if err := json.Unmarshal(response.Result, result); err != nil {
			fatalf("Failed to parse result into struct: %v", err)
		}
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func printUsage() {
	fmt.Println(`P2P Share CLI Client

Usage:
  p2pc [flags] <command> [arguments]

Commands:
  status                   Show node status
  peers                    List connected peers
  listFiles                List published files
  publish <path>           Publish a local file
  download <id> [outdir]   Download file (outdir defaults to '.')
  bootstrap <id,addr>...   Bootstrap DHT

Flags:
  -api string
        Server API Address (default "127.0.0.1:8000" or P2P_API env)`)
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
