package rpcapi

import (
	"encoding/json"

	"p2pshare/internal/dht"
	"p2pshare/internal/node"
)

const (
	MethodStatus          = "status"
	MethodPeers           = "peers"
	MethodListFiles       = "listFiles"
	MethodPublish         = "publish"
	MethodPublishFrontend = "publishFrontend"
	MethodDownload        = "download"
	MethodBootstrap       = "bootstrap"
)

type RpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

type RpcErrorCode int

type RpcError struct {
	Code    RpcErrorCode `json:"code"`
	Message string       `json:"message"`
}

type RpcResponse struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      any             `json:"id"`
	Result  json.RawMessage `json:"result,omitempty"`
	Error   *RpcError       `json:"error,omitempty"`
}

type StatusResult struct {
	ID    dht.ID `json:"id"`
	Peers int    `json:"peers"`
}

type PeersResult []dht.Contact

type ListFilesResult []*ListFilesResultEntry

type ListFilesResultEntry struct {
	ID        dht.ID `json:"id"`
	Name      string `json:"name"`
	Size      int64  `json:"size"`
	ChunkSize int64  `json:"chunk_size"`
	Chunks    int    `json:"chunks"`
}

type PublishParams struct {
	Path string `json:"path"`
}

type PublishParamsFront struct {
	Name string `json:"name"`
	Data []byte `json:"data"`
}

type PublishResult struct {
	ID       dht.ID         `json:"id"`
	Manifest *node.Manifest `json:"manifest"`
}

type DownloadParams struct {
	ID     dht.ID `json:"id"`
	OutDir string `json:"outdir"`
}

type DownloadResult struct {
	OK     bool   `json:"ok"`
	Output string `json:"output"`
}

type BootstrapParams []dht.Contact

type BootstrapResult struct {
	OK bool `json:"ok"`
}
