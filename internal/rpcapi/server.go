package rpcapi

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"

	"p2pshare/internal/node"
)

// Server exposes JSON-RPC 2.0 over HTTP for standalone GUI invocation.
type Server struct {
	node *node.Node
}

const (
	rpcParseError     RpcErrorCode = -32700
	rpcMethodNotFound RpcErrorCode = -32601
	rpcInvalidParams  RpcErrorCode = -32602
	rpcInternalError  RpcErrorCode = -32603
	rpcServerError    RpcErrorCode = -32000
)

var rpcErrorMap = map[RpcErrorCode]string{
	rpcParseError:     "Parse Error",
	rpcMethodNotFound: "Method Not Found",
	rpcInvalidParams:  "Invalid Params",
	rpcInternalError:  "Internal Error",
	rpcServerError:    "Server Error",
}

func newRpcError(code RpcErrorCode, msg string) *RpcError {
	e := &RpcError{Code: code, Message: rpcErrorMap[code]}
	if msg != "" {
		e.Message += ": " + msg
	}
	return e
}

func New(n *node.Node) *Server { return &Server{node: n} }

const maxUploadSize = 500 << 20

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	if r.Method == http.MethodOptions {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST Only", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, maxUploadSize/3*4+1<<20)

	var req RpcRequest
	resp := &RpcResponse{JSONRPC: "2.0"}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		resp.Error = newRpcError(rpcParseError, err.Error())
		writeJSON(w, resp)
		return
	}

	resp.ID = req.ID
	result, err := s.dispatch(r.Context(), req.Method, req.Params)
	if err != nil {
		resp.Error = err
	} else {
		rb, err := json.Marshal(result)
		if err != nil {
			resp.Error = newRpcError(rpcInternalError, err.Error())
		} else {
			resp.Result = rb
		}
	}
	writeJSON(w, resp)
}

func (s *Server) dispatch(ctx context.Context, method string, params json.RawMessage) (any, *RpcError) {
	switch method {
	case MethodStatus:
		return &StatusResult{
			ID:    s.node.MyID(),
			Peers: len(s.node.Peers()),
		}, nil

	case MethodPeers:
		return PeersResult(s.node.Peers()), nil

	case MethodListFiles:
		var result ListFilesResult
		for id, m := range s.node.Manifests() {
			result = append(result, &ListFilesResultEntry{
				ID:        id,
				Name:      m.Name,
				Size:      m.Size,
				ChunkSize: m.ChunkSize,
				Chunks:    len(m.Chunks),
			})
		}
		if len(result) == 0 {
			result = ListFilesResult{}
		}
		return result, nil

	case MethodPublish:
		var p PublishParams
		if err := json.Unmarshal(params, &p); err != nil || p.Path == "" {
			return nil, newRpcError(rpcInvalidParams, "need {path}")
		}
		fh, m, err := s.node.Publish(p.Path, nil)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return &PublishResult{ID: fh, Manifest: m}, nil

	case MethodPublishFrontend:
		var p PublishParamsFront
		if err := json.Unmarshal(params, &p); err != nil || p.Name == "" || len(p.Data) == 0 {
			return nil, newRpcError(rpcInvalidParams, "need {name, data}")
		}
		if len(p.Data) > maxUploadSize {
			return nil, newRpcError(rpcInvalidParams, "file exceeds the 500 MiB upload limit")
		}
		path, cleanup, err := s.node.SaveUpload(p.Name, p.Data)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		defer cleanup()
		fh, m, err := s.node.Publish(path, nil)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return &PublishResult{ID: fh, Manifest: m}, nil

	case MethodDownload:
		var p DownloadParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, newRpcError(rpcInvalidParams, "need {id, outdir}")
		}
		filename, err := s.node.Download(ctx, p.ID, p.OutDir, nil)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return &DownloadResult{OK: true, Output: filepath.Join(p.OutDir, filename)}, nil
	case MethodBootstrap:
		var p BootstrapParams
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, newRpcError(rpcInvalidParams, "need [{id, addr}]")
		}
		ok := s.node.Bootstrap(ctx, p) > 0
		return &BootstrapResult{OK: ok}, nil
	default:
		return nil, newRpcError(rpcMethodNotFound, "")
	}
}

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
