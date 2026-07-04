package rpcapi

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"

	"p2pshare/internal/dht"
	"p2pshare/internal/node"
)

// Server 暴露 JSON-RPC 2.0 over HTTP，供独立 GUI 调用。
type Server struct {
	node *node.Node
}

func New(n *node.Node) *Server { return &Server{node: n} }

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
	if r.Method == http.MethodOptions {
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}

	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, rpcResponse{JSONRPC: "2.0", Error: &rpcError{-32700, "parse error"}})
		return
	}

	result, rerr := s.dispatch(req.Method, req.Params)
	resp := rpcResponse{JSONRPC: "2.0", ID: req.ID}
	if rerr != nil {
		resp.Error = rerr
	} else {
		resp.Result = result
	}
	writeJSON(w, resp)
}

func (s *Server) dispatch(method string, params json.RawMessage) (interface{}, *rpcError) {
	switch method {
	case "status":
		id := s.node.Myid()
		return map[string]interface{}{
			"id":    id.String(),
			"peers": len(s.node.Peers()),
		}, nil

	case "peers":
		return s.node.Peers(), nil

	case "listFiles":
		var out []map[string]interface{}
		for _, m := range s.node.Manifests() {
			out = append(out, map[string]interface{}{
				"id":     m.FileID().String(),
				"name":   m.Name,
				"size":   m.Size,
				"chunks": len(m.Chunks),
			})
		}
		return out, nil

	case "publish":
		var p struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(params, &p); err != nil || p.Path == "" {
			return nil, &rpcError{-32602, "invalid params: need {path}"}
		}
		fh, m, err := s.node.Publish(p.Path)
		if err != nil {
			return nil, &rpcError{-32000, err.Error()}
		}
		return map[string]interface{}{"id": fh.String(), "manifest": m}, nil

	case "download":
		var p struct {
			FileID string `json:"id"`
			OutDir string `json:"outdir"`
		}
		if err := json.Unmarshal(params, &p); err != nil || p.FileID == "" || p.OutDir == "" {
			return nil, &rpcError{-32602, "invalid params: need {id, outdir}"}
		}
		id, err := dht.ParseID(p.FileID)
		if err != nil {
			return nil, &rpcError{-32000, err.Error()}
		}
		filename, err := s.node.Download(context.Background(), id, p.OutDir)
		if err != nil {
			return nil, &rpcError{-32000, err.Error()}
		}
		return map[string]interface{}{"ok": true, "output": filepath.Join(p.OutDir, filename)}, nil

	default:
		return nil, &rpcError{-32601, "method not found"}
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(v)
}
