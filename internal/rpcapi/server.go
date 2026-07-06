package rpcapi

import (
	"context"
	"encoding/json"
	"net/http"
	"path/filepath"

	"p2pshare/internal/dht"
	"p2pshare/internal/node"
)

// Server exposes JSON-RPC 2.0 over HTTP for standalone GUI invocation.
type Server struct {
	node *node.Node
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params"`
	ID      interface{}     `json:"id"`
}

type rpcError struct {
	Code    rpcErrorCode `json:"code"`
	Message string       `json:"message"`
}

type rpcResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result,omitempty"`
	Error   *rpcError   `json:"error,omitempty"`
	ID      interface{} `json:"id"`
}

type rpcErrorCode int

const (
	rpcParseError     rpcErrorCode = -32700
	rpcMethodNotFound rpcErrorCode = -32601
	rpcInvalidParams  rpcErrorCode = -32602
	rpcServerError    rpcErrorCode = -32000
)

var rpcErrorMap = map[rpcErrorCode]string{
	rpcParseError:     "Parse Error",
	rpcMethodNotFound: "Method Not Found",
	rpcInvalidParams:  "Invalid Params",
	rpcServerError:    "Server Error",
}

func newRpcError(code rpcErrorCode, msg string) *rpcError {
	e := &rpcError{Code: code, Message: rpcErrorMap[code]}
	if msg != "" {
		e.Message += ": " + msg
	}
	return e
}

func New(n *node.Node) *Server { return &Server{node: n} }

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

	var req rpcRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, rpcResponse{JSONRPC: "2.0", Error: newRpcError(rpcParseError, "")})
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
		id := s.node.MyID()
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
				"id":         m.FileID().String(),
				"name":       m.Name,
				"size":       m.Size,
				"chunk_size": m.ChunkSize,
				"chunks":     len(m.Chunks),
			})
		}
		return out, nil

	case "publish":
		var p struct {
			Path string `json:"path"`
		}
		if err := json.Unmarshal(params, &p); err != nil || p.Path == "" {
			return nil, newRpcError(rpcInvalidParams, "need {path}")
		}
		fh, m, err := s.node.Publish(p.Path)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return map[string]interface{}{"id": fh.String(), "manifest": m}, nil

	case "download":
		var p struct {
			FileID string `json:"id"`
			OutDir string `json:"outdir"`
		}
		if err := json.Unmarshal(params, &p); err != nil || p.FileID == "" {
			return nil, newRpcError(rpcInvalidParams, "need {id, outdir}")
		}
		id, err := dht.ParseID(p.FileID)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		filename, err := s.node.Download(context.Background(), id, p.OutDir)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return map[string]interface{}{"ok": true, "output": filepath.Join(p.OutDir, filename)}, nil

	case "bootstrap":
		var p []dht.Contact
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, newRpcError(rpcInvalidParams, "need [{id, addr}]")
		}
		err := s.node.Bootstrap(context.Background(), p)
		if err != nil {
			return nil, newRpcError(rpcServerError, err.Error())
		}
		return map[string]interface{}{"ok": true}, nil
	default:
		return nil, newRpcError(rpcMethodNotFound, "")
	}
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}
