package dht

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// RPC 消息类型。
const (
	TypePing         = "PING"
	TypePong         = "PONG"
	TypeFindNode     = "FIND_NODE"
	TypeFindValue    = "FIND_VALUE"
	TypeStore        = "STORE"
	TypeAddProvider  = "ADD_PROVIDER"
	TypeGetProviders = "GET_PROVIDERS"
	TypeGetChunk     = "GET_CHUNK" // 文件层使用
)

// Message 同时用于请求和响应，保持线协议简单。
type Message struct {
	Type      string    `json:"type"`
	Sender    ID        `json:"sender"`              // 发送方，用于更新接收方路由表
	Key       ID        `json:"key,omitempty"`       // FIND_NODE/STORE/FIND_VALUE/PROVIDER/CHUNK 的键
	Value     []byte    `json:"value,omitempty"`     // 值或块数据（JSON 中 base64）
	Contacts  []Contact `json:"contacts,omitempty"`  // 返回的更近节点
	Providers []Contact `json:"providers,omitempty"` // 返回的 provider 列表
	Found     bool      `json:"found,omitempty"`
	Error     string    `json:"error,omitempty"`
}

const maxMsgSize = 8 << 20 // 8 MiB，足够容纳 base64 后的单块

// writeMsg 以 [4字节大端长度][JSON] 的帧格式写入一条消息。
func writeMsg(w io.Writer, m *Message) error {
	data, err := json.Marshal(m)
	if err != nil {
		return err
	}
	if len(data) > maxMsgSize {
		return errors.New("message too large")
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(data)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(data)
	return err
}

func readMsg(r io.Reader) (*Message, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > maxMsgSize {
		return nil, errors.New("message too large")
	}
	buf := make([]byte, n)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}
	var m Message
	if err := json.Unmarshal(buf, &m); err != nil {
		return nil, err
	}
	return &m, nil
}
