package dht

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
)

// RPC message types.
const (
	TypePing         = "PING"
	TypePong         = "PONG"
	TypeFindNode     = "FIND_NODE"
	TypeAddProvider  = "ADD_PROVIDER"
	TypeGetProviders = "GET_PROVIDERS"
	TypeGetValue     = "GET_VALUE" // Used by the file layer
)

// Message is used for both requests and responses to keep the wire protocol simple.
type Message struct {
	Type      string    `json:"type"`
	Sender    ID        `json:"sender"`              // Sender, used to update the receiver's routing table
	Key       ID        `json:"key,omitempty"`       // Key for FIND_NODE/STORE/FIND_VALUE/PROVIDER/CHUNK
	Value     []byte    `json:"value,omitempty"`     // Value data (base64 in JSON)
	Contacts  []Contact `json:"contacts,omitempty"`  // Returned closer nodes
	Providers []Contact `json:"providers,omitempty"` // Returned provider list
	Error     string    `json:"error,omitempty"`
}

const maxMsgSize = 1 << 21 // 2 MiB, large enough to contain a single base64-encoded chunk

// writeMsg writes a message in a [4-byte big-endian length][JSON] framed format.
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
