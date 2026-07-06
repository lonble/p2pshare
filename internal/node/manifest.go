package node

import (
	"crypto/sha256"
	"encoding/binary"

	"p2pshare/internal/dht"
)

// Manifest describes how a file consists of content-addressable chunks.
type Manifest struct {
	Name      string   `json:"name"`
	Size      int64    `json:"size"`
	ChunkSize int      `json:"chunk_size"`
	Chunks    []dht.ID `json:"chunks"` // SHA-256 for each chunk
}

// ChunkID calculates the key used for content addressing.
func ChunkID(data []byte) dht.ID { return dht.ID(sha256.Sum256(data)) }

// FileID is the globally unique key of the file (i.e., "magnet link"), derived from the Manifest content.
func (m *Manifest) FileID() dht.ID {
	h := sha256.New()
	h.Write([]byte(m.Name))
	_ = binary.Write(h, binary.BigEndian, m.Size)
	for _, c := range m.Chunks {
		h.Write(c[:])
	}
	var id dht.ID
	copy(id[:], h.Sum(nil))
	return id
}
