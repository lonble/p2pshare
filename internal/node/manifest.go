package node

import (
	"crypto/sha256"
	"encoding/binary"

	"p2pshare/internal/dht"
)

// Manifest 描述一个文件如何由内容寻址的块组成。
type Manifest struct {
	Name      string   `json:"name"`
	Size      int64    `json:"size"`
	ChunkSize int      `json:"chunk_size"`
	Chunks    []dht.ID `json:"chunks"` // 每块 SHA-256
}

// ChunkID 计算内容寻址用的键。
func ChunkID(data []byte) dht.ID { return dht.ID(sha256.Sum256(data)) }

// FileID 是文件的全局唯一键（即"磁力链接"），由 Manifest 内容派生。
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
