package dht

import (
	"bytes"
	"encoding/hex"
	"errors"
	"math/bits"
)

// idLen 是节点/内容标识的字节长度（256 bit），与 SHA-256 输出对齐，
// 使节点 ID 与内容键共享同一个 keyspace。
const idLen = 32

type ID [idLen]byte

func ParseID(s string) (ID, error) {
	var id ID
	b, err := hex.DecodeString(s)
	if err != nil {
		return id, err
	}
	if len(b) != idLen {
		return id, errors.New("invalid id length")
	}
	copy(id[:], b)
	return id, nil
}

func (id ID) String() string { return hex.EncodeToString(id[:]) }

// Xor 返回 XOR 距离度量结果。
func (a ID) Xor(b ID) ID {
	var r ID
	for i := range a {
		r[i] = a[i] ^ b[i]
	}
	return r
}

// Less 把 ID 当作大端无符号整数比较，用于按距离排序。
func (id ID) Less(o ID) bool { return bytes.Compare(id[:], o[:]) < 0 }

// LeadingZeros 返回前导零比特数（0..256），用于决定 k-bucket 下标。
func (id ID) LeadingZeros() int {
	n := 0
	for _, b := range id {
		if b == 0 {
			n += 8
			continue
		}
		n += bits.LeadingZeros8(b)
		break
	}
	return n
}

func (id ID) IsZero() bool {
	var zero ID
	return id == zero
}
