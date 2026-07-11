package dht

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/bits"
	"strconv"
)

// idLen is the byte length of the node/content identifier (256-bit), aligning with the output of SHA-256,
// allowing node IDs and content keys to share the same keyspace.
const idLen = 32

type ID [idLen]byte

func (id ID) MarshalJSON() ([]byte, error) {
	return []byte(strconv.Quote(id.String())), nil
}

func (id *ID) UnmarshalJSON(data []byte) error {
	str, err := strconv.Unquote(string(data))
	if err != nil {
		return err
	}
	tempid, err := ParseID(str)
	if err != nil {
		return err
	}
	*id = tempid
	return nil
}

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

// ChunkID calculates the key used for content addressing.
func ChunkID(data []byte) ID { return ID(sha256.Sum256(data)) }

// Return the result of the XOR distance metric.
func (a ID) xor(b ID) ID {
	var r ID
	for i := range a {
		r[i] = a[i] ^ b[i]
	}
	return r
}

// Compare IDs as big-endian unsigned integers, used for sorting by distance.
func (id ID) less(o ID) bool { return bytes.Compare(id[:], o[:]) < 0 }

// Return the number of leading zero bits (0..256), used to determine the k-bucket index.
func (id ID) leadingZeros() int {
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

func (id ID) isZero() bool {
	var zero ID
	return id == zero
}
