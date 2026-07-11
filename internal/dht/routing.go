package dht

import (
	"context"
	"sort"
	"sync"
)

type peerEntry struct {
	contact Contact
	pinging bool
}

// routingTable is a k-bucket routing table partitioned by the most significant bit of XOR distance.
type routingTable struct {
	t       *transport
	k       int
	mu      sync.RWMutex
	buckets [256][]*peerEntry // index = leading zeros of XOR distance from myid
}

func newRoutingTable(t *transport, k int) *routingTable {
	return &routingTable{t: t, k: k}
}

func (rt *routingTable) bucketIndex(key ID) int {
	lz := rt.t.myID().xor(key).leadingZeros()
	if lz >= 256 {
		panic("mistakenly try to add myself to the routing table")
	}
	return lz
}

// adds a contact to the routing table, implementing Kademlia's LRU + liveness detection strategy.
func (rt *routingTable) update(c Contact) {
	if c.ID == rt.t.myID() || c.ID.isZero() || c.Addr == "" {
		return
	}
	idx := rt.bucketIndex(c.ID)

	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := rt.buckets[idx]
	// Already exists: move to the end of the queue (most recently active).
	for i := range b {
		if b[i].contact == c {
			n := b[i]
			copy(b[i:], b[i+1:])
			b[len(b)-1] = n
			return
		}
	}
	// Has empty space: append directly.
	if len(b) < rt.k {
		rt.buckets[idx] = append(b, &peerEntry{contact: c, pinging: false})
		return
	}
	// Bucket full:
	// all peers are being pinged, drop the new peer
	if b[0].pinging {
		return
	}

	oldest := b[0]
	oldest.pinging = true // mark

	// optimistic early move
	copy(b[0:], b[1:])
	b[len(b)-1] = oldest

	// Ping the oldest peer, keep it if alive, otherwise replace it with the new peer.
	go rt.tryReplace(idx, oldest, c)
}

func (rt *routingTable) tryReplace(idx int, oldest *peerEntry, cand Contact) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := rt.t.send(ctx, oldest.contact, &Message{Type: TypePing})
	alive := err == nil && resp != nil

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if alive {
		oldest.pinging = false
		return
	}

	// check if the new peer is already added by another thread
	b := rt.buckets[idx]
	candExists := false
	for _, n := range b {
		if n.contact == cand {
			candExists = true
			break
		}
	}

	if candExists {
		// delete the oldest peer
		for i, n := range b {
			if n == oldest {
				copy(b[i:], b[i+1:])
				rt.buckets[idx] = b[:len(b)-1]
				break
			}
		}
	} else {
		// replace the oldest peer with the new peer
		oldest.contact = cand
		oldest.pinging = false
	}
}

// closest returns up to count nodes closest to the target.
func (rt *routingTable) closest(target ID, count int) []Contact {
	rt.mu.RLock()
	var all []Contact
	for _, b := range rt.buckets {
		for _, peer := range b {
			all = append(all, peer.contact)
		}
	}
	rt.mu.RUnlock()
	sort.Slice(all, func(i, j int) bool {
		return target.xor(all[i].ID).less(target.xor(all[j].ID))
	})
	if len(all) > count {
		all = all[:count]
	}
	return all
}

func (rt *routingTable) allContacts() []Contact {
	rt.mu.RLock()
	defer rt.mu.RUnlock()
	var all []Contact
	for _, b := range rt.buckets {
		for _, peer := range b {
			all = append(all, peer.contact)
		}
	}
	return all
}
