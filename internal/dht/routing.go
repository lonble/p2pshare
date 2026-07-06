package dht

import (
	"context"
	"sort"
	"sync"
)

// routingTable is a k-bucket routing table partitioned by the most significant bit of XOR distance.
type routingTable struct {
	t       *transport
	k       int
	mu      sync.RWMutex
	buckets [256][]Contact // index = leading zeros of XOR distance from myid
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
	b := rt.buckets[idx]
	// Already exists: move to the end of the queue (most recently active).
	for i := range b {
		if b[i].ID == c.ID {
			b = append(append(b[:i:i], b[i+1:]...), c)
			rt.buckets[idx] = b
			rt.mu.Unlock()
			return
		}
	}
	// Has empty space: append directly.
	if len(b) < rt.k {
		rt.buckets[idx] = append(b, c)
		rt.mu.Unlock()
		return
	}
	// Bucket full: ping the oldest node (head of queue), keep the old node if it is alive, otherwise replace it with the new node.
	oldest := b[0]
	rt.mu.Unlock()
	go rt.tryReplace(idx, oldest, c)
}

func (rt *routingTable) tryReplace(idx int, oldest, cand Contact) {
	ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
	defer cancel()
	resp, err := rt.t.send(ctx, oldest, &Message{Type: TypePing})
	alive := err == nil && resp != nil
	rt.mu.Lock()
	defer rt.mu.Unlock()
	b := rt.buckets[idx]
	if len(b) == 0 || b[0].ID != oldest.ID {
		return
	}
	if alive {
		rt.buckets[idx] = append(b[1:], oldest) // Move old node to the end of the queue
	} else {
		rt.buckets[idx] = append(b[1:], cand) // Replace with new node
	}
}

// closest returns up to count nodes closest to the target.
func (rt *routingTable) closest(target ID, count int) []Contact {
	rt.mu.RLock()
	var all []Contact
	for _, b := range rt.buckets {
		all = append(all, b...)
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
		all = append(all, b...)
	}
	return all
}
