package dht

import (
	"context"
	"sort"
	"sync"
)

// routingTable 是按 XOR 距离最高有效位分桶的 k-bucket 路由表。
type routingTable struct {
	t       *transport
	k       int
	mu      sync.RWMutex
	buckets [256][]Contact // 索引 = 与 myid 的 XOR 距离前导零位数
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

// update 把一个 contact 加入路由表，实现 Kademlia 的 LRU + 存活探测策略。
func (rt *routingTable) update(c Contact) {
	if c.ID == rt.t.myID() || c.ID.isZero() || c.Addr == "" {
		return
	}
	idx := rt.bucketIndex(c.ID)

	rt.mu.Lock()
	b := rt.buckets[idx]
	// 已存在：移动到队尾（最近活跃）。
	for i := range b {
		if b[i].ID == c.ID {
			b = append(append(b[:i:i], b[i+1:]...), c)
			rt.buckets[idx] = b
			rt.mu.Unlock()
			return
		}
	}
	// 有空位：直接追加。
	if len(b) < rt.k {
		rt.buckets[idx] = append(b, c)
		rt.mu.Unlock()
		return
	}
	// 桶满：探测最旧节点（队首），存活则保留旧节点，否则用新节点替换。
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
		rt.buckets[idx] = append(b[1:], oldest) // 旧节点移到队尾
	} else {
		rt.buckets[idx] = append(b[1:], cand) // 替换为新节点
	}
}

// closest 返回与 target 最近的 count 个节点。
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
