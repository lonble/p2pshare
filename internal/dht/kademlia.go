package dht

import (
	"context"
	"fmt"
	"net"
	"sort"
	"sync"
	"time"
)

const (
	k           = 20 // bucket 大小 / 冗余副本数
	alpha       = 3  // 查找并发度
	rpcTimeout  = 5 * time.Second
	valueTTL    = time.Hour
	providerTTL = 30 * time.Minute
)

type valueEntry struct {
	data []byte
	exp  time.Time
}

type providerEntry struct {
	c   Contact
	exp time.Time
}

type GetChunk func(id ID) ([]byte, error)
type ValueSource func(key ID) ([]byte, bool)

type Kademlia struct {
	t  *transport
	rt *routingTable

	mu        sync.Mutex
	values    map[ID]valueEntry
	providers map[ID][]providerEntry

	getChunk   GetChunk
	localValue ValueSource // 新增
}

func (kad *Kademlia) SetChunkHandler(handler GetChunk) { kad.getChunk = handler }

func (kad *Kademlia) SetValueSource(f ValueSource) { kad.localValue = f }

// getValueOrLocal 先查 DHT 存储，未命中再问注入的本地值源。
func (kad *Kademlia) getValueOrLocal(k ID) ([]byte, bool) {
	if v, ok := kad.getValue(k); ok {
		return v, true
	}
	if kad.localValue != nil {
		return kad.localValue(k)
	}
	return nil, false
}

func StartKademlia(listenAddr, certDir string, ctx context.Context) (*Kademlia, error) {
	t, err := startTransport(listenAddr, certDir, ctx)
	if err != nil {
		return nil, err
	}
	kad := &Kademlia{
		t:         t,
		rt:        newRoutingTable(t, k),
		values:    make(map[ID]valueEntry),
		providers: make(map[ID][]providerEntry),
	}
	t.setHandler(kad.HandleRPC)
	return kad, nil
}

func (kad *Kademlia) MyID() ID { return kad.t.myID() }

func (kad *Kademlia) Peers() []Contact { return kad.rt.allContacts() }

func (kad *Kademlia) SendRPC(ctx context.Context, c Contact, m *Message) (*Message, error) {
	return kad.t.send(ctx, c, m)
}

// ---------- 服务端：处理收到的 RPC ----------

func (kad *Kademlia) HandleRPC(remote net.Addr, msg *Message) *Message {
	contact := Contact{}
	if !msg.Sender.isZero() {
		contact = Contact{ID: msg.Sender, Addr: remote.String()}
		kad.rt.update(contact)
	}
	resp := &Message{Type: msg.Type}
	switch msg.Type {
	case TypePing:
		resp.Type = TypePong
	case TypeFindNode:
		resp.Contacts = kad.rt.closest(msg.Key, k)
	case TypeStore:
		kad.putValue(msg.Key, msg.Value)
	case TypeFindValue:
		if v, ok := kad.getValueOrLocal(msg.Key); ok { // 改这里
			resp.Value, resp.Found = v, true
		} else {
			resp.Contacts = kad.rt.closest(msg.Key, k)
		}
	case TypeAddProvider:
		kad.addProvider(msg.Key, contact)
	case TypeGetProviders:
		resp.Providers = kad.localProviders(msg.Key)
		resp.Contacts = kad.rt.closest(msg.Key, k)
	case TypeGetChunk:
		data, err := kad.getChunk(msg.Key)
		if err == nil {
			resp.Value = data
		} else {
			resp.Error = "chunk not found"
		}
	default:
		resp.Error = "unknown rpc"
	}
	return resp
}

// ---------- 本地存储 ----------

func (kad *Kademlia) putValue(k ID, v []byte) {
	kad.mu.Lock()
	kad.values[k] = valueEntry{data: v, exp: time.Now().Add(valueTTL)}
	kad.mu.Unlock()
}

func (kad *Kademlia) getValue(k ID) ([]byte, bool) {
	kad.mu.Lock()
	defer kad.mu.Unlock()
	e, ok := kad.values[k]
	if !ok || time.Now().After(e.exp) {
		if ok {
			delete(kad.values, k)
		}
		return nil, false
	}
	return e.data, true
}

func (kad *Kademlia) addProvider(k ID, c Contact) {
	if k.isZero() || c.ID.isZero() {
		return
	}
	kad.mu.Lock()
	kad.providers[k] = append(kad.providers[k], providerEntry{c: c, exp: time.Now().Add(providerTTL)})
	kad.mu.Unlock()
}

func (kad *Kademlia) localProviders(k ID) []Contact {
	kad.mu.Lock()
	defer kad.mu.Unlock()
	m := kad.providers[k]
	var out []Contact
	now := time.Now()
	for iter := len(m) - 1; iter >= 0; iter-- {
		if now.After(m[iter].exp) {
			// delete m[iter] from the slice
			m[iter] = m[len(m)-1]
			m = m[:len(m)-1]
			continue
		}
		out = append(out, m[iter].c)
	}
	return out
}

// ---------- 迭代查找（Kademlia 核心算法） ----------

type lookupMode int

const (
	modeFindNode lookupMode = iota
	modeFindValue
	modeProviders
)

func typeForMode(m lookupMode) string {
	switch m {
	case modeFindValue:
		return TypeFindValue
	case modeProviders:
		return TypeGetProviders
	case modeFindNode:
		return TypeFindNode
	default:
		panic(fmt.Sprintf("unexpected lookupMode: %d", m))
	}
}

type lookupOutcome struct {
	closest   []Contact
	value     []byte
	found     bool
	providers []Contact
}

func (kad *Kademlia) lookup(target ID, mode lookupMode) lookupOutcome {
	sl := newShortlist(target)
	sl.push(kad.rt.closest(target, k))
	provs := []Contact{}

	for {
		batch := sl.selectAlpha(alpha)
		if len(batch) == 0 {
			break // 最近 K 个节点均已查询，收敛
		}
		type result struct {
			from Contact
			msg  *Message
			err  error
		}
		ch := make(chan result, len(batch))
		for _, c := range batch {
			sl.setInflight(c.ID)
			go func(c Contact) {
				ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
				defer cancel()
				resp, err := kad.t.send(ctx, c, &Message{Type: typeForMode(mode), Key: target})
				ch <- result{c, resp, err}
			}(c)
		}
		for i := 0; i < len(batch); i++ {
			r := <-ch
			if r.err != nil || r.msg == nil {
				sl.markFailed(r.from.ID)
				continue
			}
			sl.markQueried(r.from.ID)
			kad.rt.update(r.from)
			if mode == modeFindValue && r.msg.Found {
				return lookupOutcome{value: r.msg.Value, found: true}
			}
			if mode == modeProviders {
				for _, p := range r.msg.Providers {
					// Addr == "" 表示被请求节点本身提供此文件
					if p.Addr == "" {
						p.Addr = r.from.Addr
					}
					provs = append(provs, p)
				}
			}
			sl.push(r.msg.Contacts)
		}
	}

	out := lookupOutcome{closest: sl.closest(k)}
	out.providers = provs
	return out
}

// ---------- 对外 DHT 操作 ----------

func (kad *Kademlia) Bootstrap(ctx context.Context, contacts []Contact) error {
	for _, c := range contacts {
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := kad.t.send(cctx, c, &Message{Type: TypePing})
		cancel()
		if err == nil && resp != nil {
			kad.rt.update(c)
		}
	}
	kad.lookup(kad.MyID(), modeFindNode) // 自查找以填充路由表
	return nil
}

func (kad *Kademlia) StoreValue(key ID, value []byte) int {
	kad.putValue(key, value)
	out := kad.lookup(key, modeFindNode)
	n := 0
	for _, c := range out.closest {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, err := kad.t.send(ctx, c, &Message{Type: TypeStore, Key: key, Value: value})
		cancel()
		if err == nil {
			n++
		}
	}
	return n
}

func (kad *Kademlia) FindValue(key ID) ([]byte, bool) {
	if v, ok := kad.getValueOrLocal(key); ok { // 改这里
		return v, true
	}
	out := kad.lookup(key, modeFindValue)
	return out.value, out.found
}

func (kad *Kademlia) Announce(key ID) int {
	// Addr: "" 表示节点本身提供此文件
	kad.addProvider(key, Contact{ID: kad.MyID(), Addr: ""})
	out := kad.lookup(key, modeFindNode)
	n := 0
	for _, c := range out.closest {
		ctx, cancel := context.WithTimeout(context.Background(), rpcTimeout)
		_, err := kad.t.send(ctx, c, &Message{Type: TypeAddProvider, Key: key})
		cancel()
		if err == nil {
			n++
		}
	}
	return n
}

func (kad *Kademlia) FindProviders(key ID) []Contact {
	res := []Contact{}
	for _, c := range kad.localProviders(key) {
		res = append(res, c)
	}
	out := kad.lookup(key, modeProviders)
	for _, c := range out.providers {
		res = append(res, c)
	}
	return res
}

// ---------- shortlist：迭代查找的候选集 ----------

const (
	stPending = iota
	stInflight
	stQueried
	stFailed
)

type slItem struct {
	c     Contact
	dist  ID
	state int
}

type shortlist struct {
	target ID
	items  []*slItem
	seen   map[ID]*slItem
}

func newShortlist(target ID) *shortlist {
	return &shortlist{target: target, seen: make(map[ID]*slItem)}
}

func (s *shortlist) push(cs []Contact) {
	for _, c := range cs {
		if c.Addr == "" || c.ID.isZero() {
			continue
		}
		if _, ok := s.seen[c.ID]; ok {
			continue
		}
		it := &slItem{c: c, dist: s.target.xor(c.ID), state: stPending}
		s.seen[c.ID] = it
		s.items = append(s.items, it)
	}
}

func (s *shortlist) sortItems() {
	sort.Slice(s.items, func(i, j int) bool { return s.items[i].dist.less(s.items[j].dist) })
}

// selectAlpha 在"最近 K 个未失败节点"窗口内挑选至多 a 个待查询节点。
func (s *shortlist) selectAlpha(a int) []Contact {
	s.sortItems()
	var out []Contact
	window := 0
	for _, it := range s.items {
		if it.state == stFailed {
			continue
		}
		window++
		if window > k {
			break
		}
		if it.state == stPending {
			out = append(out, it.c)
			if len(out) >= a {
				break
			}
		}
	}
	return out
}

func (s *shortlist) setInflight(id ID) {
	if it, ok := s.seen[id]; ok {
		it.state = stInflight
	}
}
func (s *shortlist) markQueried(id ID) {
	if it, ok := s.seen[id]; ok {
		it.state = stQueried
	}
}
func (s *shortlist) markFailed(id ID) {
	if it, ok := s.seen[id]; ok {
		it.state = stFailed
	}
}

func (s *shortlist) closest(k int) []Contact {
	s.sortItems()
	var out []Contact
	for _, it := range s.items {
		if it.state == stFailed {
			continue
		}
		out = append(out, it.c)
		if len(out) >= k {
			break
		}
	}
	return out
}
