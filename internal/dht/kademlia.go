package dht

import (
	"context"
	"fmt"
	"maps"
	"net"
	"slices"
	"sort"
	"sync"
	"time"
)

const (
	k           = 20 // bucket size / redundant replica count
	alpha       = 3  // lookup concurrency
	rpcTimeout  = 5 * time.Second
	valueTTL    = time.Hour
	providerTTL = 30 * time.Minute
)

type valueEntry struct {
	data []byte
	exp  time.Time
}

type GetChunk func(id ID) ([]byte, error)
type ValueSource func(key ID) ([]byte, bool)

type Kademlia struct {
	t  *transport
	rt *routingTable

	mu        sync.Mutex
	values    map[ID]valueEntry
	providers map[ID]map[Contact]time.Time

	getChunk   GetChunk
	localValue ValueSource
}

func (kad *Kademlia) SetChunkHandler(handler GetChunk) { kad.getChunk = handler }

func (kad *Kademlia) SetValueSource(f ValueSource) { kad.localValue = f }

// Check the DHT storage first, and falls back to the injected local value source on a miss.
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
		providers: make(map[ID]map[Contact]time.Time),
	}
	t.setHandler(kad.HandleRPC)
	return kad, nil
}

func (kad *Kademlia) MyID() ID { return kad.t.myID() }

func (kad *Kademlia) Peers() []Contact { return kad.rt.allContacts() }

func (kad *Kademlia) SendRPC(ctx context.Context, c Contact, m *Message) (*Message, error) {
	return kad.t.send(ctx, c, m)
}

// ---------- Server: handle received RPC ----------

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
		if v, ok := kad.getValueOrLocal(msg.Key); ok {
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

// ---------- Local Storage ----------

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
	defer kad.mu.Unlock()
	m, ok := kad.providers[k]
	if !ok {
		m = make(map[Contact]time.Time)
		kad.providers[k] = m
	}
	m[c] = time.Now().Add(providerTTL)
}

func (kad *Kademlia) localProviders(k ID) []Contact {
	kad.mu.Lock()
	defer kad.mu.Unlock()
	m, ok := kad.providers[k]
	if !ok {
		return nil
	}
	var out []Contact
	now := time.Now()
	for c, exp := range m {
		if now.After(exp) {
			delete(m, c)
			continue
		}
		out = append(out, c)
	}
	return out
}

// ---------- Iterative Lookup (Kademlia Core Algorithm) ----------

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
	provs := make(map[Contact]struct{})

	for {
		batch := sl.selectAlpha(alpha)
		if len(batch) == 0 {
			break // Convergence, closest K nodes have all been queried
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
					// Addr == "" indicates the requested node itself provides this file
					if p.Addr == "" {
						p.Addr = r.from.Addr
					}
					provs[p] = struct{}{}
				}
			}
			sl.push(r.msg.Contacts)
		}
	}

	out := lookupOutcome{closest: sl.closest(k)}
	out.providers = slices.Collect(maps.Keys(provs))
	return out
}

// ---------- External DHT Operations ----------

func (kad *Kademlia) Bootstrap(ctx context.Context, contacts []Contact) error {
	for _, c := range contacts {
		cctx, cancel := context.WithTimeout(ctx, rpcTimeout)
		resp, err := kad.t.send(cctx, c, &Message{Type: TypePing})
		cancel()
		if err == nil && resp != nil {
			kad.rt.update(c)
		}
	}
	kad.lookup(kad.MyID(), modeFindNode) // Self-lookup to populate the routing table
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
	if v, ok := kad.getValueOrLocal(key); ok {
		return v, true
	}
	out := kad.lookup(key, modeFindValue)
	return out.value, out.found
}

func (kad *Kademlia) Announce(key ID) int {
	// Addr: "" indicates the node itself provides this file
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
	res := make(map[Contact]struct{})
	for _, c := range kad.localProviders(key) {
		res[c] = struct{}{}
	}
	out := kad.lookup(key, modeProviders)
	for _, c := range out.providers {
		res[c] = struct{}{}
	}
	return slices.Collect(maps.Keys(res))
}

// ---------- shortlist: Candidate Set for Iterative Lookup ----------

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

// Pick up to a pending query nodes within the "closest K non-failed nodes" window.
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
