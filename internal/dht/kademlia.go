package dht

import (
	"context"
	"fmt"
	"maps"
	"math/rand"
	"net"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const (
	k           = 20 // bucket size / redundant replica count
	providerTTL = 30 * time.Minute
	concurrency = 10
)

type GetValue func(id ID) ([]byte, error)

type Kademlia struct {
	t   *transport
	rt  *routingTable
	ctx context.Context

	mu        sync.Mutex
	providers map[ID]map[Contact]time.Time

	getValue GetValue
}

func StartKademlia(ctx context.Context, listenAddr, dataDir string, valueHandler GetValue) (*Kademlia, error) {
	t, err := startTransport(ctx, listenAddr, dataDir)
	if err != nil {
		return nil, err
	}
	kad := &Kademlia{
		t:         t,
		rt:        newRoutingTable(ctx, t, k),
		ctx:       ctx,
		getValue:  valueHandler,
		providers: make(map[ID]map[Contact]time.Time),
	}
	t.setHandler(kad.HandleRPC)
	return kad, nil
}

func (kad *Kademlia) MyID() ID { return kad.t.myID() }

func (kad *Kademlia) Peers() []Contact { return kad.rt.allContacts() }

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
	case TypeAddProvider:
		kad.addProvider(msg.Key, contact)
	case TypeGetProviders:
		resp.Providers = kad.localProviders(msg.Key)
		resp.Contacts = kad.rt.closest(msg.Key, k)
	case TypeGetValue:
		data, err := kad.getValue(msg.Key)
		if err == nil {
			resp.Value = data
		} else {
			resp.Error = "value not found"
		}
	default:
		resp.Error = "unknown rpc"
	}
	return resp
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
	modeGetProviders
)

func typeForMode(m lookupMode) string {
	switch m {
	case modeGetProviders:
		return TypeGetProviders
	case modeFindNode:
		return TypeFindNode
	default:
		panic(fmt.Sprintf("unexpected lookupMode: %d", m))
	}
}

type lookupOutcome struct {
	closest   []Contact
	providers []Contact
}

func (kad *Kademlia) lookup(ctx context.Context, target ID, mode lookupMode) lookupOutcome {
	sl := newShortlist(kad.MyID(), target)
	sl.push(kad.rt.closest(target, k))
	provs := make(map[Contact]struct{})

	for {
		batch := sl.pickNodes()
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
			sl.setInflight(c)
			go func(arg Contact) {
				resp, err := kad.t.send(ctx, arg, &Message{Type: typeForMode(mode), Key: target})
				ch <- result{arg, resp, err}
			}(c)
		}
		for range batch {
			r := <-ch
			if r.err != nil || r.msg == nil {
				sl.markFailed(r.from)
				continue
			}
			sl.markQueried(r.from)
			kad.rt.update(r.from)
			if mode == modeGetProviders {
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

func (kad *Kademlia) Bootstrap(ctx context.Context, contacts []Contact) int {
	var n atomic.Int32
	n.Store(0)
	var wg sync.WaitGroup
	for _, c := range contacts {
		wg.Add(1)
		go func(arg Contact) {
			defer wg.Done()
			resp, err := kad.t.send(ctx, arg, &Message{Type: TypePing})
			if err == nil && resp != nil {
				n.Add(1)
				kad.rt.update(arg)
			}
		}(c)
	}
	wg.Wait()
	kad.lookup(ctx, kad.MyID(), modeFindNode) // Self-lookup to populate the routing table
	return int(n.Load())
}

func (kad *Kademlia) Announce(key ID) int {
	// Addr: "" indicates the node itself provides this file
	kad.addProvider(key, Contact{ID: kad.MyID(), Addr: ""})
	out := kad.lookup(kad.ctx, key, modeFindNode)
	var n atomic.Int32
	n.Store(0)
	var wg sync.WaitGroup
	for _, c := range out.closest {
		wg.Add(1)
		go func(arg Contact) {
			defer wg.Done()
			_, err := kad.t.send(kad.ctx, arg, &Message{Type: TypeAddProvider, Key: key})
			if err == nil {
				n.Add(1)
			}
		}(c)
	}
	wg.Wait()
	return int(n.Load())
}

func (kad *Kademlia) findProviders(ctx context.Context, key ID) []Contact {
	res := make(map[Contact]struct{})
	for _, c := range kad.localProviders(key) {
		res[c] = struct{}{}
	}
	out := kad.lookup(ctx, key, modeGetProviders)
	for _, c := range out.providers {
		res[c] = struct{}{}
	}
	for key := range res {
		if key.ID == kad.MyID() {
			delete(res, key)
		}
	}
	return slices.Collect(maps.Keys(res))
}

func (kad *Kademlia) FindValue(ctx context.Context, key ID) ([]byte, error) {
	providers := kad.findProviders(ctx, key)
	rand.Shuffle(len(providers), func(i, j int) {
		providers[i], providers[j] = providers[j], providers[i]
	})

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	const connects = 3
	pool := make(chan struct{}, connects)
	result := make(chan []byte, len(providers))

	go func() {
		for _, p := range providers {
			select {
			case pool <- struct{}{}:
			case <-cctx.Done():
				return
			}

			go func(c Contact) {
				defer func() { <-pool }()
				resp, err := kad.t.send(cctx, c, &Message{Type: TypeGetValue, Key: key})
				var data []byte
				// check hash
				if err == nil && resp.Value != nil && ChunkID(resp.Value) == key {
					data = resp.Value
				} else {
					data = nil
				}
				result <- data
			}(p)
		}
	}()

	for range providers {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case data := <-result:
			if data != nil {
				return data, nil
			}
		}
	}
	return nil, fmt.Errorf("%s is not found in DHT", key.String())
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
	myid   ID
	target ID
	items  []*slItem
	seen   map[Contact]*slItem
}

func newShortlist(myid, target ID) *shortlist {
	return &shortlist{myid: myid, target: target, seen: make(map[Contact]*slItem)}
}

func (s *shortlist) push(cs []Contact) {
	for _, c := range cs {
		if c.ID == s.myid || c.Addr == "" || c.ID.isZero() {
			continue
		}
		if _, ok := s.seen[c]; ok {
			continue
		}
		it := &slItem{c: c, dist: s.target.xor(c.ID), state: stPending}
		s.seen[c] = it
		s.items = append(s.items, it)
	}
}

func (s *shortlist) sortItems() {
	sort.Slice(s.items, func(i, j int) bool { return s.items[i].dist.less(s.items[j].dist) })
}

// Pick up to a pending query nodes within the "closest K non-failed nodes" window.
func (s *shortlist) pickNodes() []Contact {
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
			if len(out) >= concurrency {
				break
			}
		}
	}
	return out
}

func (s *shortlist) setInflight(c Contact) {
	if it, ok := s.seen[c]; ok {
		it.state = stInflight
	}
}
func (s *shortlist) markQueried(c Contact) {
	if it, ok := s.seen[c]; ok {
		it.state = stQueried
	}
}
func (s *shortlist) markFailed(c Contact) {
	if it, ok := s.seen[c]; ok {
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
