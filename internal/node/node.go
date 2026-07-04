package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"time"

	"p2pshare/internal/dht"
)

const defaultChunkSize = 256 * 1024

// Node 把 Kademlia DHT 与文件存储/传输组合起来。
type Node struct {
	kad   *dht.Kademlia
	t     *dht.Transport
	store *Store
	myid  dht.ID
}

// New 创建节点
// 单机测试请使用形如 127.0.0.1:9000 的具体地址。
func New(listenAddr, dataDir string) (*Node, error) {
	certDir := filepath.Join(dataDir, "identity")
	t, err := dht.NewTransport(listenAddr, certDir)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(dataDir)
	if err != nil {
		return nil, err
	}
	myid := t.NodeID()
	kad := dht.NewKademlia(myid, t)

	// 新增：FIND_VALUE 未命中 DHT 缓存时，从本地文件库返回 manifest。
	// 这样"持有文件的节点"都能应答清单，而不只是离 fileID 最近的 K 个节点。
	kad.SetValueSource(func(key dht.ID) ([]byte, bool) {
		if m, ok := store.GetManifest(key); ok {
			if b, err := json.Marshal(m); err == nil {
				return b, true
			}
		}
		return nil, false
	})

	n := &Node{kad: kad, t: t, store: store, myid: myid}
	t.SetHandler(n.handle)
	return n, nil
}

// handle 区分文件层 RPC（GET_CHUNK）与 DHT RPC。
func (n *Node) handle(remote net.Addr, msg *dht.Message) *dht.Message {
	if msg.Type == dht.TypeGetChunk {
		data, err := n.store.GetChunk(msg.Key)
		if err != nil {
			return &dht.Message{Type: dht.TypeGetChunk, Sender: n.myid, Error: "chunk not found"}
		}
		return &dht.Message{Type: dht.TypeGetChunk, Sender: n.myid, Key: msg.Key, Value: data, Found: true}
	}
	return n.kad.HandleRPC(remote, msg)
}

func (n *Node) Start(ctx context.Context)                     { go n.t.Serve(ctx) }
func (n *Node) Bootstrap(ctx context.Context, addrs []string) { _ = n.kad.Bootstrap(ctx, addrs) }
func (n *Node) Myid() dht.ID                                  { return n.myid }
func (n *Node) Peers() []dht.Contact                          { return n.kad.Peers() }
func (n *Node) Manifests() []*Manifest                        { return n.store.Manifests() }

// Publish 切分文件、入库、把 Manifest 存入 DHT 并宣告 provider。
func (n *Node) Publish(path string) (dht.ID, *Manifest, error) {
	f, err := os.Open(path)
	if err != nil {
		return dht.ID{}, nil, err
	}
	defer f.Close()
	fi, err := f.Stat()
	if err != nil {
		return dht.ID{}, nil, err
	}
	if !fi.Mode().IsRegular() {
		return dht.ID{}, nil, fmt.Errorf("%s is not a regular file", path)
	}

	var chunks []dht.ID
	buf := make([]byte, defaultChunkSize)
	for {
		nr, rerr := io.ReadFull(f, buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])
			id := ChunkID(data)
			if err := n.store.PutChunk(id, data); err != nil {
				return dht.ID{}, nil, err
			}
			chunks = append(chunks, id)
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return dht.ID{}, nil, rerr
		}
	}

	m := &Manifest{Name: filepath.Base(path), Size: fi.Size(), ChunkSize: defaultChunkSize, Chunks: chunks}
	fh := m.FileID()
	n.store.AddManifest(m)

	mb, _ := json.Marshal(m)
	n.kad.StoreValue(fh, mb)
	n.kad.Announce(fh)
	return fh, m, nil
}

// Download 根据 fileID 还原文件到 outdir。
func (n *Node) Download(ctx context.Context, fileID dht.ID, outdir string) (string, error) {
	var m Manifest
	if mm, ok := n.store.GetManifest(fileID); ok {
		m = *mm
	} else {
		data, ok := n.kad.FindValue(fileID)
		if !ok {
			return "", errors.New("manifest not found in DHT")
		}
		if err := json.Unmarshal(data, &m); err != nil {
			return "", err
		}
		n.store.AddManifest(&m)
	}

	providers := n.kad.FindProviders(fileID)
	if len(providers) == 0 {
		return "", errors.New("no providers found for this file")
	}
	rand.Shuffle(len(providers), func(i, j int) { providers[i], providers[j] = providers[j], providers[i] })

	if err := os.MkdirAll(outdir, 0o777); err != nil {
		return "", err
	}
	f, err := os.Create(filepath.Join(outdir, m.Name))
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := f.Truncate(m.Size); err != nil {
		return "", err
	}

	for i, cid := range m.Chunks {
		offset := int64(i) * int64(m.ChunkSize)
		if n.store.HasChunk(cid) {
			data, _ := n.store.GetChunk(cid)
			if _, err := f.WriteAt(data, offset); err != nil {
				return "", err
			}
			continue
		}
		var got []byte
		for _, p := range providers {
			cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
			resp, rerr := n.t.Send(cctx, p.Addr, &dht.Message{Type: dht.TypeGetChunk, Key: cid, Sender: n.myid})
			cancel()
			if rerr != nil || resp == nil || resp.Error != "" || !resp.Found {
				continue
			}
			if ChunkID(resp.Value) != cid { // 完整性校验
				continue
			}
			got = resp.Value
			break
		}
		if got == nil {
			return "", fmt.Errorf("failed to fetch chunk %d/%d", i+1, len(m.Chunks))
		}
		if err := n.store.PutChunk(cid, got); err != nil {
			return "", err
		}
		if _, err := f.WriteAt(got, offset); err != nil {
			return "", err
		}
	}

	mb, _ := json.Marshal(&m)
	n.kad.StoreValue(fileID, mb) // 新增：主动承担清单的再分发
	n.kad.Announce(fileID)       // 原有：宣告自己成为 provider
	return m.Name, nil
}

// StartRepublish 周期性地把本地所有文件的清单与 provider 记录重新发布到 DHT。
// interval 应明显小于 valueTTL(1h) 与 providerTTL(30m)，建议 15 分钟。
func (n *Node) StartRepublish(ctx context.Context, interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				n.republish()
			}
		}
	}()
}

func (n *Node) republish() {
	for _, m := range n.store.Manifests() {
		fh := m.FileID()
		mb, _ := json.Marshal(&m)
		n.kad.StoreValue(fh, mb)
		n.kad.Announce(fh)
	}
}
