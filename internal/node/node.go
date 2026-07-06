package node

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"p2pshare/internal/dht"
)

const defaultChunkSize = 256 * 1024

// Node 把 Kademlia DHT 与文件存储/传输组合起来。
type Node struct {
	kad   *dht.Kademlia
	store *Store
}

// StartNode 创建节点
// 单机测试请使用形如 127.0.0.1:9000 的具体地址。
func StartNode(listenAddr, dataDir string, ctx context.Context) (*Node, error) {
	store, err := NewStore(dataDir)
	if err != nil {
		return nil, err
	}
	certDir := filepath.Join(dataDir, "identity")
	kad, err := dht.StartKademlia(listenAddr, certDir, ctx)
	if err != nil {
		return nil, err
	}
	kad.SetChunkHandler(store.GetChunk)

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

	n := &Node{kad: kad, store: store}
	return n, nil
}

func (n *Node) MyID() dht.ID           { return n.kad.MyID() }
func (n *Node) Peers() []dht.Contact   { return n.kad.Peers() }
func (n *Node) Manifests() []*Manifest { return n.store.Manifests() }
func (n *Node) Bootstrap(ctx context.Context, contacts []dht.Contact) error {
	return n.kad.Bootstrap(ctx, contacts)
}

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
	}

	providers := n.kad.FindProviders(fileID)
	if len(providers) == 0 {
		return "", errors.New("no providers found for this file")
	}
	rand.Shuffle(len(providers), func(i, j int) { providers[i], providers[j] = providers[j], providers[i] })

	outdir = filepath.Clean(outdir)
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
			value := []byte{}
			if p.ID == n.kad.MyID() {
				data, err := n.store.GetChunk(cid)
				if err != nil {
					continue
				}
				value = data
			} else {
				cctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				resp, rerr := n.kad.SendRPC(cctx, p, &dht.Message{Type: dht.TypeGetChunk, Key: cid})
				cancel()
				if rerr != nil || resp == nil || resp.Error != "" {
					continue
				}
				value = resp.Value
			}
			if ChunkID(value) != cid { // 完整性校验
				continue
			}
			got = value
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

	n.store.AddManifest(&m)
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
