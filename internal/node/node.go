package node

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"p2pshare/internal/dht"
)

const (
	minChunkSize      = 1 << 14 // 16 KiB
	maxChunkSize      = 1 << 20 // 1 MiB
	concurrency       = 10
	republishInterval = 30 * time.Second
)

type ProgressFunc func(done, total int)

// Node combines Kademlia DHT with file storage/transfer.
type Node struct {
	kad     *dht.Kademlia
	store   *Store
	dataDir string
}

// Create a node and start the DHT network.
func StartNode(ctx context.Context, listenAddr, dataDir string) (*Node, error) {
	store, err := NewStore(dataDir)
	if err != nil {
		return nil, err
	}
	kad, err := dht.StartKademlia(ctx, listenAddr, dataDir, store.GetChunk)
	if err != nil {
		return nil, err
	}
	node := &Node{kad: kad, store: store, dataDir: dataDir}
	node.startRepublish(ctx, republishInterval)

	return node, nil
}

func (n *Node) MyID() dht.ID                    { return n.kad.MyID() }
func (n *Node) Peers() []dht.Contact            { return n.kad.Peers() }
func (n *Node) Manifests() map[dht.ID]*Manifest { return n.store.Manifests() }
func (n *Node) Bootstrap(ctx context.Context, contacts []dht.Contact) int {
	return n.kad.Bootstrap(ctx, contacts)
}

func (n *Node) Publish(path string, progress ProgressFunc) (dht.ID, *Manifest, error) {
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

	chunkSize := min(max(fi.Size()/10, minChunkSize), maxChunkSize)
	total := int((fi.Size() + chunkSize - 1) / chunkSize)

	var chunks []dht.ID
	buf := make([]byte, chunkSize)
	for {
		nr, rerr := io.ReadFull(f, buf)
		if nr > 0 {
			data := make([]byte, nr)
			copy(data, buf[:nr])
			id := dht.ChunkID(data)
			if err := n.store.PutChunk(id, data); err != nil {
				return dht.ID{}, nil, err
			}
			chunks = append(chunks, id)
			go n.kad.Announce(id)
			if progress != nil {
				progress(len(chunks), total)
			}
		}
		if rerr == io.EOF || rerr == io.ErrUnexpectedEOF {
			break
		}
		if rerr != nil {
			return dht.ID{}, nil, rerr
		}
	}

	manifest := &Manifest{
		Name:      filepath.Base(path),
		Size:      fi.Size(),
		ChunkSize: chunkSize,
		Chunks:    chunks,
	}
	mb, err := json.Marshal(manifest)
	if err != nil {
		return dht.ID{}, nil, err
	}
	fid := dht.ChunkID(mb)
	if err := n.store.PutChunk(fid, mb); err != nil {
		return dht.ID{}, nil, err
	}
	go n.kad.Announce(fid)
	if err := n.store.AddManifest(fid, manifest); err != nil {
		return dht.ID{}, nil, err
	}
	return fid, manifest, nil
}

func (n *Node) SaveUpload(name string, data []byte) (path string, cleanup func(), err error) {
	name = filepath.Base(filepath.Clean(name))
	if name == "" || name == "." || name == string(filepath.Separator) {
		return "", nil, fmt.Errorf("invalid file name")
	}

	dir := filepath.Join(n.dataDir, "uploads", newUploadID())
	if err := os.MkdirAll(dir, 0o777); err != nil {
		return "", nil, err
	}
	cleanup = func() { _ = os.RemoveAll(dir) }

	path = filepath.Join(dir, name)
	if err := os.WriteFile(path, data, 0o666); err != nil {
		cleanup()
		return "", nil, err
	}
	return path, cleanup, nil
}

func newUploadID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

func (n *Node) getChunk(ctx context.Context, id dht.ID) ([]byte, error) {
	data, err := n.store.GetChunk(id)
	if err != nil {
		if data, err = n.kad.FindValue(ctx, id); err != nil {
			return nil, err
		}
		// store the downloaded chunck and announce it
		if err := n.store.PutChunk(id, data); err != nil {
			return nil, err
		}
		go n.kad.Announce(id)
	}
	return data, nil
}

func (n *Node) getManifest(ctx context.Context, id dht.ID) (*Manifest, error) {
	if m, ok := n.store.GetManifest(id); ok {
		return m, nil
	}
	mb, err := n.getChunk(ctx, id)
	if err != nil {
		return nil, err
	}
	var manifest Manifest
	if err := json.Unmarshal(mb, &manifest); err != nil {
		return nil, fmt.Errorf("%s is not a valid file ID", id.String())
	}
	return &manifest, nil
}

func (n *Node) Download(ctx context.Context, fileID dht.ID, outdir string, progress ProgressFunc) (string, error) {
	// get the manifest
	m, err := n.getManifest(ctx, fileID)
	if err != nil {
		return "", err
	}

	// get chunks
	total := len(m.Chunks)
	done := 0
	pool := make(chan struct{}, concurrency)
	result := make(chan error, len(m.Chunks))
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		for _, cid := range m.Chunks {
			select {
			case pool <- struct{}{}:
			case <-cctx.Done():
				return
			}

			go func(id dht.ID) {
				defer func() { <-pool }()
				// We do not need the data of the return value,
				// because the chunk is saved to disk by getChunk.
				// We assemble the file after all chunks are downloaded.
				_, err := n.getChunk(cctx, id)
				result <- err
			}(cid)
		}
	}()

	for range m.Chunks {
		select {
		case <-ctx.Done():
			return "", ctx.Err()
		case err := <-result:
			if err != nil {
				return "", err
			}
			done++
			if progress != nil {
				progress(done, total)
			}
		}
	}
	if err := n.store.AddManifest(fileID, m); err != nil {
		return "", err
	}

	// assemble the file
	outdir = filepath.Clean(outdir)
	if err := os.MkdirAll(outdir, 0o777); err != nil {
		return "", err
	}
	tempName := m.Name + ".temp"
	f, err := os.Create(filepath.Join(outdir, tempName))
	if err != nil {
		return "", err
	}
	defer f.Close()
	if err := f.Truncate(m.Size); err != nil {
		return "", err
	}
	for _, cid := range m.Chunks {
		data, err := n.store.GetChunk(cid)
		if err != nil {
			return "", err
		}
		if _, err := f.Write(data); err != nil {
			return "", err
		}
	}
	// Windows does not allow to rename an opened file
	if err := f.Close(); err != nil {
		return "", err
	}
	if err := os.Rename(filepath.Join(outdir, tempName), filepath.Join(outdir, m.Name)); err != nil {
		return "", err
	}

	return m.Name, nil
}

// startRepublish periodically publishes all local chunks to the DHT.
// interval should be significantly smaller than providerTTL (30m)
func (n *Node) startRepublish(ctx context.Context, interval time.Duration) {
	n.republish()
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
	for _, cid := range n.store.Chunks() {
		go n.kad.Announce(cid)
	}
}
