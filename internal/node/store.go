package node

import (
	"os"
	"path/filepath"
	"sync"

	"p2pshare/internal/dht"
)

// Store 在磁盘上保存块，在内存中保存 Manifest 索引。
type Store struct {
	dir       string
	mu        sync.RWMutex
	manifests map[dht.ID]*Manifest
}

func NewStore(dir string) (*Store, error) {
	if err := os.MkdirAll(filepath.Join(dir, "chunks"), 0o777); err != nil {
		return nil, err
	}
	return &Store{dir: dir, manifests: make(map[dht.ID]*Manifest)}, nil
}

func (s *Store) chunkPath(id dht.ID) string {
	return filepath.Join(s.dir, "chunks", id.String())
}

func (s *Store) PutChunkID(id dht.ID, data []byte) error {
	return os.WriteFile(s.chunkPath(id), data, 0o666)
}

func (s *Store) GetChunk(id dht.ID) ([]byte, error) {
	return os.ReadFile(s.chunkPath(id))
}

func (s *Store) HasChunk(id dht.ID) bool {
	_, err := os.Stat(s.chunkPath(id))
	return err == nil
}

func (s *Store) AddManifest(m *Manifest) {
	s.mu.Lock()
	s.manifests[m.FileHash()] = m
	s.mu.Unlock()
}

func (s *Store) GetManifest(id dht.ID) (*Manifest, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m, ok := s.manifests[id]
	return m, ok
}

func (s *Store) Manifests() []*Manifest {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var out []*Manifest
	for _, m := range s.manifests {
		out = append(out, m)
	}
	return out
}
