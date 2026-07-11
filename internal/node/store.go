package node

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"p2pshare/internal/dht"
)

// Manifest describes how a file consists of content-addressable chunks.
type Manifest struct {
	Name      string   `json:"name"`
	Size      int64    `json:"size"`
	ChunkSize int64    `json:"chunk_size"`
	Chunks    []dht.ID `json:"chunks"`
}

// Store saves chunks on disk and maintains the Manifest index in memory.
type Store struct {
	maniPath  string
	chunkDir  string
	manifests map[dht.ID]*Manifest
	ml        sync.RWMutex
	chunks    map[dht.ID]struct{}
	cl        sync.RWMutex
}

func NewStore(dir string) (*Store, error) {
	s := &Store{
		maniPath:  filepath.Join(dir, "manifests.json"),
		chunkDir:  filepath.Join(dir, "chunks"),
		manifests: make(map[dht.ID]*Manifest),
		chunks:    make(map[dht.ID]struct{}),
	}
	// check and load chunks
	if err := os.MkdirAll(s.chunkDir, 0o777); err != nil {
		return nil, err
	}
	files, err := os.ReadDir(s.chunkDir)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		fpath := filepath.Join(s.chunkDir, f.Name())
		data, err := os.ReadFile(fpath)
		if err != nil {
			return nil, err
		}
		// check chunk hash
		id := dht.ChunkID(data)
		if id.String() != f.Name() {
			return nil, fmt.Errorf("chunk %s is corrupted", f.Name())
		}
		// load chunck id to memory
		s.chunks[id] = struct{}{}
	}

	// check and load manifests
	mlb, err := os.ReadFile(s.maniPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if err == nil {
		var maniList []dht.ID
		if err := json.Unmarshal(mlb, &maniList); err != nil {
			return nil, err
		}
	maniListLoop:
		for _, fid := range maniList {
			// check if the manifest exists
			mb, err := s.GetChunk(fid)
			if err != nil {
				continue
			}
			// check if it is a valid manifest
			var mani Manifest
			if err := json.Unmarshal(mb, &mani); err != nil {
				continue
			}
			// check if we have all chucks of this file
			for _, cid := range mani.Chunks {
				_, ok := s.chunks[cid]
				if !ok {
					continue maniListLoop
				}
			}
			// load manifest to memory
			s.manifests[fid] = &mani
		}
	}
	// write the manifests back to disk
	if err := s.syncManifests(); err != nil {
		return nil, err
	}

	return s, nil
}

func (s *Store) PutChunk(id dht.ID, data []byte) error {
	s.cl.RLock()
	_, ok := s.chunks[id]
	s.cl.RUnlock()
	if ok {
		return nil
	}
	if err := os.WriteFile(filepath.Join(s.chunkDir, id.String()), data, 0o666); err != nil {
		return err
	}
	s.cl.Lock()
	s.chunks[id] = struct{}{}
	s.cl.Unlock()
	return nil
}

func (s *Store) GetChunk(id dht.ID) ([]byte, error) {
	s.cl.RLock()
	_, ok := s.chunks[id]
	s.cl.RUnlock()
	if !ok {
		return nil, fmt.Errorf("chunk %s is not found", id.String())
	}
	return os.ReadFile(filepath.Join(s.chunkDir, id.String()))
}

func (s *Store) AddManifest(fid dht.ID, m *Manifest) error {
	s.ml.RLock()
	_, ok := s.manifests[fid]
	s.ml.RUnlock()
	if ok {
		return nil
	}
	s.cl.RLock()
	valid := true
	if _, ok = s.chunks[fid]; !ok {
		valid = false
	} else {
		for _, cid := range m.Chunks {
			if _, ok := s.chunks[cid]; !ok {
				valid = false
				break
			}
		}
	}
	s.cl.RUnlock()
	if !valid {
		return fmt.Errorf("not all chunks of file %s are on the disk", fid.String())
	}
	s.ml.Lock()
	s.manifests[fid] = m
	s.ml.Unlock()
	go s.syncManifests()
	return nil
}

func (s *Store) GetManifest(id dht.ID) (*Manifest, bool) {
	s.ml.RLock()
	m, ok := s.manifests[id]
	s.ml.RUnlock()
	return m, ok
}

func (s *Store) Manifests() map[dht.ID]*Manifest {
	s.ml.RLock()
	clone := maps.Clone(s.manifests)
	s.ml.RUnlock()
	return clone
}

func (s *Store) Chunks() []dht.ID {
	s.cl.RLock()
	collect := slices.Collect(maps.Keys(s.chunks))
	s.cl.RUnlock()
	return collect
}

func (s *Store) syncManifests() error {
	s.ml.RLock()
	maniList := slices.Collect(maps.Keys(s.manifests))
	s.ml.RUnlock()
	if maniList == nil {
		maniList = []dht.ID{}
	}
	mlb, err := json.Marshal(maniList)
	if err != nil {
		return err
	}
	return os.WriteFile(s.maniPath, mlb, 0o666)
}
