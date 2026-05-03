package store

import (
	"scanner_eth/util"
	"sync"
)

type StoredBlockState struct {
	mu     sync.Mutex
	hashes map[string]struct{}
}

func NewStoredBlockState() StoredBlockState {
	return StoredBlockState{
		hashes: make(map[string]struct{}),
	}
}

func (s *StoredBlockState) Reset() {
	s.mu.Lock()
	s.hashes = make(map[string]struct{})
	s.mu.Unlock()
}

func (s *StoredBlockState) IsStored(hash string) bool {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	_, exists := s.hashes[hash]
	s.mu.Unlock()
	return exists
}

func (s *StoredBlockState) MarkStored(hash string) {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return
	}

	s.mu.Lock()
	if s.hashes == nil {
		s.hashes = make(map[string]struct{})
	}
	s.hashes[hash] = struct{}{}
	s.mu.Unlock()
}

func (s *StoredBlockState) UnmarkStored(hash string) {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return
	}

	s.mu.Lock()
	delete(s.hashes, hash)
	s.mu.Unlock()
}

func (s *StoredBlockState) Snapshot() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	snapshot := make(map[string]struct{}, len(s.hashes))
	for k := range s.hashes {
		snapshot[k] = struct{}{}
	}
	return snapshot
}
