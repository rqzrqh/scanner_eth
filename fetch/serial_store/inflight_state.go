package serialstore

import (
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/util"
	"sync"
)

type InflightState struct {
	mu     sync.Mutex
	hashes map[string]struct{}
}

func NewInflightState() InflightState {
	return InflightState{
		hashes: make(map[string]struct{}),
	}
}

func (s *InflightState) Has(hash string) bool {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	_, exists := s.hashes[hash]
	s.mu.Unlock()
	return exists
}

func (s *InflightState) TryStart(hash string, storedState *fetchstore.StoredBlockState) bool {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.hashes[hash]; exists {
		return false
	}
	if storedState != nil && storedState.IsStored(hash) {
		return false
	}
	s.hashes[hash] = struct{}{}
	return true
}

func (s *InflightState) Finish(hash string) {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return
	}

	s.mu.Lock()
	delete(s.hashes, hash)
	s.mu.Unlock()
}

func (s *InflightState) Count() int {
	s.mu.Lock()
	n := len(s.hashes)
	s.mu.Unlock()
	return n
}
