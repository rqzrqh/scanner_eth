package store

import (
	"strings"
	"sync"
)

type StoredState interface {
	IsStored(string) bool
	MarkStored(string)
}

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
	hash = normalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	_, exists := s.hashes[hash]
	s.mu.Unlock()
	return exists
}

func (s *InflightState) TryStart(hash string, storedState StoredState) bool {
	hash = normalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.hashes == nil {
		s.hashes = make(map[string]struct{})
	}
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
	hash = normalizeHash(hash)
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

func normalizeHash(hash string) string {
	if hash == "" {
		return ""
	}
	return strings.ToLower(hash)
}
