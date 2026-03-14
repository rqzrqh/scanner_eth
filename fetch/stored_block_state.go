package fetch

import "sync"

type storedBlockState struct {
	mu     sync.Mutex
	hashes map[string]struct{}
}

func newStoredBlockState() storedBlockState {
	return storedBlockState{
		hashes: make(map[string]struct{}),
	}
}

func (s *storedBlockState) Reset() {
	s.mu.Lock()
	s.hashes = make(map[string]struct{})
	s.mu.Unlock()
}

func (s *storedBlockState) IsStored(hash string) bool {
	hash = normalizeHash(hash)
	if hash == "" {
		return false
	}

	s.mu.Lock()
	_, exists := s.hashes[hash]
	s.mu.Unlock()
	return exists
}

func (s *storedBlockState) MarkStored(hash string) {
	hash = normalizeHash(hash)
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

func (s *storedBlockState) UnmarkStored(hash string) {
	hash = normalizeHash(hash)
	if hash == "" {
		return
	}

	s.mu.Lock()
	delete(s.hashes, hash)
	s.mu.Unlock()
}
