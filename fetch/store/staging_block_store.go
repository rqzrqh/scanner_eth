package store

import (
	fetcherpkg "scanner_eth/fetch/fetcher"
	"sync"
)

// EventBlockData carries converted block data for storage and Redis export.
type EventBlockData struct {
	StorageFullBlock *StorageFullBlock
}

type StagingBlock struct {
	PendingHeader *fetcherpkg.BlockHeaderJson
	PendingBody   *EventBlockData
}

type StagingStore struct {
	mu     sync.RWMutex
	blocks map[string]*StagingBlock
}

func NewStagingStore() *StagingStore {
	return &StagingStore{
		blocks: make(map[string]*StagingBlock),
	}
}

func (s *StagingStore) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.blocks = make(map[string]*StagingBlock)
	s.mu.Unlock()
}

func (s *StagingStore) SetPendingHeader(key string, header *fetcherpkg.BlockHeaderJson) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingBlock, ok := s.blocks[key]
	if !ok || pendingBlock == nil {
		pendingBlock = &StagingBlock{}
		s.blocks[key] = pendingBlock
	}
	pendingBlock.PendingHeader = header
}

func (s *StagingStore) SetPendingBody(key string, body *EventBlockData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingBlock, ok := s.blocks[key]
	if !ok || pendingBlock == nil {
		return
	}
	pendingBlock.PendingBody = body
}

func (s *StagingStore) GetPendingHeader(key string) *fetcherpkg.BlockHeaderJson {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pendingBlock := s.blocks[key]
	if pendingBlock == nil {
		return nil
	}
	return pendingBlock.PendingHeader
}

func (s *StagingStore) GetPendingBody(key string) *EventBlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pendingBlock := s.blocks[key]
	if pendingBlock == nil {
		return nil
	}
	return pendingBlock.PendingBody
}

func (s *StagingStore) DeleteBlock(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blocks, key)
}
