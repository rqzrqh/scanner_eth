package store

import (
	fetcherpkg "scanner_eth/fetch/fetcher"
	"sync"
)

// EventBlockData carries converted block data for storage and Redis export.
type EventBlockData struct {
	StorageFullBlock *StorageFullBlock
}

type PendingBlock struct {
	Header *fetcherpkg.BlockHeaderJson
	Body   *EventBlockData
}

type PendingBlockStore struct {
	mu     sync.RWMutex
	blocks map[string]*PendingBlock
}

func NewPendingBlockStore() *PendingBlockStore {
	return &PendingBlockStore{
		blocks: make(map[string]*PendingBlock),
	}
}

func (s *PendingBlockStore) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.blocks = make(map[string]*PendingBlock)
	s.mu.Unlock()
}

func (s *PendingBlockStore) SetHeader(key string, header *fetcherpkg.BlockHeaderJson) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingBlock, ok := s.blocks[key]
	if !ok || pendingBlock == nil {
		pendingBlock = &PendingBlock{}
		s.blocks[key] = pendingBlock
	}
	pendingBlock.Header = header
}

func (s *PendingBlockStore) SetBody(key string, body *EventBlockData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pendingBlock, ok := s.blocks[key]
	if !ok || pendingBlock == nil {
		return
	}
	pendingBlock.Body = body
}

func (s *PendingBlockStore) GetHeader(key string) *fetcherpkg.BlockHeaderJson {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pendingBlock := s.blocks[key]
	if pendingBlock == nil {
		return nil
	}
	return pendingBlock.Header
}

func (s *PendingBlockStore) GetBody(key string) *EventBlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	pendingBlock := s.blocks[key]
	if pendingBlock == nil {
		return nil
	}
	return pendingBlock.Body
}

func (s *PendingBlockStore) DeleteBlock(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blocks, key)
}
