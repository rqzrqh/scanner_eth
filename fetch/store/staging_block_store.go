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

type StagingSnapshot struct {
	Blocks         uint64 `json:"blocks"`
	PendingHeaders uint64 `json:"pending_headers"`
	PendingBodies  uint64 `json:"pending_bodies"`
	CompleteBlocks uint64 `json:"complete_blocks"`
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

func (s *StagingStore) Snapshot() StagingSnapshot {
	if s == nil {
		return StagingSnapshot{}
	}
	s.mu.RLock()
	defer s.mu.RUnlock()

	snapshot := StagingSnapshot{
		Blocks: uint64(len(s.blocks)),
	}
	for _, block := range s.blocks {
		if block == nil {
			continue
		}
		hasHeader := block.PendingHeader != nil
		hasBody := block.PendingBody != nil
		if hasHeader {
			snapshot.PendingHeaders++
		}
		if hasBody {
			snapshot.PendingBodies++
		}
		if hasHeader && hasBody {
			snapshot.CompleteBlocks++
		}
	}
	return snapshot
}
