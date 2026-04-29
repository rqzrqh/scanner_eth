package store

import (
	fetcherpkg "scanner_eth/fetch/fetcher"
	"sync"
)

// EventBlockData carries converted block payloads for storage and Redis export.
type EventBlockData struct {
	StorageFullBlock *StorageFullBlock
}

type Payload struct {
	Header *fetcherpkg.BlockHeaderJson
	Body   *EventBlockData
}

type PayloadStore struct {
	mu     sync.RWMutex
	blocks map[string]*Payload
}

func NewPayloadStore() *PayloadStore {
	return &PayloadStore{
		blocks: make(map[string]*Payload),
	}
}

func (s *PayloadStore) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.blocks = make(map[string]*Payload)
	s.mu.Unlock()
}

func (s *PayloadStore) SetHeader(key string, header *fetcherpkg.BlockHeaderJson) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		blockPayload = &Payload{}
		s.blocks[key] = blockPayload
	}
	blockPayload.Header = header
}

func (s *PayloadStore) SetBody(key string, body *EventBlockData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		return
	}
	blockPayload.Body = body
}

func (s *PayloadStore) GetHeader(key string) *fetcherpkg.BlockHeaderJson {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		return nil
	}
	return blockPayload.Header
}

func (s *PayloadStore) GetBody(key string) *EventBlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		return nil
	}
	return blockPayload.Body
}

func (s *PayloadStore) DeletePayload(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blocks, key)
}
