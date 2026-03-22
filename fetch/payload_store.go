package fetch

import "sync"

// BlockPayload stores one block's header and body.
type BlockPayload struct {
	BlockHeader *BlockHeaderJson
	BlockBody   *EventBlockData
}

// BlockPayloadStore stores node attachments regardless of topology state.
type BlockPayloadStore struct {
	mu     sync.RWMutex
	blocks map[string]*BlockPayload
}

// NewBlockPayloadStore creates an empty payload store.
func NewBlockPayloadStore() *BlockPayloadStore {
	return &BlockPayloadStore{
		blocks: make(map[string]*BlockPayload),
	}
}

func (s *BlockPayloadStore) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.blocks = make(map[string]*BlockPayload)
	s.mu.Unlock()
}

func (s *BlockPayloadStore) SetBlockHeader(key string, header *BlockHeaderJson) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		blockPayload = &BlockPayload{}
		s.blocks[key] = blockPayload
	}
	blockPayload.BlockHeader = header
}

func (s *BlockPayloadStore) SetBlockBody(key string, body *EventBlockData) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		return
	}
	blockPayload.BlockBody = body
}

func (s *BlockPayloadStore) GetBlockHeader(key string) *BlockHeaderJson {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		return nil
	}
	return blockPayload.BlockHeader
}

func (s *BlockPayloadStore) GetBlockBody(key string) *EventBlockData {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		return nil
	}
	return blockPayload.BlockBody
}

func (s *BlockPayloadStore) DeleteBlockPayload(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blocks, key)
}
