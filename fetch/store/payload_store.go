package store

import "sync"

type Payload[H any, B any] struct {
	Header H
	Body   B
}

type PayloadStore[H any, B any] struct {
	mu     sync.RWMutex
	blocks map[string]*Payload[H, B]
}

func NewPayloadStore[H any, B any]() *PayloadStore[H, B] {
	return &PayloadStore[H, B]{
		blocks: make(map[string]*Payload[H, B]),
	}
}

func (s *PayloadStore[H, B]) Reset() {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.blocks = make(map[string]*Payload[H, B])
	s.mu.Unlock()
}

func (s *PayloadStore[H, B]) SetHeader(key string, header H) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		blockPayload = &Payload[H, B]{}
		s.blocks[key] = blockPayload
	}
	blockPayload.Header = header
}

func (s *PayloadStore[H, B]) SetBody(key string, body B) {
	s.mu.Lock()
	defer s.mu.Unlock()

	blockPayload, ok := s.blocks[key]
	if !ok || blockPayload == nil {
		return
	}
	blockPayload.Body = body
}

func (s *PayloadStore[H, B]) GetHeader(key string) H {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		var zero H
		return zero
	}
	return blockPayload.Header
}

func (s *PayloadStore[H, B]) GetBody(key string) B {
	s.mu.RLock()
	defer s.mu.RUnlock()

	blockPayload := s.blocks[key]
	if blockPayload == nil {
		var zero B
		return zero
	}
	return blockPayload.Body
}

func (s *PayloadStore[H, B]) DeletePayload(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.blocks, key)
}
