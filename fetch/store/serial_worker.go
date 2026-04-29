package store

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

type BlockStorer[T any] interface {
	StoreBlockData(context.Context, T) error
}

type StoreRequest[T any] struct {
	Ctx       context.Context
	Hash      string
	Height    uint64
	BlockData T
	ResultCh  chan error
}

type SerialWorker[T any] struct {
	dbOperator   BlockStorer[T]
	storedBlocks StoredState
	isZero       func(T) bool

	reqCh    chan *StoreRequest[T]
	inflight InflightState
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	submitted uint64
	skipped   uint64
	succeeded uint64
	failed    uint64
	canceled  uint64
}

func NewSerialWorker[T any](dbOperator BlockStorer[T], storedBlocks StoredState, isZero func(T) bool) *SerialWorker[T] {
	return &SerialWorker[T]{
		dbOperator:   dbOperator,
		storedBlocks: storedBlocks,
		isZero:       isZero,
		reqCh:        make(chan *StoreRequest[T], 64),
		inflight:     NewInflightState(),
	}
}

func (w *SerialWorker[T]) SetDbOperator(dbOperator BlockStorer[T]) {
	if w == nil {
		return
	}
	w.dbOperator = dbOperator
}

func (w *SerialWorker[T]) Start() {
	if w == nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go w.runLoop(ctx)
}

func (w *SerialWorker[T]) runLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.logStats()
		case req := <-w.reqCh:
			if req == nil {
				continue
			}
			err := w.runRequest(req)
			req.ResultCh <- err
		}
	}
}

func (w *SerialWorker[T]) Stop() {
	if w == nil {
		return
	}
	if w.cancel != nil {
		w.cancel()
		w.cancel = nil
	}
	w.wg.Wait()
}

func (w *SerialWorker[T]) Submit(ctx context.Context, hash string, height uint64, blockData T) error {
	if w == nil {
		return fmt.Errorf("store block worker is nil")
	}
	hash = normalizeHash(hash)
	if hash == "" {
		return fmt.Errorf("invalid block hash")
	}
	if w.isZero != nil && w.isZero(blockData) {
		return fmt.Errorf("block data is nil")
	}
	if w.reqCh == nil {
		return fmt.Errorf("store block worker is not initialized")
	}
	if !w.inflight.TryStart(hash, w.storedBlocks) {
		atomic.AddUint64(&w.skipped, 1)
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	req := &StoreRequest[T]{
		Ctx:       ctx,
		Hash:      hash,
		Height:    height,
		BlockData: blockData,
		ResultCh:  make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		w.inflight.Finish(hash)
		atomic.AddUint64(&w.canceled, 1)
		return ctx.Err()
	case w.reqCh <- req:
		_ = height
		atomic.AddUint64(&w.submitted, 1)
	}

	select {
	case <-ctx.Done():
		atomic.AddUint64(&w.canceled, 1)
		return ctx.Err()
	case err := <-req.ResultCh:
		return err
	}
}

func (w *SerialWorker[T]) IsInflight(hash string) bool {
	if w == nil {
		return false
	}
	return w.inflight.Has(hash)
}

func (w *SerialWorker[T]) MetricsPayload() map[string]any {
	if w == nil {
		return map[string]any{}
	}

	queueCap := 0
	queuePending := 0
	if w.reqCh != nil {
		queueCap = cap(w.reqCh)
		queuePending = len(w.reqCh)
	}

	return map[string]any{
		"totals": map[string]uint64{
			"submitted": atomic.LoadUint64(&w.submitted),
			"skipped":   atomic.LoadUint64(&w.skipped),
			"succeeded": atomic.LoadUint64(&w.succeeded),
			"failed":    atomic.LoadUint64(&w.failed),
			"canceled":  atomic.LoadUint64(&w.canceled),
		},
		"queue": map[string]uint64{
			"pending":  uint64(queuePending),
			"capacity": uint64(queueCap),
		},
		"state": map[string]uint64{
			"storing": uint64(w.inflight.Count()),
		},
	}
}

func (w *SerialWorker[T]) logStats() {
	if w == nil || w.reqCh == nil {
		return
	}

	logrus.Infof("store block worker stats submitted:%v skipped:%v succeeded:%v failed:%v canceled:%v queue_pending:%v storing:%v",
		atomic.LoadUint64(&w.submitted),
		atomic.LoadUint64(&w.skipped),
		atomic.LoadUint64(&w.succeeded),
		atomic.LoadUint64(&w.failed),
		atomic.LoadUint64(&w.canceled),
		len(w.reqCh),
		w.inflight.Count(),
	)
}

func (w *SerialWorker[T]) runRequest(req *StoreRequest[T]) error {
	if w == nil || req == nil {
		return fmt.Errorf("store request is nil")
	}
	if req.Ctx == nil {
		req.Ctx = context.Background()
	}
	if w.dbOperator == nil {
		w.inflight.Finish(req.Hash)
		atomic.AddUint64(&w.failed, 1)
		return fmt.Errorf("db operator is nil")
	}

	err := w.dbOperator.StoreBlockData(req.Ctx, req.BlockData)
	if err != nil {
		w.inflight.Finish(req.Hash)
		atomic.AddUint64(&w.failed, 1)
		return err
	}
	if w.storedBlocks != nil {
		w.storedBlocks.MarkStored(req.Hash)
	}
	w.inflight.Finish(req.Hash)
	atomic.AddUint64(&w.succeeded, 1)
	return nil
}
