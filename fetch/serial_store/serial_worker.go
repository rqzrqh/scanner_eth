package serialstore

import (
	"context"
	"fmt"
	fetchstore "scanner_eth/fetch/store"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	statsLogIntervalWorker = 30 * time.Second
	reqChannelCapacity     = 64
)

type blockStorer interface {
	StoreBlockData(context.Context, *fetchstore.EventBlockData) error
}

type storeRequest struct {
	Ctx       context.Context
	Hash      string
	Height    uint64
	BlockData *fetchstore.EventBlockData
	ResultCh  chan error
}

type Worker struct {
	dbOperator   blockStorer
	storedBlocks StoredState
	isZero       func(*fetchstore.EventBlockData) bool

	reqCh       chan *storeRequest
	inflight    InflightState
	lifecycleMu sync.Mutex
	cancel      context.CancelFunc
	wg          sync.WaitGroup

	submitted uint64
	skipped   uint64
	succeeded uint64
	failed    uint64
	canceled  uint64
}

func NewWorker(dbOperator blockStorer, storedBlocks StoredState, isZero func(*fetchstore.EventBlockData) bool) *Worker {
	return &Worker{
		dbOperator:   dbOperator,
		storedBlocks: storedBlocks,
		isZero:       isZero,
		reqCh:        make(chan *storeRequest, reqChannelCapacity),
		inflight:     NewInflightState(),
	}
}

func NewStartedWorker(dbOperator blockStorer, storedBlocks StoredState, isZero func(*fetchstore.EventBlockData) bool) *Worker {
	worker := NewWorker(dbOperator, storedBlocks, isZero)
	worker.Start()
	return worker
}

func (w *Worker) SetDbOperator(dbOperator blockStorer) {
	if w == nil {
		return
	}
	w.dbOperator = dbOperator
}

func (w *Worker) Start() {
	if w == nil {
		return
	}

	w.lifecycleMu.Lock()
	defer w.lifecycleMu.Unlock()
	if w.cancel != nil {
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go w.runLoop(ctx)
}

func (w *Worker) runLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(statsLogIntervalWorker)
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

func (w *Worker) Stop() {
	if w == nil {
		return
	}

	w.lifecycleMu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.lifecycleMu.Unlock()
	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
}

func (w *Worker) Submit(ctx context.Context, hash string, height uint64, blockData *fetchstore.EventBlockData) error {
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

	req := &storeRequest{
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

func (w *Worker) IsInflight(hash string) bool {
	if w == nil {
		return false
	}
	return w.inflight.Has(hash)
}

func (w *Worker) MetricsPayload() map[string]any {
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

func (w *Worker) logStats() {
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

func (w *Worker) runRequest(req *storeRequest) error {
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
