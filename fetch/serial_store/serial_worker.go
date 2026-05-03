package serialstore

import (
	"context"
	"fmt"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/util"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	statsLogIntervalWorker = 30 * time.Second
	reqChannelCapacity     = 64
)

type Branch struct {
	Nodes []BranchNode
}

type BranchNode struct {
	Hash      string
	ParentHash string
	Height    uint64
	BlockData *fetchstore.EventBlockData
}

type storeRequest struct {
	Ctx       context.Context
	Hash      string
	Height    uint64
	BlockData *fetchstore.EventBlockData
	Branches  []Branch
	ResultCh  chan error
}

type Worker struct {
	dbOperator   fetchstore.BlockDataStorer
	storedBlocks *fetchstore.StoredBlockState
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

func NewWorker(dbOperator fetchstore.BlockDataStorer, storedBlocks *fetchstore.StoredBlockState, isZero func(*fetchstore.EventBlockData) bool) *Worker {
	return &Worker{
		dbOperator:   dbOperator,
		storedBlocks: storedBlocks,
		isZero:       isZero,
		reqCh:        make(chan *storeRequest, reqChannelCapacity),
		inflight:     NewInflightState(),
	}
}

func NewStartedWorker(dbOperator fetchstore.BlockDataStorer, storedBlocks *fetchstore.StoredBlockState, isZero func(*fetchstore.EventBlockData) bool) *Worker {
	worker := NewWorker(dbOperator, storedBlocks, isZero)
	worker.Start()
	return worker
}

func (w *Worker) SetDBOperator(dbOperator fetchstore.BlockDataStorer) {
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
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return fmt.Errorf("invalid block hash")
	}
	if w.isZero != nil && w.isZero(blockData) {
		return fmt.Errorf("block data is nil")
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

func (w *Worker) SubmitBranches(ctx context.Context, branches []Branch) error {
	if w == nil {
		return fmt.Errorf("store block worker is nil")
	}
	if len(branches) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	req := &storeRequest{
		Ctx:      ctx,
		Branches: branches,
		ResultCh: make(chan error, 1),
	}

	select {
	case <-ctx.Done():
		atomic.AddUint64(&w.canceled, 1)
		return ctx.Err()
	case w.reqCh <- req:
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

	return map[string]any{
		"totals": map[string]uint64{
			"submitted": atomic.LoadUint64(&w.submitted),
			"skipped":   atomic.LoadUint64(&w.skipped),
			"succeeded": atomic.LoadUint64(&w.succeeded),
			"failed":    atomic.LoadUint64(&w.failed),
			"canceled":  atomic.LoadUint64(&w.canceled),
		},
		"queue": map[string]uint64{
			"pending":  uint64(len(w.reqCh)),
			"capacity": uint64(cap(w.reqCh)),
		},
		"state": map[string]uint64{
			"storing": uint64(w.inflight.Count()),
		},
	}
}

func (w *Worker) logStats() {
	if w == nil {
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
	if len(req.Branches) > 0 {
		return w.runBranches(req)
	}
	return w.runSingleRequest(req)
}

func (w *Worker) runSingleRequest(req *storeRequest) error {
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

func (w *Worker) runBranches(req *storeRequest) error {
	if req == nil {
		return nil
	}
	if req.Ctx == nil {
		req.Ctx = context.Background()
	}
	for _, branch := range req.Branches {
		if err := req.Ctx.Err(); err != nil {
			atomic.AddUint64(&w.canceled, 1)
			return err
		}
		w.runBranch(req.Ctx, branch)
	}
	return nil
}

func (w *Worker) runBranch(ctx context.Context, branch Branch) {
	if w == nil {
		return
	}
	readyParents := make(map[string]struct{}, len(branch.Nodes))
	for _, node := range branch.Nodes {
		if !w.runBranchNode(ctx, node, readyParents) {
			return
		}
		hash := util.NormalizeHash(node.Hash)
		if hash != "" {
			readyParents[hash] = struct{}{}
		}
	}
}

func (w *Worker) runBranchNode(ctx context.Context, node BranchNode, readyParents map[string]struct{}) bool {
	if w == nil {
		return false
	}
	hash := util.NormalizeHash(node.Hash)
	if hash == "" {
		return false
	}
	if w.storedBlocks != nil && w.storedBlocks.IsStored(hash) {
		return true
	}
	if w.inflight.Has(hash) {
		return false
	}
	if node.BlockData == nil {
		return false
	}
	if w.isZero != nil && w.isZero(node.BlockData) {
		return false
	}
	parentHash := util.NormalizeHash(node.ParentHash)
	parentReady := parentHash == ""
	if !parentReady {
		if _, ok := readyParents[parentHash]; ok {
			parentReady = true
		}
	}
	if !parentReady && w.storedBlocks != nil && w.storedBlocks.IsStored(parentHash) {
		parentReady = true
	}
	if !parentReady {
		return false
	}
	if err := w.storeInline(ctx, hash, node.Height, node.BlockData); err != nil {
		logrus.Warnf("store block failed. height:%v hash:%v err:%v", node.Height, hash, err)
		return false
	}
	return true
}

func (w *Worker) storeInline(ctx context.Context, hash string, height uint64, blockData *fetchstore.EventBlockData) error {
	if w == nil {
		return fmt.Errorf("store block worker is nil")
	}
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return fmt.Errorf("invalid block hash")
	}
	if w.isZero != nil && w.isZero(blockData) {
		return fmt.Errorf("block data is nil")
	}
	if !w.inflight.TryStart(hash, w.storedBlocks) {
		atomic.AddUint64(&w.skipped, 1)
		if w.storedBlocks != nil && w.storedBlocks.IsStored(hash) {
			return nil
		}
		return fmt.Errorf("block is already in flight")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		w.inflight.Finish(hash)
		atomic.AddUint64(&w.canceled, 1)
		return err
	}
	req := &storeRequest{
		Ctx:       ctx,
		Hash:      hash,
		Height:    height,
		BlockData: blockData,
	}
	return w.runSingleRequest(req)
}
