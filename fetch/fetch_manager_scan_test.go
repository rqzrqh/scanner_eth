package fetch

import (
	"context"
	"database/sql"
	"errors"
	"expvar"
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/data"
	"scanner_eth/model"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newTestFetchManager(t *testing.T, irreversible int) *FetchManager {
	t.Helper()
	fm := &FetchManager{
		scanTriggerCh:       make(chan struct{}, 1),
		blockTree:           blocktree.NewBlockTree(irreversible),
		pendingPayloadStore: NewBlockPayloadStore(),
		storedBlocks:        newStoredBlockState(),
		startHeight:         1,
		irreversibleBlocks:  irreversible,
	}
	fm.nodeManager = NewNodeManager([]*ethclient.Client{nil}, 0)
	fm.blockFetcher = &mockBlockFetcher{}
	fm.taskPool = newTaskPoolWithStop(
		TaskPoolOptions{WorkerCount: 1, HighQueueSize: 64, NormalQueueSize: 64, MaxRetry: 2},
		1,
		fm.handleTaskPoolTask,
	)
	return fm
}

func runScanAndWait(t *testing.T, fm *FetchManager) {
	t.Helper()
	fm.scanEnabled.Store(true)
	fm.scanEvents(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	quietSince := time.Time{}
	for time.Now().Before(deadline) {
		drainedTrigger := false
		for {
			select {
			case <-fm.scanTriggerCh:
				drainedTrigger = true
				fm.scanEvents(context.Background())
			default:
				goto checkIdle
			}
		}

	checkIdle:
		heightCount, hashCount := fm.taskPool.headerSyncCounts()
		headerSyncing := heightCount > 0 || hashCount > 0
		if !headerSyncing && !drainedTrigger {
			if quietSince.IsZero() {
				quietSince = time.Now()
			} else if time.Since(quietSince) >= 100*time.Millisecond {
				return
			}
		} else {
			quietSince = time.Time{}
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("scan stages did not finish before timeout")
}

func makeHeader(height uint64, hash string, parent string) *BlockHeaderJson {
	return &BlockHeaderJson{
		Number:       hexutil.EncodeUint64(height),
		Hash:         hash,
		ParentHash:   parent,
		Difficulty:   "0x1",
		Transactions: []string{},
	}
}

func makeEventBlockData(height uint64, hash string, parent string) *EventBlockData {
	return &EventBlockData{
		StorageFullBlock: &StorageFullBlock{
			Block: model.Block{Height: height, Hash: hash, ParentHash: parent, Complete: true},
		},
	}
}

func makeMinimalFullBlock(height uint64, hash string, parent string) *data.FullBlock {
	return &data.FullBlock{
		Block: &data.Block{Height: height, Hash: hash, ParentHash: parent},
		StateSet: &data.StateSet{
			ContractList:       []*data.Contract{},
			ContractErc20List:  []*data.ContractErc20{},
			ContractErc721List: []*data.ContractErc721{},
			BalanceNativeList:  []*data.BalanceNative{},
			BalanceErc20List:   []*data.BalanceErc20{},
			BalanceErc1155List: []*data.BalanceErc1155{},
			TokenErc721List:    []*data.TokenErc721{},
		},
	}
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.Block{}); err != nil {
		t.Fatalf("auto migrate block failed: %v", err)
	}
	return db
}

type mockDbOperator struct {
	loadFn  func(context.Context) ([]model.Block, error)
	storeFn func(context.Context, *EventBlockData) error
}

func (m *mockDbOperator) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
	if m != nil && m.loadFn != nil {
		return m.loadFn(ctx)
	}
	return nil, nil
}

func (m *mockDbOperator) StoreBlockData(ctx context.Context, blockData *EventBlockData) error {
	if m != nil && m.storeFn != nil {
		return m.storeFn(ctx, blockData)
	}
	return nil
}

type mockBlockFetcher struct {
	fetchByHeightFn func(ctx context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson
	fetchByHashFn   func(ctx context.Context, nodeOp *NodeOperator, taskID int, hash string) *BlockHeaderJson
	fetchFullFn     func(ctx context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock
}

func (m *mockBlockFetcher) FetchBlockHeaderByHeight(ctx context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson {
	if m != nil && m.fetchByHeightFn != nil {
		return m.fetchByHeightFn(ctx, nodeOp, taskID, height)
	}
	return nil
}

func (m *mockBlockFetcher) FetchBlockHeaderByHash(ctx context.Context, nodeOp *NodeOperator, taskID int, hash string) *BlockHeaderJson {
	if m != nil && m.fetchByHashFn != nil {
		return m.fetchByHashFn(ctx, nodeOp, taskID, hash)
	}
	return nil
}

func (m *mockBlockFetcher) FetchFullBlock(ctx context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
	if m != nil && m.fetchFullFn != nil {
		return m.fetchFullFn(ctx, nodeOp, taskID, header)
	}
	return nil
}

func setNodeLatestHeight(fm *FetchManager, height uint64) {
	if fm == nil {
		return
	}
	if fm.nodeManager == nil {
		fm.nodeManager = NewNodeManager([]*ethclient.Client{nil}, 0)
	}
	// Tests need to set an absolute tip height. Production RemoteChain.Update ignores
	// regressive heights; reset node 0's view so each helper call is authoritative.
	fm.nodeManager.mu.Lock()
	if len(fm.nodeManager.nodes) > 0 && fm.nodeManager.nodes[0] != nil {
		fm.nodeManager.nodes[0].remote = NewRemoteChain()
	}
	fm.nodeManager.mu.Unlock()

	fm.nodeManager.UpdateNodeChainInfo(0, height, "")
}

func TestNewFetchManagerSupportsInjectedOperators(t *testing.T) {
	db := newTestDB(t)
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = redisClient.Close() })

	injectedDbOp := &mockDbOperator{}
	injectedFetcher := &mockBlockFetcher{}

	fm := NewFetchManager(
		"test-chain",
		[]*ethclient.Client{},
		redisClient,
		1,
		0,
		2,
		0,
		TaskPoolOptions{},
		db,
		1,
		injectedDbOp,
		injectedFetcher,
	)

	if fm.dbOperator != injectedDbOp {
		t.Fatalf("expected injected dbOperator to be used")
	}
	if fm.blockFetcher != injectedFetcher {
		t.Fatalf("expected injected blockFetcher to be used")
	}
}

func TestNewFetchManagerKeepsNilOperatorsWhenNilInjected(t *testing.T) {
	db := newTestDB(t)
	redisClient := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	t.Cleanup(func() { _ = redisClient.Close() })

	fm := NewFetchManager(
		"test-chain",
		[]*ethclient.Client{},
		redisClient,
		1,
		0,
		2,
		0,
		TaskPoolOptions{},
		db,
		1,
		nil,
		nil,
	)

	if fm.dbOperator != nil {
		t.Fatalf("expected dbOperator to remain nil when nil is injected")
	}
	if fm.blockFetcher != nil {
		t.Fatalf("expected blockFetcher to remain nil when nil is injected")
	}
}

func TestScanEventsRule1BootstrapOnEmptyTree(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.startHeight = 100
	var calls atomic.Int32
	setNodeLatestHeight(fm, 100)
	fm.blockFetcher = &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		if height != 100 {
			t.Fatalf("unexpected bootstrap height: %v", height)
		}
		calls.Add(1)
		return makeHeader(height, "0xaaa", "")
	}}

	runScanAndWait(t, fm)
	if calls.Load() != 1 {
		t.Fatalf("bootstrap header fetch count mismatch: got=%v want=1", calls.Load())
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 100 || end != 100 {
		t.Fatalf("bootstrap range mismatch: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestScanEventsRule2WindowExpandsToDoubleIrreversible(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.insertHeader(makeHeader(10, "0xa", ""))
	setNodeLatestHeight(fm, 13)
	fm.dbOperator = &mockDbOperator{}
	var calls atomic.Int32
	fm.blockFetcher = &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		calls.Add(1)
		switch height {
		case 11:
			return makeHeader(11, "0xb", "0xa")
		case 12:
			return makeHeader(12, "0xc", "0xb")
		case 13:
			return makeHeader(13, "0xd", "0xc")
		default:
			return nil
		}
	}, fetchFullFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		if header == nil {
			return nil
		}
		h := uint64(0)
		if header.Number != "" {
			if v, err := hexutil.DecodeUint64(header.Number); err == nil {
				h = v
			}
		}
		return makeMinimalFullBlock(h, normalizeHash(header.Hash), normalizeHash(header.ParentHash))
	}}

	runScanAndWait(t, fm)
	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 10 || end < 11 {
		t.Fatalf("range mismatch after window sync: ok=%v start=%v end=%v", ok, start, end)
	}
	if calls.Load() < 1 {
		t.Fatalf("window sync call mismatch: got=%v want>=1", calls.Load())
	}
}

func TestScanEventsRule3SyncOrphanParentsByHash(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.insertHeader(makeHeader(10, "0xroot", ""))
	fm.blockTree.Insert(12, "0xchild", "0xmissing", 1, nil, nil)
	setNodeLatestHeight(fm, 10)
	fm.blockFetcher = &mockBlockFetcher{fetchByHashFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, hash string) *BlockHeaderJson {
		if hash != "0xmissing" {
			return nil
		}
		return makeHeader(11, "0xmissing", "0xroot")
	}}

	runScanAndWait(t, fm)
	if fm.blockTree.Get("0xmissing") == nil {
		t.Fatalf("missing parent should be inserted")
	}
	if fm.blockTree.Get("0xchild") == nil {
		t.Fatalf("orphan child should be linked after parent sync")
	}
}

func TestScanEventsRule4BranchWriteCondition(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil, nil)
	fm.blockTree.Insert(2, "y", "a", 1, nil, nil)
	fm.blockTree.Insert(3, "z", "y", 1, nil, nil)
	fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
	fm.setNodeBlockHeader("c", makeHeader(3, "c", "b"))
	fm.setNodeBlockHeader("y", makeHeader(2, "y", "a"))
	fm.setNodeBlockHeader("z", makeHeader(3, "z", "y"))

	fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))
	fm.setNodeBlockBody("c", makeEventBlockData(3, "c", "b"))
	fm.setNodeBlockBody("y", makeEventBlockData(2, "y", "a"))
	fm.setNodeBlockBody("z", makeEventBlockData(3, "z", "y"))

	fm.storedBlocks.MarkStored("a")
	setNodeLatestHeight(fm, 3)

	writeOrder := make([]string, 0)
	var writeOrderMu sync.Mutex
	fm.dbOperator = &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			if blockData != nil && blockData.StorageFullBlock != nil {
				writeOrderMu.Lock()
				writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
				writeOrderMu.Unlock()
			}
			return nil
		},
	}

	runScanAndWait(t, fm)
	writeOrderMu.Lock()
	defer writeOrderMu.Unlock()

	want := []string{"b", "c", "y", "z"}
	if len(writeOrder) != len(want) {
		t.Fatalf("write count mismatch: got=%v want=%v order=%v", len(writeOrder), len(want), writeOrder)
	}
	for i := range want {
		if writeOrder[i] != want[i] {
			t.Fatalf("write order mismatch at %v: got=%v want=%v full=%v", i, writeOrder[i], want[i], writeOrder)
		}
	}
}

func TestScanEventsRule5PruneRemovesStoredAndTasks(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil, nil)
		fm.storedBlocks.MarkStored(hash)
		fm.taskPool.addTask(hash)
		parent = hash
	}
	setNodeLatestHeight(fm, 6)

	runScanAndWait(t, fm)

	for h := uint64(1); h <= 3; h++ {
		hash := fmt.Sprintf("h%v", h)
		if fm.storedBlocks.IsStored(hash) {
			t.Fatalf("pruned hash should be removed from storagedBlock: %v", hash)
		}
		if fm.taskPool.hasTask(hash) {
			t.Fatalf("pruned hash should be removed from taskManager: %v", hash)
		}
	}
	for h := uint64(4); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		if !fm.storedBlocks.IsStored(hash) {
			t.Fatalf("kept hash should remain stored: %v", hash)
		}
	}
}

func TestTaskPoolAsyncDeduplicated(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "0xabc", "", 1, nil, nil)
	fm.setNodeBlockHeader("0xabc", makeHeader(1, "0xabc", ""))
	setNodeLatestHeight(fm, 1)

	release := make(chan struct{})
	var handled int32
	fm.blockFetcher = &mockBlockFetcher{fetchFullFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		if normalizeHash(header.Hash) == "0xabc" {
			atomic.AddInt32(&handled, 1)
			<-release
		}
		return makeMinimalFullBlock(1, header.Hash, header.ParentHash)
	}}
	fm.taskPool.start()
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.taskPool.enqueueTask("0xabc")
	fm.taskPool.enqueueTask("0xabc")

	waitHandleDeadline := time.Now().Add(300 * time.Millisecond)
	for atomic.LoadInt32(&handled) == 0 && time.Now().Before(waitHandleDeadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if !fm.taskPool.hasTask("0xabc") {
		t.Fatalf("task should exist while worker is processing")
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed before release: got=%v want=1", got)
	}

	close(release)
	deadline := time.Now().Add(500 * time.Millisecond)
	for fm.taskPool.hasTask("0xabc") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if fm.taskPool.hasTask("0xabc") {
		t.Fatalf("task should be removed after async worker completes")
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed: got=%v want=1", got)
	}
}

func TestTaskPoolPriorityHighFirst(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "normal", "", 1, nil, nil)
	fm.blockTree.Insert(2, "high", "normal", 1, nil, nil)
	fm.setNodeBlockHeader("normal", makeHeader(1, "normal", ""))
	fm.setNodeBlockHeader("high", makeHeader(2, "high", "normal"))
	setNodeLatestHeight(fm, 2)
	order := make([]string, 0, 2)
	ready := make(chan struct{})

	fm.blockFetcher = &mockBlockFetcher{fetchFullFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		order = append(order, header.Hash)
		if len(order) == 2 {
			close(ready)
		}
		height := uint64(1)
		if header.Hash == "high" {
			height = 2
		}
		return makeMinimalFullBlock(height, header.Hash, header.ParentHash)
	}}
	fm.taskPool.addTask("normal")
	fm.taskPool.pushTask(&syncTask{hash: "normal", priority: taskPriorityNormal, retry: 0}, true)
	fm.taskPool.addTask("high")
	fm.taskPool.pushTask(&syncTask{hash: "high", priority: taskPriorityHigh, retry: 0}, true)
	fm.taskPool.start()
	t.Cleanup(func() { fm.taskPool.stop() })

	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for tasks")
	}

	if len(order) != 2 {
		t.Fatalf("order length mismatch: %v", order)
	}
	if order[0] != "high" {
		t.Fatalf("high priority should run first, got order=%v", order)
	}
}

func TestTaskPoolRetryAndStats(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "retry-me", "", 1, nil, nil)
	setNodeLatestHeight(fm, 1)
	fm.taskPool.maxRetry = 2
	var attempts int32
	done := make(chan struct{})

	fm.blockFetcher = &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, hash string) *BlockHeaderJson {
			if normalizeHash(hash) != "retry-me" {
				return nil
			}
			current := atomic.AddInt32(&attempts, 1)
			if current >= 3 {
				return makeHeader(1, "retry-me", "")
			}
			return nil
		},
		fetchFullFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
			if normalizeHash(header.Hash) == "retry-me" {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return makeMinimalFullBlock(1, header.Hash, header.ParentHash)
		},
	}
	fm.taskPool.start()
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.taskPool.enqueueTask("retry-me")

	select {
	case <-done:
	case <-time.After(800 * time.Millisecond):
		t.Fatalf("timeout waiting retry task completion")
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for fm.taskPool.hasTask("retry-me") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if fm.taskPool.hasTask("retry-me") {
		t.Fatalf("retry task should be removed after success")
	}

	stats := fm.taskPool.stats()
	if stats.Retried < 2 {
		t.Fatalf("expected retried >= 2, got=%v", stats.Retried)
	}
	if stats.Succeeded < 1 {
		t.Fatalf("expected succeeded >= 1, got=%v", stats.Succeeded)
	}
	if stats.WorkerCount != 1 {
		t.Fatalf("expected worker count 1, got=%v", stats.WorkerCount)
	}
	if stats.MaxRetry != 2 {
		t.Fatalf("expected max retry 2, got=%v", stats.MaxRetry)
	}
}

func TestNormalizeTaskPoolOptions(t *testing.T) {
	options := normalizeTaskPoolOptions(TaskPoolOptions{}, 3)
	if options.WorkerCount != 3 {
		t.Fatalf("worker count mismatch: got=%v want=3", options.WorkerCount)
	}
	if options.HighQueueSize != 1024 {
		t.Fatalf("high queue size mismatch: got=%v want=1024", options.HighQueueSize)
	}
	if options.NormalQueueSize != 2048 {
		t.Fatalf("normal queue size mismatch: got=%v want=2048", options.NormalQueueSize)
	}
	if options.MaxRetry != 2 {
		t.Fatalf("max retry mismatch: got=%v want=2", options.MaxRetry)
	}
}

func TestEnableTaskPoolMetricsPublishesStats(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	name := fmt.Sprintf("fetch_task_pool_test_%d", time.Now().UnixNano())

	fm.EnableTaskPoolMetrics(name)
	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("expected expvar metric to be published")
	}
	if !strings.Contains(v.String(), "\"config\"") || !strings.Contains(v.String(), "\"worker_count\":1") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}
	if !strings.Contains(v.String(), "\"by_kind\"") || !strings.Contains(v.String(), "\"header_hash_sync\"") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}
}

func TestComplexOrphanCascadeInsertionTracksTreeAndStoredRange(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.storedBlocks.MarkStored("ghost")
	fm.storedBlocks.MarkStored("a")
	fm.storedBlocks.MarkStored("g")
	fm.storedBlocks.MarkStored("y")

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(5, "e", "d", 1, nil, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil, nil)
	fm.blockTree.Insert(7, "g", "f", 1, nil, nil)
	fm.blockTree.Insert(6, "f", "e", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.blockTree.Insert(4, "d", "c", 1, nil, nil)
	fm.blockTree.Insert(4, "x", "c", 1, nil, nil)
	fm.blockTree.Insert(5, "y", "x", 1, nil, nil)

	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "x", "y"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to exist after cascade insert", key)
		}
	}

	root := fm.blockTree.Root()
	if root == nil || root.Key != "a" || root.Height != 1 {
		t.Fatalf("unexpected root after cascade insert: %+v", root)
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 1 || end != 7 {
		t.Fatalf("unexpected height range after cascade insert: ok=%v start=%v end=%v", ok, start, end)
	}

	branches := fm.blockTree.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header == nil || branches[0].Header.Key != "g" {
		t.Fatalf("expected first branch header g, got=%+v", branches[0].Header)
	}
	if branches[1].Header == nil || branches[1].Header.Key != "y" {
		t.Fatalf("expected second branch header y, got=%+v", branches[1].Header)
	}

	branch0 := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		branch0 = append(branch0, node.Key)
	}
	if got := strings.Join(branch0, ","); got != "g,f,e,d,c,b,a" {
		t.Fatalf("unexpected main branch path: %s", got)
	}

	branch1 := make([]string, 0, len(branches[1].Nodes))
	for _, node := range branches[1].Nodes {
		branch1 = append(branch1, node.Key)
	}
	if got := strings.Join(branch1, ","); got != "y,x,c,b,a" {
		t.Fatalf("unexpected fork branch path: %s", got)
	}

	storedStart, storedEnd, storedOK := fm.storedHeightRangeOnTree()
	if !storedOK || storedStart != 1 || storedEnd != 7 {
		t.Fatalf("unexpected stored height range: ok=%v start=%v end=%v", storedOK, storedStart, storedEnd)
	}
}

func TestPruneComplexForkRemovesPrunedBranchesAndStoredState(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil, nil)
	fm.blockTree.Insert(4, "d", "c", 1, nil, nil)
	fm.blockTree.Insert(5, "e", "d", 1, nil, nil)
	fm.blockTree.Insert(3, "x", "b", 1, nil, nil)
	fm.blockTree.Insert(4, "y", "x", 1, nil, nil)

	for _, key := range []string{"a", "b", "c", "d", "e", "x", "y"} {
		fm.storedBlocks.MarkStored(key)
		fm.taskPool.addTask(key)
	}

	fm.pruneStoredBlocks(context.Background())

	root := fm.blockTree.Root()
	if root == nil || root.Key != "c" || root.Height != 3 {
		t.Fatalf("unexpected root after prune: %+v", root)
	}

	for _, key := range []string{"a", "b", "x", "y"} {
		if fm.blockTree.Get(key) != nil {
			t.Fatalf("expected pruned node %s to be removed from tree", key)
		}
		if fm.taskPool.hasTask(key) {
			t.Fatalf("expected pruned node %s to be removed from taskManager", key)
		}
		stored := fm.storedBlocks.IsStored(key)
		if stored {
			t.Fatalf("expected pruned node %s to be removed from storagedBlock", key)
		}
	}

	for _, key := range []string{"c", "d", "e"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected kept node %s to remain in tree", key)
		}
		if !fm.taskPool.hasTask(key) {
			t.Fatalf("expected kept node %s task to remain", key)
		}
		stored := fm.storedBlocks.IsStored(key)
		if !stored {
			t.Fatalf("expected kept node %s to remain in storagedBlock", key)
		}
	}

	branches := fm.blockTree.Branches()
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch after prune, got=%d", len(branches))
	}
	path := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		path = append(path, node.Key)
	}
	if got := strings.Join(path, ","); got != "e,d,c" {
		t.Fatalf("unexpected remaining branch path after prune: %s", got)
	}
}

func TestProcessBranchesDoesNotStoreWhenParentNotStored(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil, nil)
	fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
	fm.setNodeBlockHeader("c", makeHeader(3, "c", "b"))

	fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))
	fm.setNodeBlockBody("c", makeEventBlockData(3, "c", "b"))

	writeOrder := make([]string, 0)
	fm.dbOperator = &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			if blockData != nil && blockData.StorageFullBlock != nil {
				writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
			}
			return nil
		},
	}

	fm.processBranchesLowToHigh(context.Background())

	if len(writeOrder) != 0 {
		t.Fatalf("expected no writes when parent is not stored, got=%v", writeOrder)
	}
	if fm.storedBlocks.IsStored("b") || fm.storedBlocks.IsStored("c") {
		t.Fatalf("child nodes should not be marked stored when parent is not stored")
	}
	if fm.taskPool.hasTask("b") || fm.taskPool.hasTask("c") {
		t.Fatalf("nodes with data but blocked parent should be skipped without auto-enqueueing tasks")
	}
}

func TestProcessBranchesMarksStoredOnlyOnSuccessfulWrite(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.storedBlocks.MarkStored("a")
	fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
	fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))

	called := 0
	fm.dbOperator = &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			called++
			return errors.New("write failed")
		},
	}

	fm.processBranchesLowToHigh(context.Background())

	if called != 1 {
		t.Fatalf("expected one write attempt, got=%d", called)
	}
	if fm.storedBlocks.IsStored("b") {
		t.Fatalf("block should not be marked stored on write failure")
	}
}

func TestPruneNoopWhenStoredSpanWithinKeepEvenWithStaleStoredHashes(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(10, "a", "", 1, nil, nil)
	fm.blockTree.Insert(11, "b", "a", 1, nil, nil)
	fm.blockTree.Insert(12, "c", "b", 1, nil, nil)

	fm.storedBlocks.MarkStored("a")
	fm.storedBlocks.MarkStored("b")
	fm.storedBlocks.MarkStored("c")
	fm.storedBlocks.MarkStored("ghost-1")
	fm.storedBlocks.MarkStored("ghost-2")
	fm.taskPool.addTask("a")
	fm.taskPool.addTask("b")
	fm.taskPool.addTask("c")

	fm.pruneStoredBlocks(context.Background())

	for _, key := range []string{"a", "b", "c"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to remain when prune is noop", key)
		}
		if !fm.storedBlocks.IsStored(key) {
			t.Fatalf("expected stored node %s to remain stored when prune is noop", key)
		}
	}
	for _, key := range []string{"ghost-1", "ghost-2"} {
		ok := fm.storedBlocks.IsStored(key)
		if !ok {
			t.Fatalf("expected stale stored hash %s to be untouched by noop prune", key)
		}
	}
	if !fm.taskPool.hasTask("a") || !fm.taskPool.hasTask("b") || !fm.taskPool.hasTask("c") {
		t.Fatalf("expected tasks to remain untouched when prune is noop")
	}
}

func TestRestoreBlockTreeLoadsWindowAndCompleteState(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 7, Hash: "0x07", ParentHash: "0x06", Difficulty: "7", Complete: true},
		{Height: 8, Hash: "0x08", ParentHash: "", Difficulty: "8", Complete: false},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true},
		{Height: 10, Hash: "0x0A", ParentHash: "0x09", Difficulty: "10", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	fm.dbOperator = newFetchManagerDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)

	windowBlocks, err := fm.dbOperator.LoadBlockWindowFromDB(context.Background())
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load latest blocks failed: %v", err)
	}
	if loaded != 3 {
		t.Fatalf("loaded block count mismatch: got=%v want=3", loaded)
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected loaded height range: ok=%v start=%v end=%v", ok, start, end)
	}

	if fm.blockTree.Get("0x07") != nil {
		t.Fatalf("block below recovery window should not be loaded")
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if fm.blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s to be loaded into tree", hash)
		}
	}

	if fm.storedBlocks.IsStored("0x08") {
		t.Fatalf("incomplete block should not be marked stored")
	}
	if !fm.storedBlocks.IsStored("0x09") || !fm.storedBlocks.IsStored("0x0a") {
		t.Fatalf("complete blocks in recovery window should be marked stored")
	}

	node := fm.blockTree.Get("0x0a")
	if node == nil || node.Weight != 10 {
		t.Fatalf("unexpected stored weight for height 10 node: %+v", node)
	}
	if fm.getNodeBlockHeader("0x0a") == nil {
		t.Fatalf("expected non-nil header on inserted node")
	}
	if node.Irreversible.Key == "" {
		t.Fatalf("expected non-empty irreversible key on inserted node")
	}
}

func TestRestoreBlockTreeInsertsWhenParentMissingOutsideWindow(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 8, Hash: "0x08", ParentHash: "0x07", Difficulty: "8", Complete: true},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true},
		{Height: 10, Hash: "0x0A", ParentHash: "0x09", Difficulty: "10", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	fm.dbOperator = newFetchManagerDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)

	windowBlocks, err := fm.dbOperator.LoadBlockWindowFromDB(context.Background())
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("restore with missing off-window parent should succeed: %v", err)
	}
	if loaded != 3 {
		t.Fatalf("loaded block count mismatch: got=%v want=3", loaded)
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if fm.blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s in tree", hash)
		}
	}
	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected height range: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestRestoreBlockTreeHandlesEmptyTableAndHeightZero(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	fm.dbOperator = newFetchManagerDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)

	windowBlocks, err := fm.dbOperator.LoadBlockWindowFromDB(context.Background())
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load empty table failed: %v", err)
	}
	if loaded != 0 {
		t.Fatalf("expected no blocks loaded from empty table, got=%v", loaded)
	}

	block := model.Block{Height: 0, Hash: "0xgenesis", ParentHash: "", Difficulty: "11", Complete: true}
	if err := db.Create(&block).Error; err != nil {
		t.Fatalf("seed height zero block failed: %v", err)
	}

	fm.deleteRuntimeState()
	fm.createRuntimeState()
	windowBlocks, err = fm.dbOperator.LoadBlockWindowFromDB(context.Background())
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}

	loaded, err = fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load height zero block failed: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("expected one block loaded, got=%v", loaded)
	}
	node := fm.blockTree.Get("0xgenesis")
	if node == nil || node.Height != 0 {
		t.Fatalf("expected height zero block in tree, got=%+v", node)
	}
	if !fm.storedBlocks.IsStored("0xgenesis") {
		t.Fatalf("complete height zero block should be marked stored")
	}

	var maxHeight sql.NullInt64
	if err := db.Model(&model.Block{}).Select("MAX(height)").Scan(&maxHeight).Error; err != nil {
		t.Fatalf("query max height failed: %v", err)
	}
	if !maxHeight.Valid || maxHeight.Int64 != 0 {
		t.Fatalf("expected valid max height 0, got valid=%v value=%v", maxHeight.Valid, maxHeight.Int64)
	}
}

func TestOnBecameLeaderUsesDBBootstrapWithoutRemoteFetch(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 20, Hash: "0x20", ParentHash: "", Difficulty: "20", Complete: true},
		{Height: 21, Hash: "0x21", ParentHash: "0x20", Difficulty: "21", Complete: false},
		{Height: 22, Hash: "0x22", ParentHash: "0x21", Difficulty: "22", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed bootstrap blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	fm.dbOperator = newFetchManagerDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	fm.storedBlocks.MarkStored("stale")
	fm.taskPool.addTask("stale")
	remoteCalls := 0
	fm.blockFetcher = &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		remoteCalls++
		return makeHeader(height, "0xremote", "")
	}}

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader failed: %v", err)
	}
	if remoteCalls != 0 {
		t.Fatalf("expected db bootstrap to avoid remote calls, got=%v", remoteCalls)
	}
	if fm.taskPool.hasTask("stale") || fm.storedBlocks.IsStored("stale") {
		t.Fatalf("runtime state should be reset before db bootstrap")
	}
	if fm.blockTree.Get("0x20") == nil || fm.blockTree.Get("0x21") == nil || fm.blockTree.Get("0x22") == nil {
		t.Fatalf("expected db bootstrap blocks to populate block tree")
	}
	if !fm.storedBlocks.IsStored("0x20") || fm.storedBlocks.IsStored("0x21") != false || !fm.storedBlocks.IsStored("0x22") {
		t.Fatalf("stored markers should follow complete state after db bootstrap")
	}
}

func TestOnBecameLeaderFallsBackToRemoteBootstrapWhenDBEmpty(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	fm.dbOperator = newFetchManagerDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	fm.startHeight = 30
	fm.storedBlocks.MarkStored("stale")
	fm.taskPool.addTask("stale")
	fetchHeaderCalls := 0
	setNodeLatestHeight(fm, 30)
	fm.blockFetcher = &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp *NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		fetchHeaderCalls++
		if height != 30 {
			t.Fatalf("unexpected header fetch height: %v", height)
		}
		return makeHeader(30, "0x30", "0x29")
	}}

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader fallback failed: %v", err)
	}
	if fetchHeaderCalls != 1 {
		t.Fatalf("expected one remote bootstrap header fetch, got=%v", fetchHeaderCalls)
	}
	if fm.taskPool.hasTask("stale") || fm.storedBlocks.IsStored("stale") {
		t.Fatalf("runtime state should be reset before remote bootstrap")
	}
	root := fm.blockTree.Root()
	if root == nil || root.Height != 30 || root.Key != "0x30" {
		t.Fatalf("expected remote bootstrap root at height 30, got=%+v", root)
	}
}
