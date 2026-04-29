package fetch

import (
	"context"
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/data"
	headernotify "scanner_eth/fetch/header_notify"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	"gorm.io/driver/sqlite"
	"gorm.io/gorm"

	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchscan "scanner_eth/fetch/scan"
	fetchtask "scanner_eth/fetch/task"
)

func newTestFetchManager(t *testing.T, irreversible int) *FetchManager {
	t.Helper()
	fm := &FetchManager{
		blockTree:           blocktree.NewBlockTree(irreversible),
		pendingPayloadStore: fetchstore.NewPayloadStore[*BlockHeaderJson, *EventBlockData](),
		storedBlocks:        fetchstore.NewStoredBlockState(),
		irreversibleBlocks: irreversible,
		scanConfig: fetchscan.Config{
			StartHeight: 1,
		},
		blockFetcher: &mockBlockFetcher{},
		dbOperator:   &mockDbOperator{},
	}
	fm.nodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	flow := fetchscan.NewFlow(fm.scanFlowRuntimeDeps, fetchscan.Config{StartHeight: fm.scanConfig.StartHeight})
	fm.scanFlow = flow
	fm.scanWorker = fm.newScanWorker(flow)
	attachTestRuntime(fm)
	fm.taskPool = fetchtask.NewTaskPoolWithStop(
		fetchtask.TaskPoolOptions{WorkerCount: 1, HighQueueSize: 64, NormalQueueSize: 64, MaxRetry: 2},
		1,
		func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
			return fetchscan.HandleTaskPoolTask(flow, task, stopCh)
		},
	)
	attachTestRuntime(fm)
	fm.storeWorker = fm.newStoreWorker()
	attachTestRuntime(fm)
	flow.BindRuntimeDeps()
	fm.storeWorker.Start()
	t.Cleanup(func() {
		if scanWorker := fm.runtimeScanWorker(); scanWorker != nil {
			scanWorker.Stop()
		}
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
		if storeWorker := fm.runtimeStoreWorker(); storeWorker != nil {
			storeWorker.Stop()
		}
		if taskPool := fm.runtimeTaskPool(); taskPool != nil {
			taskPool.Stop()
		}
	})
	return fm
}

func attachTestRuntime(fm *FetchManager) {
	if fm == nil {
		return
	}
	rt := fm.runtime
	if rt == nil {
		rt = &fetchRuntimeState{}
		fm.runtime = rt
	}
	rt.blockTree = fm.blockTree
	rt.storedBlocks = &fm.storedBlocks
	rt.pendingPayloadStore = fm.pendingPayloadStore
	rt.taskPool = &fm.taskPool
	rt.headerManager = fm.headerManager
	rt.scanFlow = fm.scanFlow
	rt.scanWorker = fm.scanWorker
	rt.storeWorker = fm.storeWorker
	fm.syncRuntimeFields()
}

func mustTestScanFlow(t *testing.T, fm *FetchManager) *fetchscan.Flow {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	scanFlow := fm.runtimeScanFlow()
	if scanFlow == nil {
		t.Fatal("scanFlow is nil")
	}
	return scanFlow
}

func mustTestScanWorker(t *testing.T, fm *FetchManager) *fetchscan.Worker {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	scanWorker := fm.runtimeScanWorker()
	if scanWorker == nil {
		t.Fatal("scanWorker is nil")
	}
	return scanWorker
}

func mustTestHeaderManager(t *testing.T, fm *FetchManager) *headernotify.Manager {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	headerManager := fm.runtimeHeaderManager()
	if headerManager == nil {
		t.Fatal("headerManager is nil")
	}
	return headerManager
}

func mustTestPayloadStore(t *testing.T, fm *FetchManager) *fetchstore.PayloadStore[*BlockHeaderJson, *EventBlockData] {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	payloadStore := fm.runtimePayloadStore()
	if payloadStore == nil {
		t.Fatal("payloadStore is nil")
	}
	return payloadStore
}

func setTestNodeBlockHeader(t *testing.T, fm *FetchManager, hash string, header *BlockHeaderJson) {
	t.Helper()
	if fm.runtimeBlockTree() == nil || fm.runtimeBlockTree().Get(hash) == nil {
		t.Fatalf("failed to set node block header for %q", hash)
	}
	mustTestPayloadStore(t, fm).SetHeader(hash, header)
	if got := mustTestPayloadStore(t, fm).GetHeader(hash); got != header {
		t.Fatalf("failed to set node block header for %q", hash)
	}
}

func setTestNodeBlockBody(t *testing.T, fm *FetchManager, hash string, data *EventBlockData) {
	t.Helper()
	if fm.runtimeBlockTree() == nil || fm.runtimeBlockTree().Get(hash) == nil {
		t.Fatalf("failed to set node block body for %q", hash)
	}
	mustTestPayloadStore(t, fm).SetBody(hash, data)
}

func getTestNodeBlockHeader(t *testing.T, fm *FetchManager, hash string) *BlockHeaderJson {
	t.Helper()
	return mustTestPayloadStore(t, fm).GetHeader(hash)
}

func snapshotTestStoredHashes(t *testing.T, fm *FetchManager) map[string]struct{} {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	storedBlocks := fm.runtimeStoredBlocks()
	if storedBlocks == nil {
		t.Fatal("storedBlocks is nil")
	}
	return storedBlocks.Snapshot()
}

func setTestDbOperator(fm *FetchManager, op DbOperator) {
	if fm == nil {
		return
	}
	fm.dbOperator = op
	if storeWorker := fm.runtimeStoreWorker(); storeWorker != nil {
		storeWorker.SetDbOperator(op)
	}
}

func newRuntimeDbOperator(db *gorm.DB, chainId int64, irreversibleBlocks int) DbOperator {
	return fetchstore.NewFullBlockDbOperator[*EventBlockData](db, chainId, irreversibleBlocks, func(blockData *EventBlockData) *fetchstore.StorageFullBlock {
		if blockData == nil {
			return nil
		}
		return blockData.StorageFullBlock
	})
}

func loadTestBlockWindowMaybeError(t *testing.T, fm *FetchManager, ctx context.Context) ([]model.Block, error) {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	if fm.dbOperator == nil {
		return nil, fmt.Errorf("db operator is nil")
	}
	return fm.dbOperator.LoadBlockWindowFromDB(ctx)
}

func loadTestBlockWindow(t *testing.T, fm *FetchManager, ctx context.Context) []model.Block {
	t.Helper()
	blocks, err := loadTestBlockWindowMaybeError(t, fm, ctx)
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}
	return blocks
}

func attachTestScanWorker(fm *FetchManager) {
	if fm == nil {
		return
	}
	flow := fm.runtimeScanFlow()
	if flow == nil {
		flow = fetchscan.NewFlow(fm.scanFlowRuntimeDeps, fetchscan.Config{StartHeight: fm.scanConfig.StartHeight})
	}
	if scanWorker := fm.runtimeScanWorker(); scanWorker != nil {
		scanWorker.Stop()
	}
	fm.scanFlow = flow
	fm.scanWorker = fm.newScanWorker(flow)
	attachTestRuntime(fm)
	flow.BindRuntimeDeps()
}

func attachTestHeaderManager(fm *FetchManager) {
	if fm == nil {
		return
	}
	if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
		headerManager.Stop()
	}
	fm.headerManager = fm.newHeaderManager()
	attachTestRuntime(fm)
}

func runScanAndWait(t *testing.T, fm *FetchManager) {
	t.Helper()
	scanWorker := fm.runtimeScanWorker()
	scanFlow := fm.runtimeScanFlow()
	taskPool := fm.runtimeTaskPool()
	if scanWorker == nil {
		t.Fatal("scanWorker is nil")
	}
	if scanWorker != nil {
		scanWorker.SetEnabled(true)
	}
	if scanFlow == nil {
		t.Fatal("scanFlow is nil")
	}
	if taskPool == nil {
		t.Fatal("taskPool is nil")
	}
	scanFlow.ScanEvents(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	quietSince := time.Time{}
	for time.Now().Before(deadline) {
		drainedTrigger := false
		for {
			select {
			case <-scanWorker.TriggerChan():
				drainedTrigger = true
				scanFlow.ScanEvents(context.Background())
			default:
				goto checkIdle
			}
		}

	checkIdle:
		heightCount, hashCount := taskPool.HeaderSyncCounts()
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
	fetchByHeightFn func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson
	fetchByHashFn   func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson
	fetchFullFn     func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock
}

func (m *mockBlockFetcher) FetchBlockHeaderByHeight(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
	if m != nil && m.fetchByHeightFn != nil {
		return m.fetchByHeightFn(ctx, nodeOp, taskID, height)
	}
	return nil
}

func (m *mockBlockFetcher) FetchBlockHeaderByHash(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson {
	if m != nil && m.fetchByHashFn != nil {
		return m.fetchByHashFn(ctx, nodeOp, taskID, hash)
	}
	return nil
}

func (m *mockBlockFetcher) FetchFullBlock(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
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
		fm.nodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	}
	// Tests need to set an absolute tip height. Production RemoteChain.Update ignores
	// regressive heights; reset node 0's view so each helper call is authoritative.
	fm.nodeManager.ResetNodeRemoteTip(0)
	fm.nodeManager.UpdateNodeChainInfo(0, height, "")
}

func setTestScanStartHeight(fm *FetchManager, height uint64) {
	if fm == nil {
		return
	}
	fm.scanConfig.StartHeight = height
	if scanFlow := fm.runtimeScanFlow(); scanFlow != nil {
		scanFlow.BindRuntimeDeps()
	}
}

func setTestBlockFetcher(fm *FetchManager, fetcher fetcherpkg.BlockFetcher) {
	if fm == nil {
		return
	}
	fm.blockFetcher = fetcher
}
