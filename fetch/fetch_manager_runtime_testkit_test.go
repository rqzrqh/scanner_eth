package fetch

import (
	"scanner_eth/blocktree"
	headernotify "scanner_eth/fetch/header_notify"
	fetchstore "scanner_eth/fetch/store"
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"

	nodepkg "scanner_eth/fetch/node"
	fetchscan "scanner_eth/fetch/scan"
	fetchtask "scanner_eth/fetch/task"
)

func newTestFetchManager(t *testing.T, irreversible int) *FetchManager {
	t.Helper()
	stored := fetchstore.NewStoredBlockState()
	fm := &FetchManager{
		blockTree:          blocktree.NewBlockTree(irreversible),
		pendingBlockStore:  fetchstore.NewPendingBlockStore(),
		storedBlocks:       &stored,
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
	pool := fetchtask.NewTaskPoolWithStop(
		fetchtask.TaskPoolOptions{WorkerCount: 1, HighQueueSize: 64, NormalQueueSize: 64, MaxRetry: 2},
		1,
		func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
			return fetchscan.HandleTaskPoolTask(flow, task, stopCh)
		},
	)
	fm.taskPool = &pool
	attachTestRuntime(fm)
	fm.storeWorker = fm.newStoreWorker()
	attachTestRuntime(fm)
	flow.BindRuntimeDeps()
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
	rt.storedBlocks = fm.storedBlocks
	rt.pendingBlockStore = fm.pendingBlockStore
	rt.taskPool = fm.taskPool
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

func mustTestPendingBlockStore(t *testing.T, fm *FetchManager) *fetchstore.PendingBlockStore {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	pendingStore := fm.runtimePendingBlockStore()
	if pendingStore == nil {
		t.Fatal("pendingBlockStore is nil")
	}
	return pendingStore
}

func getTestNodeBlockHeader(t *testing.T, fm *FetchManager, hash string) *BlockHeaderJson {
	t.Helper()
	return mustTestPendingBlockStore(t, fm).GetHeader(hash)
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
