package fetch

import (
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	headernotify "scanner_eth/fetch/header_notify"
	fetchstore "scanner_eth/fetch/store"
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"

	nodepkg "scanner_eth/fetch/node"
	fetchscan "scanner_eth/fetch/scan"
	fetchtaskprocess "scanner_eth/fetch/task_process"
	fetchtask "scanner_eth/fetch/taskpool"
)

func newTestFetchManager(t *testing.T, irreversible int) *FetchManager {
	t.Helper()
	fm := &FetchManager{
		runtime:            newFetchRuntimeState(irreversible),
		irreversibleBlocks: irreversible,
		scanConfig: fetchscan.Config{
			StartHeight: 1,
		},
		dbOperator: &mockDbOperator{},
		fetcher:    fetcherpkg.NewMockFetcher(nil, nil, nil),
	}
	fm.nodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	rt := fm.currentRuntime()
	flow := fetchscan.NewFlow(fm.scanFlowRuntimeDeps, fetchscan.Config{StartHeight: fm.scanConfig.StartHeight})
	rt.scanFlow = flow
	rt.scanWorker = fm.newScanWorker(flow)
	pool := fetchtask.NewTaskPoolWithStop(
		fetchtask.TaskPoolOptions{WorkerCount: 1, HighQueueSize: 64, NormalQueueSize: 64, MaxRetry: 2},
		1,
		func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
			taskRuntime := newTaskProcessRuntimeDeps(
				rt.blockTree,
				rt.stagingStore,
				rt.taskPool,
				fm.nodeManager,
				fm.fetcher,
			)
			return fetchtask.DispatchSyncTask(
				task,
				stopCh,
				func(hash string, stopCh <-chan struct{}) bool {
					return fetchtaskprocess.HandleBodyTask(taskRuntime, hash, stopCh)
				},
				func(hash string) bool {
					return fetchtaskprocess.HandleHeaderHashTask(taskRuntime, hash)
				},
				func(height uint64) bool {
					return fetchtaskprocess.HandleHeaderHeightTask(taskRuntime, height)
				},
			)
		},
	)
	rt.taskPool = &pool
	rt.storeWorker = fm.newStoreWorker()
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

func mustTestRuntime(t *testing.T, fm *FetchManager) *fetchRuntimeState {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	rt := fm.runtime
	if rt == nil {
		t.Fatal("runtime is nil")
	}
	return rt
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

func mustTestStagingStore(t *testing.T, fm *FetchManager) *fetchstore.StagingStore {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	stagingStore := fm.runtimeStagingStore()
	if stagingStore == nil {
		t.Fatal("stagingStore is nil")
	}
	return stagingStore
}

func mustTestTaskPool(t *testing.T, fm *FetchManager) *fetchtask.Pool {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	taskPool := fm.runtimeTaskPool()
	if taskPool == nil {
		t.Fatal("taskPool is nil")
	}
	return taskPool
}

func mustTestStoredBlocks(t *testing.T, fm *FetchManager) *fetchstore.StoredBlockState {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	storedBlocks := fm.runtimeStoredBlocks()
	if storedBlocks == nil {
		t.Fatal("storedBlocks is nil")
	}
	return storedBlocks
}

func mustTestBlockTree(t *testing.T, fm *FetchManager) *blocktree.BlockTree {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	blockTree := fm.runtimeBlockTree()
	if blockTree == nil {
		t.Fatal("blockTree is nil")
	}
	return blockTree
}

func getTestNodeBlockHeader(t *testing.T, fm *FetchManager, hash string) *fetcherpkg.BlockHeaderJson {
	t.Helper()
	return mustTestStagingStore(t, fm).GetPendingHeader(hash)
}

func setTestDbOperator(
	fm *FetchManager,
	dbOperator fetchstore.DBOperator,
) {
	if fm == nil {
		return
	}
	fm.dbOperator = dbOperator
	if storeWorker := fm.runtimeStoreWorker(); storeWorker != nil {
		storeWorker.SetDBOperator(dbOperator)
	}
}
