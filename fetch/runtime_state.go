package fetch

import (
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	headernotify "scanner_eth/fetch/header_notify"
	nodepkg "scanner_eth/fetch/node"
	fetchscan "scanner_eth/fetch/scan"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtaskprocess "scanner_eth/fetch/task_process"
	fetchtask "scanner_eth/fetch/taskpool"
)

type fetchRuntimeState struct {
	blockTree     *blocktree.BlockTree
	storedBlocks  *fetchstore.StoredBlockState
	stagingStore  *fetchstore.StagingStore
	taskPool      *fetchtask.Pool
	scanFlow      *fetchscan.Flow
	headerManager *headernotify.Manager
	scanWorker    *fetchscan.Worker
	storeWorker   *fetchserialstore.Worker
}

func newTaskProcessRuntimeDeps(
	blockTree *blocktree.BlockTree,
	stagingStore *fetchstore.StagingStore,
	taskPool *fetchtask.Pool,
	nodeManager *nodepkg.NodeManager,
	fetcher fetcherpkg.Fetcher,
) fetchtaskprocess.RuntimeDeps {
	var tryClaimHeaderHeight func(uint64) bool
	var releaseHeaderHeight func(uint64)
	var tryClaimHeaderHash func(string) bool
	var releaseHeaderHash func(string)
	if taskPool != nil {
		tryClaimHeaderHeight = taskPool.TryStartHeaderHeightSync
		releaseHeaderHeight = taskPool.FinishHeaderHeightSync
		tryClaimHeaderHash = taskPool.TryStartHeaderHashSync
		releaseHeaderHash = taskPool.FinishHeaderHashSync
	}
	return fetchtaskprocess.NewRuntimeDeps(
		blockTree,
		stagingStore,
		nodeManager,
		fetcher,
		func(hash string) {
			if taskPool == nil {
				return
			}
			taskPool.EnqueueTaskWithPriority(hash, fetchtask.TaskPriorityHigh)
		},
		tryClaimHeaderHeight,
		releaseHeaderHeight,
		tryClaimHeaderHash,
		releaseHeaderHash,
	)
}

func newFetchRuntimeState(irreversibleBlocks int) *fetchRuntimeState {
	stored := fetchstore.NewStoredBlockState()
	taskPoolState := fetchtask.Pool{}
	return &fetchRuntimeState{
		blockTree:    blocktree.NewBlockTree(irreversibleBlocks),
		storedBlocks: &stored,
		stagingStore: fetchstore.NewStagingStore(),
		taskPool:     &taskPoolState,
	}
}

func (fm *FetchManager) currentRuntime() *fetchRuntimeState {
	if fm == nil {
		return nil
	}
	return fm.runtime
}

func (fm *FetchManager) runtimeTaskPool() *fetchtask.Pool {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.taskPool
}

func (fm *FetchManager) runtimeStoredBlocks() *fetchstore.StoredBlockState {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.storedBlocks
}

func (fm *FetchManager) runtimeBlockTree() *blocktree.BlockTree {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.blockTree
}

func (fm *FetchManager) runtimeStagingStore() *fetchstore.StagingStore {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.stagingStore
}

func (fm *FetchManager) runtimeHeaderManager() *headernotify.Manager {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.headerManager
}

func (fm *FetchManager) runtimeScanWorker() *fetchscan.Worker {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.scanWorker
}

func (fm *FetchManager) runtimeScanFlow() *fetchscan.Flow {
	rt := fm.currentRuntime()
	if rt != nil {
		return rt.scanFlow
	}
	return nil
}

func (fm *FetchManager) runtimeStoreWorker() *fetchserialstore.Worker {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.storeWorker
}

func (fm *FetchManager) createRuntimeState() {
	fm.releaseLeaderRuntime(true)
	fm.nodeManager.SetAllNodesIdle()
	fm.nodeManager.ResetRemoteChainTips()
	rt := newFetchRuntimeState(fm.irreversibleBlocks)
	fm.runtime = rt
	rt.scanFlow = fm.newScanFlow()
	rt.scanWorker = fm.newScanWorker(rt.scanFlow)
	rt.storeWorker = fm.newStoreWorker()
	*rt.taskPool = fetchtask.NewTaskPoolWithStop(fm.taskPoolOptions, fm.nodeManager.NodeCount(), func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
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
	})
	rt.scanFlow.BindRuntimeDeps()
	rt.headerManager = fm.newHeaderManager()
}

func (fm *FetchManager) deleteRuntimeState() {
	fm.runtime = nil
}
