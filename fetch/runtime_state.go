package fetch

import (
	"scanner_eth/blocktree"
	headernotify "scanner_eth/fetch/header_notify"
	fetchscan "scanner_eth/fetch/scan"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/task"
)

type fetchRuntimeState struct {
	blockTree           *blocktree.BlockTree
	storedBlocks        *fetchstore.StoredBlockState
	pendingPayloadStore *fetchstore.PayloadStore
	taskPool            *fetchtask.Pool
	scanFlow            *fetchscan.Flow
	headerManager       *headernotify.Manager
	scanWorker          *fetchscan.Worker
	storeWorker         *fetchstore.SerialWorker[*EventBlockData]
}

func newFetchRuntimeState(irreversibleBlocks int) *fetchRuntimeState {
	stored := fetchstore.NewStoredBlockState()
	taskPoolState := fetchtask.Pool{}
	return &fetchRuntimeState{
		blockTree:           blocktree.NewBlockTree(irreversibleBlocks),
		storedBlocks:        &stored,
		pendingPayloadStore: fetchstore.NewPayloadStore(),
		taskPool:            &taskPoolState,
	}
}

func (fm *FetchManager) currentRuntime() *fetchRuntimeState {
	if fm == nil {
		return nil
	}
	if fm.runtime == nil {
		return &fetchRuntimeState{
			blockTree:           fm.blockTree,
			storedBlocks:        fm.storedBlocks,
			pendingPayloadStore: fm.pendingPayloadStore,
			taskPool:            fm.taskPool,
			scanFlow:            fm.scanFlow,
			headerManager:       fm.headerManager,
			scanWorker:          fm.scanWorker,
			storeWorker:         fm.storeWorker,
		}
	}
	return fm.runtime
}

func (fm *FetchManager) syncRuntimeFields() {
	if fm == nil {
		return
	}
	if fm.runtime == nil {
		fm.blockTree = nil
		sb := fetchstore.NewStoredBlockState()
		fm.storedBlocks = &sb
		fm.pendingPayloadStore = nil
		tp := fetchtask.Pool{}
		fm.taskPool = &tp
		fm.scanFlow = nil
		fm.headerManager = nil
		fm.scanWorker = nil
		fm.storeWorker = nil
		return
	}
	rt := fm.runtime
	fm.blockTree = rt.blockTree
	fm.storedBlocks = rt.storedBlocks
	fm.pendingPayloadStore = rt.pendingPayloadStore
	fm.taskPool = rt.taskPool
	fm.scanFlow = rt.scanFlow
	fm.headerManager = rt.headerManager
	fm.scanWorker = rt.scanWorker
	fm.storeWorker = rt.storeWorker
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

func (fm *FetchManager) runtimePayloadStore() *fetchstore.PayloadStore {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.pendingPayloadStore
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

func (fm *FetchManager) runtimeStoreWorker() *fetchstore.SerialWorker[*EventBlockData] {
	rt := fm.currentRuntime()
	if rt == nil {
		return nil
	}
	return rt.storeWorker
}

func (fm *FetchManager) createRuntimeState() {
	fm.stopRuntimeWorkers()
	fm.nodeManager.SetAllNodesIdle()
	fm.nodeManager.ResetRemoteChainTips()
	rt := newFetchRuntimeState(fm.irreversibleBlocks)
	fm.runtime = rt
	fm.syncRuntimeFields()
	rt.scanFlow = fm.newScanFlow()
	rt.scanWorker = fm.newScanWorker(rt.scanFlow)
	fm.syncRuntimeFields()
	rt.storeWorker = fm.newStoreWorker()
	fm.syncRuntimeFields()
	*rt.taskPool = fetchtask.NewTaskPoolWithStop(fm.taskPoolOptions, fm.nodeManager.NodeCount(), func(task *fetchtask.SyncTask, stopCh <-chan struct{}) bool {
		return fetchscan.HandleTaskPoolTask(rt.scanFlow, task, stopCh)
	})
	fm.syncRuntimeFields()
	if rt.scanFlow != nil {
		rt.scanFlow.BindRuntimeDeps()
	}
	rt.headerManager = fm.newHeaderManager()
	fm.syncRuntimeFields()
	fm.syncRuntimeFields()
}

func (fm *FetchManager) deleteRuntimeState() {
	fm.stopRuntimeWorkers()
	fm.runtime = nil
	fm.syncRuntimeFields()
}
