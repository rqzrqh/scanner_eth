package fetch

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	headernotify "scanner_eth/fetch/header_notify"
	fetchscan "scanner_eth/fetch/scan"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/util"
)

func (fm *FetchManager) newScanFlow() *fetchscan.Flow {
	if fm == nil {
		return nil
	}
	return fetchscan.NewFlow(fm.scanFlowRuntimeDeps, fetchscan.Config{StartHeight: fm.scanConfig.StartHeight})
}

func (fm *FetchManager) newScanWorker(flow *fetchscan.Flow) *fetchscan.Worker {
	if flow == nil {
		return nil
	}
	flow.BindRuntimeDeps()
	return fetchscan.NewWorker(flow)
}

func (fm *FetchManager) newHeaderManager() *headernotify.Manager {
	if fm == nil {
		return headernotify.NewManager(nil, nil)
	}
	rt := fm.currentRuntime()
	if rt == nil {
		return headernotify.NewManager(nil, nil)
	}
	rt.scanFlow.BindRuntimeDeps()
	handleUpdate := func(update *headernotify.RemoteChainUpdate) {
		if update == nil {
			return
		}
		fm.nodeManager.UpdateNodeChainInfo(update.NodeId, update.Height, update.BlockHash)
		runtime := fm.currentRuntime()
		if runtime != nil && update.BlockHash != "" && runtime.scanWorker.IsEnabled() {
			taskRuntime := newTaskProcessRuntimeDeps(
				runtime.blockTree,
				runtime.stagingStore,
				runtime.taskPool,
				fm.nodeManager,
				fm.fetcher,
			)
			// newHeads provides enough header fields to extend the block tree,
			// but it must not populate pending block data because tx hashes are absent.
			if update.Header != nil {
				taskRuntime.InsertTreeHeader(&fetcherpkg.BlockHeaderJson{
					Number:     update.Header.Number,
					Hash:       update.Header.Hash,
					ParentHash: update.Header.ParentHash,
					Difficulty: update.Header.Difficulty,
				})
			} else {
				_ = taskRuntime.SyncHeaderByHash(context.Background(), util.NormalizeHash(update.BlockHash))
			}
		}
		if runtime != nil {
			runtime.scanWorker.Trigger()
		}
	}
	return headernotify.NewManager(fm.nodeManager.EthClients(), handleUpdate)
}

func (fm *FetchManager) newStoreWorker() *fetchserialstore.Worker {
	worker := fetchserialstore.NewStartedWorker(fm.dbOperator, fm.runtimeStoredBlocks(), func(data *fetchstore.EventBlockData) bool {
		return data == nil || data.StorageFullBlock == nil
	})
	return worker
}

func (fm *FetchManager) stopRuntimeWorkers() {
	if fm == nil {
		return
	}
	rt := fm.currentRuntime()
	if rt == nil {
		return
	}
	rt.headerManager.Stop()
	rt.scanWorker.Stop()
	rt.storeWorker.Stop()
}
