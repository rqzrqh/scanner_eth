package fetch

import (
	headernotify "scanner_eth/fetch/header_notify"
	fetchscan "scanner_eth/fetch/scan"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
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
		if runtime != nil && update.BlockHash != "" && update.Header != nil && runtime.scanWorker.IsEnabled() {
			runtime.scanFlow.EnqueueRemoteHeaderCandidate(
				update.BlockHash,
				update.Header.Hash,
				update.Header.ParentHash,
				update.Header.Number,
			)
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
