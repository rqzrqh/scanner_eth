package fetch

import (
	headernotify "scanner_eth/fetch/header_notify"
	fetchscan "scanner_eth/fetch/scan"
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
	return fetchscan.NewWorker(fetchscan.RunnerFunc(flow.ScanEvents))
}

func (fm *FetchManager) newHeaderManager() *headernotify.Manager {
	if scanFlow := fm.runtimeScanFlow(); scanFlow != nil {
		scanFlow.BindRuntimeDeps()
	}
	return headernotify.NewManager(fm.hns, func(update *headernotify.RemoteChainUpdate) {
		if update == nil {
			return
		}
		fm.nodeManager.UpdateNodeChainInfo(update.NodeId, update.Height, update.BlockHash)
		runtime := fm.currentRuntime()
		if runtime != nil && update.BlockHash != "" && runtime.scanFlow != nil && runtime.scanWorker != nil && runtime.scanWorker.IsEnabled() {
			runtime.scanFlow.FetchAndInsertHeaderByHashImmediate(normalizeHash(update.BlockHash))
		}
		if runtime != nil && runtime.scanWorker != nil {
			runtime.scanWorker.Trigger()
		}
	})
}

func (fm *FetchManager) newStoreWorker() *fetchstore.SerialWorker[*EventBlockData] {
	worker := fetchstore.NewSerialWorker(fm.dbOperator, fm.runtimeStoredBlocks(), func(data *EventBlockData) bool {
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
	if rt.headerManager != nil {
		rt.headerManager.Stop()
	}
	if rt.scanWorker != nil {
		rt.scanWorker.Stop()
	}
	if rt.storeWorker != nil {
		rt.storeWorker.Stop()
	}
}
