package scan

import (
	"context"

	"github.com/sirupsen/logrus"
)

func (sf *Flow) RunFillTreeStage(ctx context.Context) {
	if !sf.canRunScanStage(ctx) {
		return
	}
	sf.enqueueFillTreeTargets(sf.GetFillTreeTargets())
}

func (sf *Flow) GetFillTreeTargets() []string {
	return sf.collectFillTreeTargets()
}

func (sf *Flow) GetHeaderByHashSyncTargets() []string {
	return sf.GetFillTreeTargets()
}

func (sf *Flow) collectFillTreeTargets() []string {
	if sf == nil {
		return nil
	}
	hashes := make([]string, 0)
	for _, missingParent := range sf.blockTree.UnlinkedNodes() {
		hash := sf.normalize(missingParent)
		if sf.ShouldSyncOrphanParent(hash) && !sf.taskPool.IsHeaderHashSyncing(hash) {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

func (sf *Flow) enqueueFillTreeTargets(hashes []string) {
	if sf == nil || sf.taskPool == nil {
		return
	}
	for _, hash := range hashes {
		if !sf.taskPool.EnqueueHeaderHashTask(hash) {
			logrus.Warnf("enqueue header-by-hash task failed. hash:%v", sf.normalize(hash))
		}
	}
}

func (sf *Flow) SyncHeaderByHashTarget(_ context.Context, target string) (bool, string) {
	return sf.SyncFillTreeTarget(target)
}

func (sf *Flow) SyncFillTreeTarget(target string) (bool, string) {
	hash := sf.normalize(target)
	if hash == "" {
		return false, "invalid header-by-hash target"
	}
	if !sf.taskRuntime.FetchAndInsertHeaderByHash(hash) {
		return false, "header-by-hash fetch failed"
	}
	return true, ""
}

func (sf *Flow) SyncOrphanParents() {
	sf.FillTreeMissingParents()
}

func (sf *Flow) FillTreeMissingParents() {
	if sf == nil {
		return
	}
	for _, missingParent := range sf.blockTree.UnlinkedNodes() {
		hash := sf.normalize(missingParent)
		if sf.ShouldSyncOrphanParent(hash) {
			sf.taskRuntime.FetchAndInsertHeaderByHash(hash)
		}
	}
}

func (sf *Flow) ShouldSyncOrphanParent(hash string) bool {
	return sf != nil && hash != "" && sf.blockTree.Get(hash) == nil
}
