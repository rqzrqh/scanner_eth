package scan

import (
	"context"
	"strconv"

	fetcherpkg "scanner_eth/fetch/fetcher"

	"github.com/sirupsen/logrus"
)

func (sf *Flow) RunExpandTreeStage(ctx context.Context) {
	if !sf.canRunScanStage(ctx) {
		return
	}
	sf.inspectBlockTreeState("before_expand_tree")
	sf.enqueueExpandTreeTargets(sf.GetExpandTreeTargets())
}

func (sf *Flow) latestRemoteHeight() uint64 {
	if sf == nil {
		return 0
	}
	return sf.nodeManager.GetLatestHeight()
}

func (sf *Flow) bootstrapHeaderByHeight(ctx context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
	if sf == nil {
		return nil
	}
	for _, nodeOp := range sf.nodeManager.NodeOperators() {
		if header := sf.fetcher.FetchBlockHeaderByHeight(ctx, nodeOp, 0, height); header != nil {
			return header
		}
	}
	return nil
}

func (sf *Flow) GetExpandTreeTargets() []uint64 {
	return sf.collectExpandTreeTargets()
}

func (sf *Flow) GetHeaderByHeightSyncTargets() []uint64 {
	return sf.GetExpandTreeTargets()
}

func (sf *Flow) collectExpandTreeTargets() []uint64 {
	if sf == nil {
		return nil
	}
	start, end, hasRange := sf.blockTree.HeightRange()
	if !hasRange {
		if sf.taskPool.IsHeaderHeightSyncing(sf.startHeight) {
			return nil
		}
		return []uint64{sf.startHeight}
	}

	targetSize, ok := sf.HeaderWindowTargetSize()
	if !ok || sf.ShouldStopHeaderWindowSync(start, end, targetSize) {
		return nil
	}
	window := end - start + 1
	if window >= targetSize {
		return nil
	}
	maxEnd := end + (targetSize - window)
	latestRemote := sf.latestRemoteHeight()
	if latestRemote == 0 {
		return nil
	}
	if maxEnd > latestRemote {
		maxEnd = latestRemote
	}
	if maxEnd <= end {
		return nil
	}

	heights := make([]uint64, 0, maxEnd-end)
	for h := end + 1; h <= maxEnd; h++ {
		if sf.taskPool.IsHeaderHeightSyncing(h) {
			continue
		}
		heights = append(heights, h)
	}
	return heights
}

func (sf *Flow) enqueueExpandTreeTargets(heights []uint64) {
	if sf == nil || sf.taskPool == nil {
		return
	}
	for _, height := range heights {
		if !sf.taskPool.EnqueueHeaderHeightTask(height) {
			logrus.Warnf("enqueue header-by-height task failed. height:%v", height)
		}
	}
}

func (sf *Flow) SyncHeaderByHeightTarget(_ context.Context, target string) (bool, string) {
	return sf.SyncExpandTreeTarget(target)
}

func (sf *Flow) SyncExpandTreeTarget(target string) (bool, string) {
	height, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return false, "invalid header-by-height target"
	}
	if sf.taskRuntime.FetchAndInsertHeaderByHeight(height) == nil {
		return false, "header-by-height fetch failed"
	}
	return true, ""
}

func (sf *Flow) EnsureBootstrapHeader() bool {
	sf.BindRuntimeDeps()
	if sf == nil {
		return false
	}
	if _, _, ok := sf.blockTree.HeightRange(); ok {
		return true
	}
	height := sf.startHeight
	header := sf.bootstrapHeaderByHeight(context.Background(), height)
	if header == nil {
		logrus.Warnf("bootstrap get header failed from all nodes. height:%v", height)
		return false
	}
	sf.taskRuntime.InsertTreeHeader(header)
	logrus.Infof("bootstrap blocktree root by startHeight success. height:%v hash:%v", height, sf.normalize(header.Hash))
	return true
}

func (sf *Flow) SyncHeaderWindow() {
	sf.ExpandTreeWindow()
}

func (sf *Flow) ExpandTreeWindow() {
	sf.BindRuntimeDeps()
	if sf == nil {
		return
	}
	targetSize, ok := sf.HeaderWindowTargetSize()
	if !ok {
		return
	}
	for {
		start, end, hasRange := sf.blockTree.HeightRange()
		if !hasRange {
			height := sf.startHeight
			header := sf.taskRuntime.FetchAndInsertHeaderByHeight(height)
			if header == nil {
				return
			}
			logrus.Infof("sync header window bootstrap by startHeight success. height:%v hash:%v", height, sf.normalize(header.Hash))
			continue
		}
		if sf.ShouldStopHeaderWindowSync(start, end, targetSize) {
			return
		}
		if sf.taskRuntime.FetchAndInsertHeaderByHeight(end+1) == nil {
			return
		}
	}
}

func (sf *Flow) HeaderWindowTargetSize() (uint64, bool) {
	sf.BindRuntimeDeps()
	if sf == nil || sf.irreversible <= 0 {
		return 0, false
	}
	targetSize := uint64(2 * sf.irreversible)
	return targetSize, targetSize != 0
}

func (sf *Flow) ShouldStopHeaderWindowSync(start, end, targetSize uint64) bool {
	if sf == nil {
		return true
	}
	window := end - start + 1
	if window >= targetSize {
		return true
	}
	latestRemote := sf.latestRemoteHeight()
	return latestRemote == 0 || end >= latestRemote
}
