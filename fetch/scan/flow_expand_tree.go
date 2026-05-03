package scan

import (
	"context"
	"strconv"
	"strings"
	"time"

	fetcherpkg "scanner_eth/fetch/fetcher"

	"github.com/sirupsen/logrus"
)

func (sf *Flow) RunExpandTreeStage(ctx context.Context) {
	startedAt := time.Now()
	if !sf.canRunScanStage(ctx) {
		if sf != nil {
			sf.logScanStageEvent(scanStageEvent{stage: scanStageExpandTree, success: false, duration: time.Since(startedAt), errMsg: "scan stage unavailable"})
		}
		return
	}
	sf.inspectBlockTreeState("before_expand_tree")
	targets := sf.GetExpandTreeTargets()
	sf.enqueueExpandTreeTargets(targets)
	sf.logScanStageEvent(scanStageEvent{
		stage:       scanStageExpandTree,
		target:      formatUintTargets(targets),
		targetCount: len(targets),
		success:     true,
		duration:    time.Since(startedAt),
	})
}

func formatUintTargets(targets []uint64) string {
	if len(targets) == 0 {
		return ""
	}
	values := make([]string, 0, len(targets))
	for _, target := range targets {
		values = append(values, strconv.FormatUint(target, 10))
	}
	return strings.Join(values, ",")
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
	sf.MarkRootParentReady()
	logrus.Infof("bootstrap blocktree root by startHeight success. height:%v hash:%v", height, sf.normalize(header.Hash))
	return true
}

func (sf *Flow) MarkRootParentReady() bool {
	if sf == nil || sf.blockTree == nil || sf.storedBlocks == nil {
		return false
	}
	root := sf.blockTree.Root()
	if root == nil {
		return false
	}
	parentHash := sf.normalize(root.ParentKey)
	if parentHash == "" {
		return false
	}
	sf.storedBlocks.MarkStored(parentHash)
	logrus.Infof("mark blocktree root parent ready. root_height:%v root_hash:%v parent_hash:%v", root.Height, sf.normalize(root.Key), parentHash)
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
			sf.MarkRootParentReady()
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
