package fetch

import (
	"context"
	"scanner_eth/blocktree"
	"strconv"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
)

type scanStage int

const (
	scanStageHeaderByHeightDone scanStage = iota + 1
	scanStageHeaderByHashDone
	scanStageBodyDone
)

type scanStageEvent struct {
	stage    scanStage
	target   string
	success  bool
	duration time.Duration
	errMsg   string
}

func scanStageName(stage scanStage) string {
	switch stage {
	case scanStageHeaderByHeightDone:
		return "header_by_height"
	case scanStageHeaderByHashDone:
		return "header_by_hash"
	case scanStageBodyDone:
		return "body"
	default:
		return "unknown"
	}
}

type branchProcessState struct {
	hash        string
	parentReady bool
}

func (fm *FetchManager) scanEvents(ctx context.Context) {
	if !fm.runScanCoordinator(ctx) {
		return
	}
	fm.pruneStoredBlocks()
}

func (fm *FetchManager) runScanCoordinator(ctx context.Context) bool {
	if ctx.Err() != nil {
		return false
	}

	fm.inspectBlockTreeState("before_header_sync")
	headerHeights := fm.getHeaderByHeightSyncTargets()
	for _, height := range headerHeights {
		if !fm.taskPool.enqueueHeaderHeightTask(height) {
			logrus.Warnf("enqueue header-by-height task failed. height:%v", height)
		}
	}

	headerHashes := fm.getHeaderByHashSyncTargets()
	for _, hash := range headerHashes {
		if !fm.taskPool.enqueueHeaderHashTask(hash) {
			logrus.Warnf("enqueue header-by-hash task failed. hash:%v", normalizeHash(hash))
		}
	}

	bodyTargets := fm.getBodySyncTargets()
	if len(bodyTargets) > 0 {
		fm.runScanStageAsync(scanStageBodyDone, strings.Join(bodyTargets, ","), fm.syncBodyTarget, func(event scanStageEvent) {
			fm.logScanStageEvent(event)
			if event.success {
				fm.inspectBlockTreeState("after_body_sync")
			}
		})
	}

	return true
}

func (fm *FetchManager) handleTaskPoolTask(task *syncTask, stopCh <-chan struct{}) bool {
	if fm == nil {
		return false
	}
	if task == nil {
		return true
	}
	select {
	case <-stopCh:
		return false
	default:
	}

	if task.kind == syncTaskKindBody || (task.kind == 0 && task.hash != "") {
		if task.hash == "" {
			return true
		}
		select {
		case <-stopCh:
			return false
		default:
		}
		success := fm.syncNodeDataByHash(task.hash)
		select {
		case <-stopCh:
			return false
		default:
		}
		if success {
			fm.triggerScan()
		}
		return success
	}

	startedAt := time.Now()
	if task.kind == syncTaskKindHeaderHash {
		select {
		case <-stopCh:
			return false
		default:
		}
		success := fm.fetchAndInsertHeaderByHashCore(task.hash)
		event := scanStageEvent{
			stage:    scanStageHeaderByHashDone,
			target:   normalizeHash(task.hash),
			success:  success,
			duration: time.Since(startedAt),
		}
		if !success {
			event.errMsg = "header-by-hash fetch failed"
		}
		fm.logScanStageEvent(event)
		if success {
			select {
			case <-stopCh:
				return false
			default:
			}
			fm.inspectBlockTreeState("after_header_sync_by_hash")
			fm.triggerScan()
		}
		return success
	}

	select {
	case <-stopCh:
		return false
	default:
	}
	header := fm.fetchAndInsertHeaderByHeightCore(task.height)
	event := scanStageEvent{
		stage:    scanStageHeaderByHeightDone,
		target:   strconv.FormatUint(task.height, 10),
		success:  header != nil,
		duration: time.Since(startedAt),
	}
	if header == nil {
		event.errMsg = "header-by-height fetch failed"
	}
	fm.logScanStageEvent(event)
	if header != nil {
		select {
		case <-stopCh:
			return false
		default:
		}
		fm.inspectBlockTreeState("after_header_sync_by_height")
		fm.triggerScan()
	}
	return header != nil
}

func (fm *FetchManager) runScanStageAsync(stage scanStage, target string, fn func(string) (bool, string), after func(scanStageEvent)) bool {
	go func() {
		startedAt := time.Now()
		success, errMsg := fn(target)
		event := scanStageEvent{stage: stage, target: target, success: success, duration: time.Since(startedAt), errMsg: errMsg}
		if after != nil {
			after(event)
		}
	}()

	return true
}

func (fm *FetchManager) getHeaderByHeightSyncTargets() []uint64 {
	start, end, hasRange := fm.blockTree.HeightRange()
	if !hasRange {
		if fm.taskPool.isHeaderHeightSyncing(fm.startHeight) {
			return nil
		}
		return []uint64{fm.startHeight}
	}

	targetSize, ok := fm.headerWindowTargetSize()
	if !ok || fm.shouldStopHeaderWindowSync(start, end, targetSize) {
		return nil
	}

	window := end - start + 1
	if window >= targetSize {
		return nil
	}

	maxEnd := end + (targetSize - window)
	latestRemote := fm.nodeManager.GetLatestHeight()
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
		if fm.taskPool.isHeaderHeightSyncing(h) {
			continue
		}
		heights = append(heights, h)
	}
	return heights
}

func (fm *FetchManager) getHeaderByHashSyncTargets() []string {
	hashes := make([]string, 0)
	for _, missingParent := range fm.blockTree.UnlinkedNodes() {
		hash := normalizeHash(missingParent)
		if fm.shouldSyncOrphanParent(hash) && !fm.taskPool.isHeaderHashSyncing(hash) {
			hashes = append(hashes, hash)
		}
	}
	return hashes
}

func (fm *FetchManager) getBodySyncTargets() []string {
	targets := make([]string, 0)
	branches := fm.blockTree.Branches()
	seen := make(map[string]struct{})
	for _, branch := range branches {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			node := branch.Nodes[i]
			if node == nil {
				continue
			}
			state, ok := fm.buildBranchProcessState(node)
			if !ok {
				continue
			}
			nodeData := fm.getNodeBlockBody(node.Key)
			if nodeData == nil {
				if _, exists := seen[state.hash]; !exists {
					seen[state.hash] = struct{}{}
					targets = append(targets, state.hash)
				}
				continue
			}
			if fm.hasStorableNodeData(nodeData) {
				if _, exists := seen[state.hash]; !exists {
					seen[state.hash] = struct{}{}
					targets = append(targets, state.hash)
				}
			}
		}
	}
	return targets
}

func (fm *FetchManager) syncHeaderByHeightTarget(target string) (bool, string) {
	height, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return false, "invalid header-by-height target"
	}
	header := fm.fetchAndInsertHeaderByHeight(height)
	if header == nil {
		return false, "header-by-height fetch failed"
	}
	return true, ""
}

func (fm *FetchManager) syncHeaderByHashTarget(target string) (bool, string) {
	hash := normalizeHash(target)
	if hash == "" {
		return false, "invalid header-by-hash target"
	}
	if !fm.fetchAndInsertHeaderByHash(hash) {
		return false, "header-by-hash fetch failed"
	}
	return true, ""
}

func (fm *FetchManager) syncBodyTarget(target string) (bool, string) {
	if strings.TrimSpace(target) == "" {
		return false, "invalid body target"
	}
	hashes := strings.Split(target, ",")
	for _, raw := range hashes {
		hash := normalizeHash(raw)
		if hash == "" {
			continue
		}
		node := fm.blockTree.Get(hash)
		if node == nil {
			continue
		}
		state, ok := fm.buildBranchProcessState(node)
		if !ok {
			continue
		}
		nodeData := fm.getNodeBlockBody(node.Key)
		if nodeData == nil {
			fm.enqueueNodeBodySync(state)
			continue
		}
		if !state.parentReady || !fm.hasStorableNodeData(nodeData) {
			continue
		}
		fm.storeNodeBodyData(node, nodeData, state)
	}
	return true, ""
}

func (fm *FetchManager) countActionableBodyNodes() int {
	count := 0
	branches := fm.blockTree.Branches()
	for _, branch := range branches {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			node := branch.Nodes[i]
			if node == nil {
				continue
			}

			state, ok := fm.buildBranchProcessState(node)
			if !ok {
				continue
			}

			nodeData := fm.getNodeBlockBody(node.Key)
			if nodeData == nil {
				count++
				continue
			}

			if !state.parentReady {
				continue
			}

			if fm.hasStorableNodeData(nodeData) {
				count++
			}
		}
	}
	return count
}

func (fm *FetchManager) countStoredLinkedNodes() int {
	count := 0
	linkedNodes := fm.blockTree.LinkedNodes()
	for _, node := range linkedNodes {
		if node == nil {
			continue
		}
		if fm.storedBlocks.IsStored(node.Key) {
			count++
		}
	}
	return count
}

func (fm *FetchManager) logScanStageEvent(event scanStageEvent) {
	if event.success {
		logrus.Infof("scan stage event stage:%v target:%v success:%v duration:%v", event.stage, event.target, event.success, event.duration)
		return
	}
	logrus.Warnf("scan stage event stage:%v target:%v success:%v duration:%v err:%v", event.stage, event.target, event.success, event.duration, event.errMsg)
}

func (fm *FetchManager) inspectBlockTreeState(stage string) {
	start, end, ok := fm.blockTree.HeightRange()
	if !ok {
		logrus.Infof("scan stage:%s blocktree empty", stage)
		return
	}

	unlinked := len(fm.blockTree.UnlinkedNodes())
	logrus.Infof("scan stage:%s blocktree range:[%v,%v] unlinked:%v", stage, start, end, unlinked)
}

func (fm *FetchManager) ensureBootstrapHeader() bool {
	_, _, ok := fm.blockTree.HeightRange()
	if ok {
		return true
	}

	height := fm.startHeight
	success := false
	clients := fm.nodeManager.Clients()
	for i, client := range clients {
		header := fm.blockFetcher.FetchBlockHeaderByHeight(i, 0, client, height)
		if header == nil {
			continue
		}
		fm.insertHeader(header)
		logrus.Infof("bootstrap blocktree root by startHeight success. height:%v hash:%v node:%v", height, normalizeHash(header.Hash), i)
		success = true
	}
	if !success {
		logrus.Warnf("bootstrap get header failed from all nodes. height:%v", height)
	}
	return success
}

func (fm *FetchManager) syncHeaderWindow() {
	targetSize, ok := fm.headerWindowTargetSize()
	if !ok {
		return
	}

	for {
		start, end, hasRange := fm.blockTree.HeightRange()
		if !hasRange {
			height := fm.startHeight
			header := fm.fetchAndInsertHeaderByHeight(height)
			if header == nil {
				return
			}
			logrus.Infof("sync header window bootstrap by startHeight success. height:%v hash:%v", height, normalizeHash(header.Hash))
			continue
		}
		if fm.shouldStopHeaderWindowSync(start, end, targetSize) {
			return
		}

		nextHeight := end + 1
		if fm.fetchAndInsertHeaderByHeight(nextHeight) == nil {
			return
		}
	}
}

func (fm *FetchManager) headerWindowTargetSize() (uint64, bool) {
	if fm.irreversibleBlocks <= 0 {
		return 0, false
	}

	targetSize := uint64(2 * fm.irreversibleBlocks)
	if targetSize == 0 {
		return 0, false
	}

	return targetSize, true
}

func (fm *FetchManager) shouldStopHeaderWindowSync(start uint64, end uint64, targetSize uint64) bool {
	window := end - start + 1
	if window >= targetSize {
		return true
	}

	latestRemote := fm.nodeManager.GetLatestHeight()
	return latestRemote == 0 || end >= latestRemote
}

func (fm *FetchManager) fetchAndInsertHeaderByHeight(height uint64) *BlockHeaderJson {
	if !fm.taskPool.tryStartHeaderHeightSync(height) {
		return nil
	}
	defer fm.taskPool.finishHeaderHeightSync(height)
	return fm.fetchAndInsertHeaderByHeightCore(height)
}

func (fm *FetchManager) fetchAndInsertHeaderByHeightCore(height uint64) *BlockHeaderJson {

	nodeID, client, err := fm.nodeManager.GetBestNode(height)
	if err != nil {
		return nil
	}

	header := fm.blockFetcher.FetchBlockHeaderByHeight(nodeID, 0, client, height)
	if header == nil {
		return nil
	}

	fm.insertHeader(header)
	return header
}

func (fm *FetchManager) syncOrphanParents() {
	for _, missingParent := range fm.blockTree.UnlinkedNodes() {
		hash := normalizeHash(missingParent)
		if !fm.shouldSyncOrphanParent(hash) {
			continue
		}
		fm.fetchAndInsertHeaderByHash(hash)
	}
}

func (fm *FetchManager) shouldSyncOrphanParent(hash string) bool {
	if hash == "" {
		return false
	}
	return fm.blockTree.Get(hash) == nil
}

func (fm *FetchManager) fetchAndInsertHeaderByHash(hash string) bool {
	hash = normalizeHash(hash)
	if hash == "" {
		return false
	}
	if !fm.taskPool.tryStartHeaderHashSync(hash) {
		return false
	}
	defer fm.taskPool.finishHeaderHashSync(hash)
	return fm.fetchAndInsertHeaderByHashCore(hash)
}

func (fm *FetchManager) fetchAndInsertHeaderByHashCore(hash string) bool {

	nodeID, client, err := fm.nodeManager.GetBestNode(0)
	if err != nil {
		return false
	}

	header := fm.blockFetcher.FetchBlockHeaderByHash(nodeID, 0, client, hash)
	if header == nil {
		return false
	}

	fm.insertHeader(header)
	return true
}
func (fm *FetchManager) processBranchesLowToHigh() {
	branches := fm.blockTree.Branches()
	for _, branch := range branches {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			fm.processBranchNode(branch.Nodes[i])
		}
	}
}

func (fm *FetchManager) processBranchNode(node *blocktree.LinkedNode) {
	if node == nil {
		return
	}

	state, ok := fm.buildBranchProcessState(node)
	if !ok {
		return
	}

	nodeData := fm.getNodeBlockBody(node.Key)
	if nodeData == nil {
		fm.enqueueNodeBodySync(state)
		return
	}

	fm.storeNodeBodyData(node, nodeData, state)
}

func (fm *FetchManager) buildBranchProcessState(node *blocktree.LinkedNode) (branchProcessState, bool) {
	hash := normalizeHash(node.Key)
	if hash == "" || fm.storedBlocks.IsStored(hash) {
		return branchProcessState{}, false
	}

	parentHash := normalizeHash(node.ParentKey)
	parentNode := fm.blockTree.Get(parentHash)
	return branchProcessState{
		hash:        hash,
		parentReady: parentHash == "" || parentNode == nil || fm.storedBlocks.IsStored(parentHash),
	}, true
}

func (fm *FetchManager) enqueueNodeBodySync(state branchProcessState) {
	priority := taskPriorityNormal
	if state.parentReady {
		priority = taskPriorityHigh
	}
	fm.taskPool.enqueueTaskWithPriority(state.hash, priority)
}

func (fm *FetchManager) storeNodeBodyData(node *blocktree.LinkedNode, nodeData *EventBlockData, state branchProcessState) {
	if !fm.hasStorableNodeData(nodeData) {
		return
	}
	blockData := nodeData

	if !state.parentReady {
		return
	}

	if err := fm.dbOperator.StoreBlockData(blockData); err != nil {
		logrus.Warnf("store block failed. height:%v hash:%v err:%v", node.Height, state.hash, err)
		return
	}
	fm.storedBlocks.MarkStored(state.hash)
}

func (fm *FetchManager) hasStorableNodeData(nodeData *EventBlockData) bool {
	if nodeData == nil {
		return false
	}
	return nodeData.StorageFullBlock != nil && nodeData.ProtocolFullBlock != nil
}
