package scan

import (
	"context"
	"scanner_eth/blocktree"
	"strconv"
	"strings"
	"time"

	fetchtask "scanner_eth/fetch/task"

	"github.com/sirupsen/logrus"
)

type TaskPool interface {
	EnqueueHeaderHeightTask(uint64) bool
	EnqueueHeaderHashTask(string) bool
	EnqueueBodyTask(string, int)
	IsHeaderHeightSyncing(uint64) bool
	TryStartHeaderHeightSync(uint64) bool
	FinishHeaderHeightSync(uint64)
	IsHeaderHashSyncing(string) bool
	TryStartHeaderHashSync(string) bool
	FinishHeaderHashSync(string)
}

type StoreWorker interface {
	IsInflight(string) bool
	Submit(context.Context, string, uint64, any) error
}

type StoredBlocks interface {
	IsStored(string) bool
}

type RuntimeDeps struct {
	StartHeight       uint64
	Irreversible      int
	BlockTree         *blocktree.BlockTree
	TaskPool          TaskPool
	StoreWorker       StoreWorker
	StoredBlocks      StoredBlocks
	TriggerScan       func()
	PruneStoredBlocks func(context.Context, int)

	SetNodeBlockHeader func(string, any) bool
	SetNodeBlockBody   func(string, any) bool
	GetNodeBlockHeader func(string) any
	GetNodeBlockBody   func(string) any

	LatestRemoteHeight      func() uint64
	BootstrapHeaderByHeight func(context.Context, uint64) any
	FetchHeaderByHeight     func(context.Context, uint64) any
	FetchHeaderByHash       func(context.Context, string) any
	FetchBodyByHash         func(context.Context, string, uint64, any) (body any, nodeID int, costMicros int64, ok bool)
	UpdateNodeState         func(int, int64, bool)

	NormalizeHash    func(string) string
	HeaderExists     func(any) bool
	HeaderHeight     func(any) (uint64, bool)
	HeaderHash       func(any) string
	HeaderParentHash func(any) string
	HeaderWeight     func(any) uint64

	BodyExists   func(any) bool
	BodyStorable func(any) bool
}

type scanStage int

const (
	scanStageHeaderByHeightDone scanStage = iota + 1
	scanStageHeaderByHashDone
	scanStageBodyDone
)

type ScanStageEvent struct {
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

type BranchProcessState struct {
	Hash        string
	ParentReady bool
}

type Flow struct {
	runtimeDepsFn func() RuntimeDeps

	startHeight       uint64
	irreversible      int
	blockTree         *blocktree.BlockTree
	taskPool          TaskPool
	storeWorker       StoreWorker
	storedBlocks      StoredBlocks
	triggerScan       func()
	pruneStoredBlocks func(context.Context, int)

	setNodeBlockHeader func(string, any) bool
	setNodeBlockBody   func(string, any) bool
	getNodeBlockHeader func(string) any
	getNodeBlockBody   func(string) any

	latestRemoteHeight      func() uint64
	bootstrapHeaderByHeight func(context.Context, uint64) any
	fetchHeaderByHeight     func(context.Context, uint64) any
	fetchHeaderByHash       func(context.Context, string) any
	fetchBodyByHash         func(context.Context, string, uint64, any) (body any, nodeID int, costMicros int64, ok bool)
	updateNodeState         func(int, int64, bool)

	normalizeHash    func(string) string
	headerExists     func(any) bool
	headerHeight     func(any) (uint64, bool)
	headerHash       func(any) string
	headerParentHash func(any) string
	headerWeight     func(any) uint64
	bodyExists       func(any) bool
	bodyStorable     func(any) bool
}

func NewFlow(runtimeDepsFn func() RuntimeDeps, config Config) *Flow {
	sf := &Flow{
		runtimeDepsFn: runtimeDepsFn,
		startHeight:   config.StartHeight,
	}
	sf.BindRuntimeDeps()
	return sf
}

func (sf *Flow) BindRuntimeDeps() {
	if sf == nil || sf.runtimeDepsFn == nil {
		return
	}
	deps := sf.runtimeDepsFn()
	if deps.StartHeight != 0 {
		sf.startHeight = deps.StartHeight
	}
	sf.irreversible = deps.Irreversible
	sf.blockTree = deps.BlockTree
	sf.taskPool = deps.TaskPool
	sf.storeWorker = deps.StoreWorker
	sf.storedBlocks = deps.StoredBlocks
	sf.triggerScan = deps.TriggerScan
	sf.pruneStoredBlocks = deps.PruneStoredBlocks
	sf.setNodeBlockHeader = deps.SetNodeBlockHeader
	sf.setNodeBlockBody = deps.SetNodeBlockBody
	sf.getNodeBlockHeader = deps.GetNodeBlockHeader
	sf.getNodeBlockBody = deps.GetNodeBlockBody
	sf.latestRemoteHeight = deps.LatestRemoteHeight
	sf.bootstrapHeaderByHeight = deps.BootstrapHeaderByHeight
	sf.fetchHeaderByHeight = deps.FetchHeaderByHeight
	sf.fetchHeaderByHash = deps.FetchHeaderByHash
	sf.fetchBodyByHash = deps.FetchBodyByHash
	sf.updateNodeState = deps.UpdateNodeState
	sf.normalizeHash = deps.NormalizeHash
	sf.headerExists = deps.HeaderExists
	sf.headerHeight = deps.HeaderHeight
	sf.headerHash = deps.HeaderHash
	sf.headerParentHash = deps.HeaderParentHash
	sf.headerWeight = deps.HeaderWeight
	sf.bodyExists = deps.BodyExists
	sf.bodyStorable = deps.BodyStorable
}

func (sf *Flow) normalize(v string) string {
	if sf == nil || sf.normalizeHash == nil {
		return strings.TrimSpace(v)
	}
	return sf.normalizeHash(v)
}

func (sf *Flow) hasHeader(v any) bool {
	return sf != nil && sf.headerExists != nil && sf.headerExists(v)
}

func (sf *Flow) hasBody(v any) bool {
	return sf != nil && sf.bodyExists != nil && sf.bodyExists(v)
}

func (sf *Flow) TriggerScan() {
	if sf == nil || sf.triggerScan == nil {
		return
	}
	sf.triggerScan()
}

func (sf *Flow) RunScanCycle(ctx context.Context) {
	sf.BindRuntimeDeps()
	if sf == nil || !sf.RunScanCoordinator(ctx) {
		return
	}
	if sf.irreversible > 0 && sf.pruneStoredBlocks != nil {
		sf.pruneStoredBlocks(ctx, sf.irreversible)
	}
}

func (sf *Flow) RunScanCoordinator(ctx context.Context) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if sf == nil || sf.taskPool == nil {
		return false
	}

	sf.inspectBlockTreeState("before_header_sync")
	for _, height := range sf.GetHeaderByHeightSyncTargets() {
		if !sf.taskPool.EnqueueHeaderHeightTask(height) {
			logrus.Warnf("enqueue header-by-height task failed. height:%v", height)
		}
	}
	for _, hash := range sf.GetHeaderByHashSyncTargets() {
		if !sf.taskPool.EnqueueHeaderHashTask(hash) {
			logrus.Warnf("enqueue header-by-hash task failed. hash:%v", sf.normalize(hash))
		}
	}

	bodyTargets := sf.GetBodySyncTargets()
	if len(bodyTargets) > 0 {
		sf.runScanStageAsync(ctx, scanStageBodyDone, strings.Join(bodyTargets, ","), sf.SyncBodyTarget, func(event ScanStageEvent) {
			sf.logScanStageEvent(event)
			if event.success && (ctx == nil || ctx.Err() == nil) {
				sf.inspectBlockTreeState("after_body_sync")
			}
		})
	}
	return true
}

func (sf *Flow) runScanStageAsync(ctx context.Context, stage scanStage, target string, fn func(context.Context, string) (bool, string), after func(ScanStageEvent)) bool {
	go func() {
		startedAt := time.Now()
		if ctx != nil && ctx.Err() != nil {
			if after != nil {
				after(ScanStageEvent{stage: stage, target: target, duration: time.Since(startedAt), errMsg: "scan context cancelled"})
			}
			return
		}
		success, errMsg := fn(ctx, target)
		if after != nil {
			after(ScanStageEvent{stage: stage, target: target, success: success, duration: time.Since(startedAt), errMsg: errMsg})
		}
	}()
	return true
}

func (sf *Flow) GetHeaderByHeightSyncTargets() []uint64 {
	if sf == nil || sf.blockTree == nil || sf.taskPool == nil || sf.latestRemoteHeight == nil {
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

func (sf *Flow) GetHeaderByHashSyncTargets() []string {
	if sf == nil || sf.blockTree == nil || sf.taskPool == nil {
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

func (sf *Flow) GetBodySyncTargets() []string {
	if sf == nil || sf.blockTree == nil || sf.getNodeBlockBody == nil {
		return nil
	}
	targets := make([]string, 0)
	seen := make(map[string]struct{})
	for _, branch := range sf.blockTree.Branches() {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			node := branch.Nodes[i]
			if node == nil {
				continue
			}
			state, ok := sf.BuildBranchProcessState(node)
			if !ok {
				continue
			}
			nodeData := sf.getNodeBlockBody(node.Key)
			if !sf.hasBody(nodeData) || sf.HasStorableNodeData(nodeData) {
				if _, exists := seen[state.Hash]; !exists {
					seen[state.Hash] = struct{}{}
					targets = append(targets, state.Hash)
				}
			}
		}
	}
	return targets
}

func (sf *Flow) SyncHeaderByHeightTarget(_ context.Context, target string) (bool, string) {
	height, err := strconv.ParseUint(target, 10, 64)
	if err != nil {
		return false, "invalid header-by-height target"
	}
	if !sf.hasHeader(sf.FetchAndInsertHeaderByHeight(height)) {
		return false, "header-by-height fetch failed"
	}
	return true, ""
}

func (sf *Flow) SyncHeaderByHashTarget(_ context.Context, target string) (bool, string) {
	hash := sf.normalize(target)
	if hash == "" {
		return false, "invalid header-by-hash target"
	}
	if !sf.FetchAndInsertHeaderByHash(hash) {
		return false, "header-by-hash fetch failed"
	}
	return true, ""
}

func (sf *Flow) SyncBodyTarget(ctx context.Context, target string) (bool, string) {
	if strings.TrimSpace(target) == "" {
		return false, "invalid body target"
	}
	if sf == nil || sf.blockTree == nil || sf.getNodeBlockBody == nil {
		return false, "scan runtime or blocktree not available"
	}
	for _, raw := range strings.Split(target, ",") {
		if ctx != nil && ctx.Err() != nil {
			return false, ctx.Err().Error()
		}
		hash := sf.normalize(raw)
		if hash == "" {
			continue
		}
		node := sf.blockTree.Get(hash)
		if node == nil {
			continue
		}
		state, ok := sf.BuildBranchProcessState(node)
		if !ok {
			continue
		}
		nodeData := sf.getNodeBlockBody(node.Key)
		if !sf.hasBody(nodeData) {
			sf.EnqueueNodeBodySync(state)
			continue
		}
		if !state.ParentReady || !sf.HasStorableNodeData(nodeData) {
			continue
		}
		sf.StoreNodeBodyData(ctx, node, nodeData, state)
	}
	return true, ""
}

func (sf *Flow) CountActionableBodyNodes() int {
	if sf == nil || sf.blockTree == nil || sf.getNodeBlockBody == nil {
		return 0
	}
	count := 0
	for _, branch := range sf.blockTree.Branches() {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			node := branch.Nodes[i]
			if node == nil {
				continue
			}
			state, ok := sf.BuildBranchProcessState(node)
			if !ok {
				continue
			}
			nodeData := sf.getNodeBlockBody(node.Key)
			if !sf.hasBody(nodeData) {
				count++
				continue
			}
			if !state.ParentReady {
				continue
			}
			if sf.HasStorableNodeData(nodeData) {
				count++
			}
		}
	}
	return count
}

func (sf *Flow) CountStoredLinkedNodes() int {
	if sf == nil || sf.blockTree == nil || sf.storedBlocks == nil {
		return 0
	}
	count := 0
	for _, node := range sf.blockTree.LinkedNodes() {
		if node != nil && sf.storedBlocks.IsStored(node.Key) {
			count++
		}
	}
	return count
}

func (sf *Flow) EnsureBootstrapHeader() bool {
	sf.BindRuntimeDeps()
	if sf == nil || sf.blockTree == nil || sf.bootstrapHeaderByHeight == nil {
		return false
	}
	if _, _, ok := sf.blockTree.HeightRange(); ok {
		return true
	}
	height := sf.startHeight
	header := sf.bootstrapHeaderByHeight(context.Background(), height)
	if !sf.hasHeader(header) {
		logrus.Warnf("bootstrap get header failed from all nodes. height:%v", height)
		return false
	}
	sf.InsertHeader(header)
	logrus.Infof("bootstrap blocktree root by startHeight success. height:%v hash:%v", height, sf.normalize(sf.headerHash(header)))
	return true
}

func (sf *Flow) SyncHeaderWindow() {
	sf.BindRuntimeDeps()
	if sf == nil || sf.blockTree == nil {
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
			header := sf.FetchAndInsertHeaderByHeight(height)
			if !sf.hasHeader(header) {
				return
			}
			logrus.Infof("sync header window bootstrap by startHeight success. height:%v hash:%v", height, sf.normalize(sf.headerHash(header)))
			continue
		}
		if sf.ShouldStopHeaderWindowSync(start, end, targetSize) {
			return
		}
		if !sf.hasHeader(sf.FetchAndInsertHeaderByHeight(end + 1)) {
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
	if sf == nil || sf.latestRemoteHeight == nil {
		return true
	}
	window := end - start + 1
	if window >= targetSize {
		return true
	}
	latestRemote := sf.latestRemoteHeight()
	return latestRemote == 0 || end >= latestRemote
}

func (sf *Flow) FetchAndInsertHeaderByHeight(height uint64) any {
	sf.BindRuntimeDeps()
	if sf == nil || sf.taskPool == nil {
		return nil
	}
	if !sf.taskPool.TryStartHeaderHeightSync(height) {
		return nil
	}
	defer sf.taskPool.FinishHeaderHeightSync(height)
	return sf.FetchAndInsertHeaderByHeightCore(height)
}

func (sf *Flow) FetchAndInsertHeaderByHeightCore(height uint64) any {
	if sf == nil || sf.fetchHeaderByHeight == nil {
		return nil
	}
	header := sf.fetchHeaderByHeight(context.Background(), height)
	if !sf.hasHeader(header) {
		return nil
	}
	sf.InsertHeader(header)
	return header
}

func (sf *Flow) SyncOrphanParents() {
	if sf == nil || sf.blockTree == nil {
		return
	}
	for _, missingParent := range sf.blockTree.UnlinkedNodes() {
		hash := sf.normalize(missingParent)
		if sf.ShouldSyncOrphanParent(hash) {
			sf.FetchAndInsertHeaderByHash(hash)
		}
	}
}

func (sf *Flow) ShouldSyncOrphanParent(hash string) bool {
	return sf != nil && sf.blockTree != nil && hash != "" && sf.blockTree.Get(hash) == nil
}

func (sf *Flow) FetchAndInsertHeaderByHash(hash string) bool {
	sf.BindRuntimeDeps()
	hash = sf.normalize(hash)
	if hash == "" || sf == nil || sf.taskPool == nil {
		return false
	}
	if !sf.taskPool.TryStartHeaderHashSync(hash) {
		return false
	}
	defer sf.taskPool.FinishHeaderHashSync(hash)
	return sf.FetchAndInsertHeaderByHashCore(hash)
}

func (sf *Flow) FetchAndInsertHeaderByHashImmediate(hash string) bool {
	sf.BindRuntimeDeps()
	hash = sf.normalize(hash)
	if hash == "" || sf == nil || sf.fetchHeaderByHash == nil {
		return false
	}
	header := sf.fetchHeaderByHash(context.Background(), hash)
	if !sf.hasHeader(header) {
		return false
	}
	sf.InsertHeader(header)
	return true
}

func (sf *Flow) FetchAndInsertHeaderByHashCore(hash string) bool {
	return sf.FetchAndInsertHeaderByHashImmediate(hash)
}

func (sf *Flow) ProcessBranchesLowToHigh(ctx context.Context) {
	sf.BindRuntimeDeps()
	if sf == nil || sf.blockTree == nil {
		return
	}
	for _, branch := range sf.blockTree.Branches() {
		for i := len(branch.Nodes) - 1; i >= 0; i-- {
			sf.ProcessBranchNode(ctx, branch.Nodes[i])
		}
	}
}

func (sf *Flow) ProcessBranchNode(ctx context.Context, node *blocktree.LinkedNode) {
	if node == nil || sf == nil || sf.getNodeBlockBody == nil {
		return
	}
	state, ok := sf.BuildBranchProcessState(node)
	if !ok {
		return
	}
	nodeData := sf.getNodeBlockBody(node.Key)
	if !sf.hasBody(nodeData) {
		sf.EnqueueNodeBodySync(state)
		return
	}
	sf.StoreNodeBodyData(ctx, node, nodeData, state)
}

func (sf *Flow) BuildBranchProcessState(node *blocktree.LinkedNode) (BranchProcessState, bool) {
	if sf == nil || sf.blockTree == nil || sf.storedBlocks == nil {
		return BranchProcessState{}, false
	}
	hash := sf.normalize(node.Key)
	if hash == "" || sf.storedBlocks.IsStored(hash) || (sf.storeWorker != nil && sf.storeWorker.IsInflight(hash)) {
		return BranchProcessState{}, false
	}
	parentHash := sf.normalize(node.ParentKey)
	parentNode := sf.blockTree.Get(parentHash)
	return BranchProcessState{
		Hash:        hash,
		ParentReady: parentHash == "" || parentNode == nil || sf.storedBlocks.IsStored(parentHash),
	}, true
}

func (sf *Flow) EnqueueNodeBodySync(state BranchProcessState) {
	if sf == nil || sf.taskPool == nil {
		return
	}
	priority := fetchtask.TaskPriorityNormal
	if state.ParentReady {
		priority = fetchtask.TaskPriorityHigh
	}
	sf.taskPool.EnqueueBodyTask(state.Hash, priority)
}

func (sf *Flow) StoreNodeBodyData(ctx context.Context, node *blocktree.LinkedNode, nodeData any, state BranchProcessState) {
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if !sf.HasStorableNodeData(nodeData) || !state.ParentReady {
		return
	}
	if sf.storeWorker == nil {
		logrus.Warn("store block worker is not initialized")
		return
	}
	if err := sf.storeWorker.Submit(ctx, state.Hash, node.Height, nodeData); err != nil {
		logrus.Warnf("store block failed. height:%v hash:%v err:%v", node.Height, state.Hash, err)
	}
}

func (sf *Flow) HasStorableNodeData(nodeData any) bool {
	return sf != nil && sf.bodyStorable != nil && sf.bodyStorable(nodeData)
}

func (sf *Flow) SyncNodeDataByHash(ctx context.Context, hash string) bool {
	sf.BindRuntimeDeps()
	if ctx == nil {
		ctx = context.Background()
	}
	hash = sf.normalize(hash)
	if hash == "" {
		return true
	}
	if sf == nil || sf.blockTree == nil || sf.getNodeBlockHeader == nil || sf.setNodeBlockHeader == nil || sf.setNodeBlockBody == nil || sf.fetchHeaderByHash == nil || sf.fetchBodyByHash == nil {
		return false
	}
	node := sf.blockTree.Get(hash)
	if node == nil {
		return true
	}

	header := sf.getNodeBlockHeader(hash)
	if !sf.hasHeader(header) {
		header = sf.fetchHeaderByHash(ctx, hash)
		if !sf.hasHeader(header) {
			return false
		}
		sf.setNodeBlockHeader(hash, header)
	}

	height, ok := sf.headerHeight(header)
	if !ok {
		height = node.Height
	}

	body, nodeID, costMicros, ok := sf.fetchBodyByHash(ctx, hash, height, header)
	if !ok {
		if sf.updateNodeState != nil && nodeID >= 0 {
			sf.updateNodeState(nodeID, costMicros, false)
		}
		return false
	}

	sf.setNodeBlockBody(hash, body)
	if sf.updateNodeState != nil && nodeID >= 0 {
		sf.updateNodeState(nodeID, costMicros, true)
	}
	return true
}

func (sf *Flow) InsertHeader(header any) {
	sf.BindRuntimeDeps()
	if sf == nil || sf.blockTree == nil || sf.setNodeBlockHeader == nil || !sf.hasHeader(header) {
		return
	}
	height, ok := sf.headerHeight(header)
	if !ok {
		return
	}
	key := sf.normalize(sf.headerHash(header))
	parentKey := sf.normalize(sf.headerParentHash(header))
	weight := sf.headerWeight(header)
	sf.blockTree.Insert(height, key, parentKey, weight, nil)
	sf.setNodeBlockHeader(key, nil)
}

func (sf *Flow) logScanStageEvent(event ScanStageEvent) {
	stage := scanStageName(event.stage)
	if event.success {
		logrus.Infof("scan stage event stage:%s target:%v success:%v duration:%v", stage, event.target, event.success, event.duration)
		return
	}
	logrus.Warnf("scan stage event stage:%s target:%v success:%v duration:%v err:%v", stage, event.target, event.success, event.duration, event.errMsg)
}

func (sf *Flow) inspectBlockTreeState(stage string) {
	if sf == nil || sf.blockTree == nil {
		return
	}
	start, end, ok := sf.blockTree.HeightRange()
	if !ok {
		logrus.Infof("scan stage:%s blocktree empty", stage)
		return
	}
	logrus.Infof("scan stage:%s blocktree range:[%v,%v] unlinked:%v", stage, start, end, len(sf.blockTree.UnlinkedNodes()))
}
