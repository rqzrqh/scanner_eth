package scan

import (
	"context"
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtaskprocess "scanner_eth/fetch/task_process"
	"scanner_eth/util"
	"time"

	fetchtask "scanner_eth/fetch/taskpool"

	"github.com/sirupsen/logrus"
)

type RuntimeDeps struct {
	StartHeight  uint64
	Irreversible int
	BlockTree    *blocktree.BlockTree
	TaskPool     *fetchtask.Pool
	StoreWorker  *fetchserialstore.Worker
	StoredBlocks *fetchstore.StoredBlockState
	ScanWorker   *Worker
	StagingStore *fetchstore.StagingStore
	NodeManager  *nodepkg.NodeManager
	Fetcher      fetcherpkg.Fetcher
	PruneRuntime PruneRuntimeDeps
}

type scanStage int

const (
	scanStageExpandTree scanStage = iota + 1
	scanStageFillTree
	scanStageSyncBody
	scanStageStoreBranches
	scanStagePrune

	bodyTargetNodeSep   = ","
	bodyTargetBranchSep = ";"
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
	case scanStageExpandTree:
		return "expand_tree"
	case scanStageFillTree:
		return "fill_tree"
	case scanStageSyncBody:
		return "sync_body"
	case scanStageStoreBranches:
		return "store_branches"
	case scanStagePrune:
		return "prune"
	default:
		return "unknown"
	}
}

type Flow struct {
	runtimeDepsFn func() RuntimeDeps

	startHeight  uint64
	irreversible int
	blockTree    *blocktree.BlockTree
	taskPool     *fetchtask.Pool
	storeWorker  *fetchserialstore.Worker
	storedBlocks *fetchstore.StoredBlockState
	scanWorker   *Worker
	stagingStore *fetchstore.StagingStore
	nodeManager  *nodepkg.NodeManager
	fetcher      fetcherpkg.Fetcher
	taskRuntime  fetchtaskprocess.RuntimeDeps
	pruneRuntime PruneRuntimeDeps
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
	sf.scanWorker = deps.ScanWorker
	sf.stagingStore = deps.StagingStore
	sf.nodeManager = deps.NodeManager
	sf.fetcher = deps.Fetcher
	var enqueueBodyTask func(string)
	if deps.TaskPool != nil && deps.StoreWorker != nil {
		enqueueBodyTask = func(hash string) {
			deps.TaskPool.EnqueueTaskWithPriority(hash, fetchtask.TaskPriorityHigh)
		}
	}
	sf.taskRuntime = fetchtaskprocess.NewRuntimeDeps(
		deps.BlockTree,
		deps.StagingStore,
		deps.NodeManager,
		deps.Fetcher,
		enqueueBodyTask,
		deps.TaskPool.TryStartHeaderHeightSync,
		deps.TaskPool.FinishHeaderHeightSync,
		deps.TaskPool.TryStartHeaderHashSync,
		deps.TaskPool.FinishHeaderHashSync,
	)
	sf.pruneRuntime = deps.PruneRuntime
}

func (sf *Flow) normalize(v string) string {
	return util.NormalizeHash(v)
}

func (sf *Flow) TriggerScan() {
	if sf == nil || sf.scanWorker == nil {
		return
	}
	sf.scanWorker.Trigger()
}

func (sf *Flow) nodeExists(hash string) bool {
	hash = sf.normalize(hash)
	return sf != nil && hash != "" && sf.blockTree.Get(hash) != nil
}

func (sf *Flow) getPendingBody(hash string) *fetchstore.EventBlockData {
	if sf == nil {
		return nil
	}
	return sf.stagingStore.GetPendingBody(sf.normalize(hash))
}

func (sf *Flow) RunScanCycle(ctx context.Context) {
	sf.BindRuntimeDeps()
	if sf == nil || !sf.RunScanStages(ctx) {
		return
	}
	sf.RunPruneStage(ctx)
}

func (sf *Flow) RunScanStages(ctx context.Context) bool {
	if !sf.canRunScanStage(ctx) {
		return false
	}

	sf.RunExpandTreeStage(ctx)
	sf.RunFillTreeStage(ctx)
	bodyBranches := sf.RunSyncBodyStage(ctx)
	if len(bodyBranches) > 0 {
		sf.RunStoreBranchesStage(ctx, bodyBranches)
	}
	return true
}

func (sf *Flow) canRunScanStage(ctx context.Context) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	return sf != nil
}

func (sf *Flow) logScanStageEvent(event scanStageEvent) {
	stage := scanStageName(event.stage)
	if event.success {
		logrus.Infof("scan stage event stage:%s target:%v success:%v duration:%v", stage, event.target, event.success, event.duration)
		return
	}
	logrus.Warnf("scan stage event stage:%s target:%v success:%v duration:%v err:%v", stage, event.target, event.success, event.duration, event.errMsg)
}

func (sf *Flow) inspectBlockTreeState(stage string) {
	if sf == nil {
		return
	}
	start, end, ok := sf.blockTree.HeightRange()
	if !ok {
		logrus.Infof("scan stage:%s blocktree empty", stage)
		return
	}
	logrus.Infof("scan stage:%s blocktree range:[%v,%v] unlinked:%v", stage, start, end, len(sf.blockTree.UnlinkedNodes()))
}
