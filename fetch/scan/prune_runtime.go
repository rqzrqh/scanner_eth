package scan

import (
	"context"
	"scanner_eth/blocktree"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/taskpool"
	"scanner_eth/util"
	"time"

	"github.com/sirupsen/logrus"
)

type PruneRuntimeDeps struct {
	BlockTree    *blocktree.BlockTree
	StagingStore *fetchstore.StagingStore
	StoredBlocks *fetchstore.StoredBlockState
	TaskPool     *fetchtask.Pool
}

type pruneNodeSnapshot struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
	Weight uint64 `json:"weight"`
}

type pruneBranchSnapshot struct {
	Header    pruneNodeSnapshot `json:"header"`
	NodeCount int               `json:"node_count"`
}

type pruneStateSnapshot struct {
	Root     *pruneNodeSnapshot    `json:"root"`
	Branches []pruneBranchSnapshot `json:"branches"`
}

func (sf *Flow) RunPruneStage(ctx context.Context) {
	startedAt := time.Now()
	if sf == nil || sf.irreversible <= 0 {
		return
	}
	// Prune must observe the post-store state from the same scan cycle. Scan
	// therefore waits for body persistence to finish before pruning. This keeps
	// serial_store on the simpler contract of consuming scan-time low-to-high
	// full branch snapshots without prune interleaving during the same cycle.
	before := sf.pruneRuntime.CaptureStateSnapshot()
	sf.pruneRuntime.PruneStoredBlocks(ctx, sf.irreversible)
	after := sf.pruneRuntime.CaptureStateSnapshot()
	beforeCount := 0
	afterCount := 0
	if before != nil {
		beforeCount = len(before.Branches)
	}
	if after != nil {
		afterCount = len(after.Branches)
	}
	event := scanStageEvent{
		stage:       scanStagePrune,
		target:      "stored_blocks",
		targetCount: beforeCount,
		success:     ctx == nil || ctx.Err() == nil,
		duration:    time.Since(startedAt),
	}
	if !event.success {
		event.errMsg = ctx.Err().Error()
	}
	if afterCount != beforeCount {
		event.target = "stored_blocks_pruned"
	}
	sf.logScanStageEvent(event)
}

func (deps PruneRuntimeDeps) CaptureStateSnapshot() *pruneStateSnapshot {
	if deps.BlockTree == nil {
		return &pruneStateSnapshot{Branches: make([]pruneBranchSnapshot, 0)}
	}

	state := &pruneStateSnapshot{
		Branches: make([]pruneBranchSnapshot, 0),
	}
	root := deps.BlockTree.Root()
	if root != nil {
		state.Root = &pruneNodeSnapshot{
			Height: root.Height,
			Hash:   util.NormalizeHash(root.Key),
			Weight: root.Weight,
		}
	}

	for _, branch := range deps.BlockTree.Branches() {
		state.Branches = append(state.Branches, pruneBranchSnapshot{
			Header: pruneNodeSnapshot{
				Height: branch.Header.Height,
				Hash:   util.NormalizeHash(branch.Header.Key),
				Weight: branch.Header.Weight,
			},
			NodeCount: len(branch.Nodes),
		})
	}

	return state
}

func (deps PruneRuntimeDeps) StoredHeightRangeOnTree() (uint64, uint64, bool) {
	if deps.BlockTree == nil || deps.StoredBlocks == nil {
		return 0, 0, false
	}
	linkedNodes := deps.BlockTree.LinkedNodes()
	var minHeight uint64
	var maxHeight uint64
	hasStored := false
	for _, nv := range linkedNodes {
		hash := util.NormalizeHash(nv.Key)
		if !deps.StoredBlocks.IsStored(hash) {
			continue
		}
		if !hasStored {
			minHeight = nv.Height
			maxHeight = nv.Height
			hasStored = true
			continue
		}
		if nv.Height < minHeight {
			minHeight = nv.Height
		}
		if nv.Height > maxHeight {
			maxHeight = nv.Height
		}
	}
	return minHeight, maxHeight, hasStored
}

func (deps PruneRuntimeDeps) PruneStoredBlocks(ctx context.Context, irreversibleBlocks int) {
	if ctx != nil && ctx.Err() != nil {
		return
	}
	if deps.BlockTree == nil || irreversibleBlocks <= 0 {
		return
	}

	start, end, ok := deps.StoredHeightRangeOnTree()
	if !ok {
		return
	}

	keep := uint64(irreversibleBlocks + 1)
	if end-start+1 <= keep {
		return
	}

	beforeState := deps.CaptureStateSnapshot()
	if ctx != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
	prunedNodes := deps.BlockTree.Prune((end - start + 1) - keep)
	if len(prunedNodes) == 0 {
		return
	}
	afterState := deps.CaptureStateSnapshot()

	for _, nv := range prunedNodes {
		hash := util.NormalizeHash(nv.Key)
		if deps.StagingStore != nil {
			deps.StagingStore.DeleteBlock(hash)
		}
		if deps.StoredBlocks != nil {
			deps.StoredBlocks.UnmarkStored(hash)
		}
		if deps.TaskPool != nil {
			deps.TaskPool.DelTask(hash)
		}
	}

	logPruneSnapshot("before", beforeState)
	logrus.Infof("prune blocktree nodes. pruned:%v keep:%v", len(prunedNodes), keep)
	logPruneSnapshot("after", afterState)
}

func logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
	if snapshot == nil {
		logrus.Infof("prune %s snapshot root:null branchs:0", stage)
		return
	}

	if snapshot.Root == nil {
		logrus.Infof("prune %s snapshot root:null branchs:0", stage)
	} else {
		logrus.Infof(
			"prune %s snapshot root height:%v hash:%v weight:%v branchs:%v",
			stage,
			snapshot.Root.Height,
			snapshot.Root.Hash,
			snapshot.Root.Weight,
			len(snapshot.Branches),
		)
	}

	for i, branch := range snapshot.Branches {
		logrus.Infof(
			"prune %s snapshot branch[%v] header_height:%v header_hash:%v header_weight:%v node_count:%v",
			stage,
			i,
			branch.Header.Height,
			branch.Header.Hash,
			branch.Header.Weight,
			branch.NodeCount,
		)
	}
}
