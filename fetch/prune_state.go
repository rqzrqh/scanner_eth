package fetch

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
)

type pruneNodeSnapshot = fetchstore.PruneNodeSnapshot
type pruneBranchSnapshot = fetchstore.PruneBranchSnapshot
type pruneStateSnapshot = fetchstore.PruneStateSnapshot

func (fm *FetchManager) capturePruneStateSnapshot() *pruneStateSnapshot {
	if fm == nil {
		return (&fetchstore.TreeRuntimeDeps{}).CapturePruneStateSnapshot()
	}
	return fm.buildTreeRuntimeDeps().CapturePruneStateSnapshot()
}

func (fm *FetchManager) logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
	logPruneSnapshot(stage, snapshot)
}

func logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
	fetchstore.LogPruneSnapshot(stage, snapshot)
}

func (fm *FetchManager) storedHeightRangeOnTree() (uint64, uint64, bool) {
	return fm.buildTreeRuntimeDeps().StoredHeightRangeOnTree()
}

func (fm *FetchManager) pruneStoredBlocks(ctx context.Context) {
	if fm == nil {
		return
	}
	deps := fm.buildTreeRuntimeDeps()
	deps.PruneStoredBlocks(ctx, fm.irreversibleBlocks)
}
