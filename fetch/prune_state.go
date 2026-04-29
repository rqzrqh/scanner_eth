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
	payloadStore := fm.runtimePayloadStore()
	blockTree := fm.runtimeBlockTree()
	deps := fetchstore.TreeRuntimeDeps{
		BlockTree: blockTree,
		PayloadAccessor: &treePayloadAccessorAdapter{
			nodeExists: func(hash string) bool { return blockTree != nil && blockTree.Get(hash) != nil },
			store:      payloadStore,
		},
		StoredBlocks:  fm.runtimeStoredBlocks(),
		TaskPool:      fm.runtimeTaskPool(),
		NormalizeHash: normalizeHash,
		ParseWeight:   parseStoredBlockWeight,
	}
	return deps.CapturePruneStateSnapshot()
}

func (fm *FetchManager) logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
	logPruneSnapshot(stage, snapshot)
}

func logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
	fetchstore.LogPruneSnapshot(stage, snapshot)
}

func (fm *FetchManager) storedHeightRangeOnTree() (uint64, uint64, bool) {
	payloadStore := fm.runtimePayloadStore()
	blockTree := fm.runtimeBlockTree()
	deps := fetchstore.TreeRuntimeDeps{
		BlockTree: blockTree,
		PayloadAccessor: &treePayloadAccessorAdapter{
			nodeExists: func(hash string) bool { return blockTree != nil && blockTree.Get(hash) != nil },
			store:      payloadStore,
		},
		StoredBlocks:  fm.runtimeStoredBlocks(),
		TaskPool:      fm.runtimeTaskPool(),
		NormalizeHash: normalizeHash,
		ParseWeight:   parseStoredBlockWeight,
	}
	return deps.StoredHeightRangeOnTree()
}

func (fm *FetchManager) pruneStoredBlocks(ctx context.Context) {
	if fm == nil {
		return
	}
	payloadStore := fm.runtimePayloadStore()
	blockTree := fm.runtimeBlockTree()
	deps := fetchstore.TreeRuntimeDeps{
		BlockTree: blockTree,
		PayloadAccessor: &treePayloadAccessorAdapter{
			nodeExists: func(hash string) bool { return blockTree != nil && blockTree.Get(hash) != nil },
			store:      payloadStore,
		},
		StoredBlocks:  fm.runtimeStoredBlocks(),
		TaskPool:      fm.runtimeTaskPool(),
		NormalizeHash: normalizeHash,
		ParseWeight:   parseStoredBlockWeight,
	}
	deps.PruneStoredBlocks(ctx, fm.irreversibleBlocks)
}
