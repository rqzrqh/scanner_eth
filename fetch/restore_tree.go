package fetch

import (
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
)

func (fm *FetchManager) restoreBlockTree(blocks []model.Block) (int, error) {
	if fm == nil {
		return 0, nil
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
	return deps.RestoreBlockTree(blocks)
}

func parseStoredBlockWeight(difficulty string) uint64 {
	return fetchstore.ParseStoredBlockWeight(difficulty)
}
