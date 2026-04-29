package fetch

import (
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
)

func (fm *FetchManager) restoreBlockTree(blocks []model.Block) (int, error) {
	if fm == nil {
		return 0, nil
	}
	return fm.buildTreeRuntimeDeps().RestoreBlockTree(blocks)
}

func parseStoredBlockWeight(difficulty string) uint64 {
	return fetchstore.ParseStoredBlockWeight(difficulty)
}
