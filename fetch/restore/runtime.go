package restore

import (
	"scanner_eth/blocktree"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"scanner_eth/util"
	"sort"
	"strconv"
	"strings"
)

type RuntimeDeps struct {
	BlockTree    *blocktree.BlockTree
	StagingStore *fetchstore.StagingStore
	StoredBlocks *fetchstore.StoredBlockState
}

func parseStoredBlockWeight(difficulty string) uint64 {
	if strings.TrimSpace(difficulty) == "" {
		return 0
	}
	v, err := strconv.ParseUint(difficulty, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (deps RuntimeDeps) RestoreBlockTree(blocks []model.Block) (int, error) {
	if len(blocks) == 0 || deps.BlockTree == nil {
		return 0, nil
	}

	sort.Slice(blocks, func(i, j int) bool {
		if blocks[i].Height != blocks[j].Height {
			return blocks[i].Height < blocks[j].Height
		}
		return util.NormalizeHash(blocks[i].Hash) < util.NormalizeHash(blocks[j].Hash)
	})

	for i := range blocks {
		blk := &blocks[i]
		hash := util.NormalizeHash(blk.Hash)
		if hash == "" || deps.BlockTree.Get(hash) != nil {
			continue
		}

		parentHash := util.NormalizeHash(blk.ParentHash)
		weight := parseStoredBlockWeight(blk.Difficulty)
		deps.BlockTree.Insert(blk.Height, hash, parentHash, weight)

		if deps.BlockTree.Get(hash) == nil {
			continue
		}
		if deps.StagingStore != nil {
			deps.StagingStore.SetPendingHeader(hash, nil)
		}
		if blk.Complete && deps.StoredBlocks != nil {
			deps.StoredBlocks.MarkStored(hash)
		}
	}

	return len(blocks), nil
}
