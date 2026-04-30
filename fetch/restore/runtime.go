package restore

import (
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	"scanner_eth/model"
	"sort"
	"strconv"
	"strings"
)

type PendingBlockStore interface {
	SetHeader(string, *fetcherpkg.BlockHeaderJson)
}

type StoredBlockState interface {
	MarkStored(string)
}

type RuntimeDeps struct {
	BlockTree         *blocktree.BlockTree
	PendingBlockStore PendingBlockStore
	StoredBlocks      StoredBlockState
	NormalizeHash     func(string) string
}

type branch struct {
	nodes  []*model.Block
	weight uint64
}

func (deps RuntimeDeps) normalizeHash(hash string) string {
	if deps.NormalizeHash == nil {
		return strings.ToLower(strings.TrimSpace(hash))
	}
	return deps.NormalizeHash(hash)
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

	blocksByHash := make(map[string]*model.Block, len(blocks))
	childrenByParent := make(map[string][]*model.Block)
	for i := range blocks {
		blk := &blocks[i]
		hash := deps.normalizeHash(blk.Hash)
		if hash == "" {
			continue
		}
		parentHash := deps.normalizeHash(blk.ParentHash)
		blocksByHash[hash] = blk
		childrenByParent[parentHash] = append(childrenByParent[parentHash], blk)
	}

	sortByHeightAndHash := func(list []*model.Block) {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Height != list[j].Height {
				return list[i].Height < list[j].Height
			}
			return deps.normalizeHash(list[i].Hash) < deps.normalizeHash(list[j].Hash)
		})
	}
	for parent := range childrenByParent {
		sortByHeightAndHash(childrenByParent[parent])
	}

	starts := make([]*model.Block, 0)
	for i := range blocks {
		blk := &blocks[i]
		hash := deps.normalizeHash(blk.Hash)
		if hash == "" {
			continue
		}
		parentHash := deps.normalizeHash(blk.ParentHash)
		if parentHash == "" {
			starts = append(starts, blk)
			continue
		}
		if _, ok := blocksByHash[parentHash]; !ok {
			starts = append(starts, blk)
		}
	}
	sortByHeightAndHash(starts)

	branches := make([]branch, 0)
	var dfs func(path []*model.Block, cur *model.Block, accWeight uint64)
	dfs = func(path []*model.Block, cur *model.Block, accWeight uint64) {
		path = append(path, cur)
		accWeight += parseStoredBlockWeight(cur.Difficulty)

		children := childrenByParent[deps.normalizeHash(cur.Hash)]
		if len(children) == 0 {
			branchNodes := make([]*model.Block, len(path))
			copy(branchNodes, path)
			branches = append(branches, branch{nodes: branchNodes, weight: accWeight})
			return
		}
		for _, child := range children {
			dfs(path, child, accWeight)
		}
	}
	for _, start := range starts {
		dfs(nil, start, 0)
	}

	sort.Slice(branches, func(i, j int) bool {
		if branches[i].weight != branches[j].weight {
			return branches[i].weight > branches[j].weight
		}
		iLast := branches[i].nodes[len(branches[i].nodes)-1]
		jLast := branches[j].nodes[len(branches[j].nodes)-1]
		if iLast.Height != jLast.Height {
			return iLast.Height > jLast.Height
		}
		return deps.normalizeHash(iLast.Hash) < deps.normalizeHash(jLast.Hash)
	})

	for _, branch := range branches {
		for _, blk := range branch.nodes {
			hash := deps.normalizeHash(blk.Hash)
			if hash == "" || deps.BlockTree.Get(hash) != nil {
				continue
			}

			parentHash := deps.normalizeHash(blk.ParentHash)
			weight := parseStoredBlockWeight(blk.Difficulty)
			irreversible := blocktree.IrreversibleNode{Height: blk.IrreversibleHeight, Key: deps.normalizeHash(blk.IrreversibleHash)}
			if irreversible.Key == "" {
				irreversible = blocktree.IrreversibleNode{Height: blk.Height, Key: hash}
			}

			deps.BlockTree.Insert(blk.Height, hash, parentHash, weight, &irreversible)
			if deps.PendingBlockStore != nil && deps.BlockTree.Get(hash) != nil {
				deps.PendingBlockStore.SetHeader(hash, nil)
			}
			if blk.Complete && deps.StoredBlocks != nil {
				deps.StoredBlocks.MarkStored(hash)
			}
		}
	}

	return len(blocks), nil
}
