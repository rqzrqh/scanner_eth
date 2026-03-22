package fetch

import (
	"fmt"
	"scanner_eth/blocktree"
	"scanner_eth/model"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type restoreBranch struct {
	nodes  []*model.Block
	weight uint64
}

func (fm *FetchManager) restoreBlockTree(blocks []model.Block) (int, error) {
	if len(blocks) == 0 {
		return 0, nil
	}

	blocksByHash := make(map[string]*model.Block, len(blocks))
	childrenByParent := make(map[string][]*model.Block)
	for i := range blocks {
		blk := &blocks[i]
		hash := normalizeHash(blk.Hash)
		if hash == "" {
			continue
		}
		parentHash := normalizeHash(blk.ParentHash)
		blocksByHash[hash] = blk
		childrenByParent[parentHash] = append(childrenByParent[parentHash], blk)
	}

	sortByHeightAndHash := func(list []*model.Block) {
		sort.Slice(list, func(i, j int) bool {
			if list[i].Height != list[j].Height {
				return list[i].Height < list[j].Height
			}
			return normalizeHash(list[i].Hash) < normalizeHash(list[j].Hash)
		})
	}
	for parent := range childrenByParent {
		sortByHeightAndHash(childrenByParent[parent])
	}

	starts := make([]*model.Block, 0)
	for i := range blocks {
		blk := &blocks[i]
		hash := normalizeHash(blk.Hash)
		if hash == "" {
			continue
		}
		parentHash := normalizeHash(blk.ParentHash)
		if parentHash == "" {
			starts = append(starts, blk)
			continue
		}
		if _, ok := blocksByHash[parentHash]; !ok {
			starts = append(starts, blk)
		}
	}
	sortByHeightAndHash(starts)

	branches := make([]restoreBranch, 0)
	var dfs func(path []*model.Block, cur *model.Block, accWeight uint64)
	dfs = func(path []*model.Block, cur *model.Block, accWeight uint64) {
		path = append(path, cur)
		accWeight += parseStoredBlockWeight(cur.Difficulty)

		children := childrenByParent[normalizeHash(cur.Hash)]
		if len(children) == 0 {
			branchNodes := make([]*model.Block, len(path))
			copy(branchNodes, path)
			branches = append(branches, restoreBranch{nodes: branchNodes, weight: accWeight})
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
		return normalizeHash(iLast.Hash) < normalizeHash(jLast.Hash)
	})

	for _, branch := range branches {
		for _, blk := range branch.nodes {
			hash := normalizeHash(blk.Hash)
			if hash == "" || fm.blockTree.Get(hash) != nil {
				continue
			}

			parentHash := normalizeHash(blk.ParentHash)
			parentExists := parentHash == "" || fm.blockTree.Get(parentHash) != nil
			if !parentExists && fm.blockTree.Root() == nil {
				return 0, fmt.Errorf("bootstrap insert failed: missing parent %s for block %s and tree has no root", parentHash, hash)
			}

			weight := parseStoredBlockWeight(blk.Difficulty)
			irreversible := blocktree.IrreversibleNode{Height: blk.IrreversibleHeight, Key: normalizeHash(blk.IrreversibleHash)}
			if irreversible.Key == "" {
				irreversible = blocktree.IrreversibleNode{Height: blk.Height, Key: hash}
			}
			header := &BlockHeaderJson{
				Number:       hexutil.EncodeUint64(blk.Height),
				Hash:         hash,
				ParentHash:   parentHash,
				Difficulty:   blk.Difficulty,
				Transactions: []string{},
			}

			fm.blockTree.Insert(blk.Height, hash, parentHash, weight, header, &irreversible)
			fm.pendingPayloadStore.SetBlockHeader(hash, header)
			if blk.Complete {
				fm.storedBlocks.MarkStored(hash)
			}
		}
	}

	return len(blocks), nil
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