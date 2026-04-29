package store

import (
	"context"
	"scanner_eth/blocktree"
	"scanner_eth/model"
	"sort"
	"strconv"
	"strings"

	"github.com/sirupsen/logrus"
)

type TreeTaskPool interface {
	DelTask(string)
}

type TreeRuntimeDeps struct {
	BlockTree    *blocktree.BlockTree
	PayloadStore *PayloadStore
	StoredBlocks *StoredBlockState
	TaskPool     TreeTaskPool
}

type PruneNodeSnapshot struct {
	Height uint64 `json:"height"`
	Hash   string `json:"hash"`
	Weight uint64 `json:"weight"`
}

type PruneBranchSnapshot struct {
	Header    PruneNodeSnapshot `json:"header"`
	NodeCount int               `json:"node_count"`
}

type PruneStateSnapshot struct {
	Root     *PruneNodeSnapshot    `json:"root"`
	Branches []PruneBranchSnapshot `json:"branches"`
}

type restoreBranch struct {
	nodes  []*model.Block
	weight uint64
}

func (deps TreeRuntimeDeps) normalizeHash(hash string) string {
	return normalizeHash(hash)
}

func (deps TreeRuntimeDeps) parseWeight(v string) uint64 {
	return ParseStoredBlockWeight(v)
}

func ParseStoredBlockWeight(difficulty string) uint64 {
	if strings.TrimSpace(difficulty) == "" {
		return 0
	}
	v, err := strconv.ParseUint(difficulty, 10, 64)
	if err != nil {
		return 0
	}
	return v
}

func (deps TreeRuntimeDeps) CapturePruneStateSnapshot() *PruneStateSnapshot {
	if deps.BlockTree == nil {
		return &PruneStateSnapshot{Branches: make([]PruneBranchSnapshot, 0)}
	}

	state := &PruneStateSnapshot{
		Branches: make([]PruneBranchSnapshot, 0),
	}
	root := deps.BlockTree.Root()
	if root != nil {
		state.Root = &PruneNodeSnapshot{
			Height: root.Height,
			Hash:   deps.normalizeHash(root.Key),
			Weight: root.Weight,
		}
	}

	for _, branch := range deps.BlockTree.Branches() {
		state.Branches = append(state.Branches, PruneBranchSnapshot{
			Header: PruneNodeSnapshot{
				Height: branch.Header.Height,
				Hash:   deps.normalizeHash(branch.Header.Key),
				Weight: branch.Header.Weight,
			},
			NodeCount: len(branch.Nodes),
		})
	}

	return state
}

func (deps TreeRuntimeDeps) StoredHeightRangeOnTree() (uint64, uint64, bool) {
	if deps.BlockTree == nil || deps.StoredBlocks == nil {
		return 0, 0, false
	}
	linkedNodes := deps.BlockTree.LinkedNodes()
	var minHeight uint64
	var maxHeight uint64
	hasStored := false
	for _, nv := range linkedNodes {
		hash := deps.normalizeHash(nv.Key)
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

func (deps TreeRuntimeDeps) PruneStoredBlocks(ctx context.Context, irreversibleBlocks int) {
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

	storedSpan := end - start + 1
	if storedSpan <= uint64(irreversibleBlocks) {
		return
	}

	keep := uint64(irreversibleBlocks + 1)
	if storedSpan <= keep {
		return
	}

	pruneCount := storedSpan - keep
	beforeState := deps.CapturePruneStateSnapshot()
	if ctx != nil {
		select {
		case <-ctx.Done():
			return
		default:
		}
	}
	prunedNodes := deps.BlockTree.Prune(pruneCount)
	if len(prunedNodes) == 0 {
		return
	}
	afterState := deps.CapturePruneStateSnapshot()

	for _, nv := range prunedNodes {
		hash := deps.normalizeHash(nv.Key)
		if deps.PayloadStore != nil {
			deps.PayloadStore.DeletePayload(hash)
		}
		if deps.StoredBlocks != nil {
			deps.StoredBlocks.UnmarkStored(hash)
		}
		if deps.TaskPool != nil {
			deps.TaskPool.DelTask(hash)
		}
	}

	LogPruneSnapshot("before", beforeState)
	logrus.Infof("prune blocktree nodes. pruned:%v keep:%v", len(prunedNodes), keep)
	LogPruneSnapshot("after", afterState)
}

func (deps TreeRuntimeDeps) RestoreBlockTree(blocks []model.Block) (int, error) {
	if len(blocks) == 0 {
		return 0, nil
	}
	if deps.BlockTree == nil {
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

	branches := make([]restoreBranch, 0)
	var dfs func(path []*model.Block, cur *model.Block, accWeight uint64)
	dfs = func(path []*model.Block, cur *model.Block, accWeight uint64) {
		path = append(path, cur)
		accWeight += deps.parseWeight(cur.Difficulty)

		children := childrenByParent[deps.normalizeHash(cur.Hash)]
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
		return deps.normalizeHash(iLast.Hash) < deps.normalizeHash(jLast.Hash)
	})

	for _, branch := range branches {
		for _, blk := range branch.nodes {
			hash := deps.normalizeHash(blk.Hash)
			if hash == "" || deps.BlockTree.Get(hash) != nil {
				continue
			}

			parentHash := deps.normalizeHash(blk.ParentHash)
			weight := deps.parseWeight(blk.Difficulty)
			irreversible := blocktree.IrreversibleNode{Height: blk.IrreversibleHeight, Key: deps.normalizeHash(blk.IrreversibleHash)}
			if irreversible.Key == "" {
				irreversible = blocktree.IrreversibleNode{Height: blk.Height, Key: hash}
			}

			deps.BlockTree.Insert(blk.Height, hash, parentHash, weight, &irreversible)
			if deps.PayloadStore != nil && deps.BlockTree.Get(hash) != nil {
				deps.PayloadStore.SetHeader(hash, nil)
			}
			if blk.Complete && deps.StoredBlocks != nil {
				deps.StoredBlocks.MarkStored(hash)
			}
		}
	}

	return len(blocks), nil
}

func LogPruneSnapshot(stage string, snapshot *PruneStateSnapshot) {
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
