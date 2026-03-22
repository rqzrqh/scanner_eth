package fetch

import "github.com/sirupsen/logrus"

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

func (fm *FetchManager) capturePruneStateSnapshot() *pruneStateSnapshot {
	if fm == nil || fm.blockTree == nil {
		return &pruneStateSnapshot{Branches: make([]pruneBranchSnapshot, 0)}
	}

	state := &pruneStateSnapshot{
		Branches: make([]pruneBranchSnapshot, 0),
	}
	root := fm.blockTree.Root()
	if root != nil {
		state.Root = &pruneNodeSnapshot{
			Height: root.Height,
			Hash:   normalizeHash(root.Key),
			Weight: root.Weight,
		}
	}

	branches := fm.blockTree.Branches()
	for _, branch := range branches {
		state.Branches = append(state.Branches, pruneBranchSnapshot{
			Header: pruneNodeSnapshot{
				Height: branch.Header.Height,
				Hash:   normalizeHash(branch.Header.Key),
				Weight: branch.Header.Weight,
			},
			NodeCount: len(branch.Nodes),
		})
	}

	return state
}

func (fm *FetchManager) logPruneSnapshot(stage string, snapshot *pruneStateSnapshot) {
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

func (fm *FetchManager) storedHeightRangeOnTree() (uint64, uint64, bool) {
	linkedNodes := fm.blockTree.LinkedNodes()
	var minHeight uint64
	var maxHeight uint64
	hasStored := false
	for _, nv := range linkedNodes {
		hash := normalizeHash(nv.Key)
		if !fm.storedBlocks.IsStored(hash) {
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

func (fm *FetchManager) pruneStoredBlocks() {
	if fm.irreversibleBlocks <= 0 {
		return
	}

	start, end, ok := fm.storedHeightRangeOnTree()
	if !ok {
		return
	}

	storedSpan := end - start + 1
	if storedSpan <= uint64(fm.irreversibleBlocks) {
		return
	}

	keep := uint64(fm.irreversibleBlocks + 1)
	if storedSpan <= keep {
		return
	}

	pruneCount := storedSpan - keep
	beforeState := fm.capturePruneStateSnapshot()
	prunedNodes := fm.blockTree.Prune(pruneCount)
	if len(prunedNodes) == 0 {
		return
	}
	afterState := fm.capturePruneStateSnapshot()

	for _, nv := range prunedNodes {
		hash := normalizeHash(nv.Key)
		fm.deleteNodeBlockPayload(hash)
		fm.storedBlocks.UnmarkStored(hash)
		fm.taskPool.delTask(hash)
	}

	fm.logPruneSnapshot("before", beforeState)
	logrus.Infof("prune blocktree nodes. pruned:%v keep:%v", len(prunedNodes), keep)
	fm.logPruneSnapshot("after", afterState)
}
