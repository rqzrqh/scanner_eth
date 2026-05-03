package restore

import (
	"context"
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"scanner_eth/util"
	"sort"
	"strconv"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type RuntimeDeps struct {
	BlockTree    *blocktree.BlockTree
	StagingStore *fetchstore.StagingStore
	StoredBlocks *fetchstore.StoredBlockState
	NodeManager  *nodepkg.NodeManager
	Fetcher      fetcherpkg.Fetcher
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

	deps.MarkRootParentReady()
	return len(blocks), nil
}

func (deps RuntimeDeps) MarkRootParentReady() bool {
	if deps.BlockTree == nil || deps.StoredBlocks == nil {
		return false
	}
	root := deps.BlockTree.Root()
	if root == nil {
		return false
	}
	parentHash := util.NormalizeHash(root.ParentKey)
	if parentHash == "" {
		return false
	}
	deps.StoredBlocks.MarkStored(parentHash)
	return true
}

func (deps RuntimeDeps) RestoreRemoteRoot(ctx context.Context, height uint64) bool {
	if deps.BlockTree == nil || deps.NodeManager == nil || deps.Fetcher == nil {
		return false
	}
	if _, _, ok := deps.BlockTree.HeightRange(); ok {
		return deps.BlockTree.Root() != nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	header := deps.fetchRemoteHeaderByHeight(ctx, height)
	if header == nil {
		return false
	}
	headerHeight, ok := decodeHeaderHeight(header)
	if !ok {
		return false
	}
	hash := util.NormalizeHash(header.Hash)
	if hash == "" {
		return false
	}
	parentHash := util.NormalizeHash(header.ParentHash)
	deps.BlockTree.Insert(headerHeight, hash, parentHash, fetcherpkg.HeaderWeight(header))
	if deps.BlockTree.Get(hash) == nil {
		return false
	}
	deps.MarkRootParentReady()
	return true
}

func (deps RuntimeDeps) fetchRemoteHeaderByHeight(ctx context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
	if deps.NodeManager == nil || deps.Fetcher == nil {
		return nil
	}
	for _, nodeOp := range deps.NodeManager.NodeOperators() {
		if header := deps.Fetcher.FetchBlockHeaderByHeight(ctx, nodeOp, 0, height); header != nil {
			return header
		}
	}
	return nil
}

func decodeHeaderHeight(header *fetcherpkg.BlockHeaderJson) (uint64, bool) {
	if header == nil {
		return 0, false
	}
	height, err := hexutil.DecodeUint64(strings.TrimSpace(header.Number))
	return height, err == nil
}
