package fetch

import (
	"context"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func (fm *FetchManager) syncNodeDataByHash(ctx context.Context, hash string) bool {
	if ctx == nil {
		ctx = context.Background()
	}
	hash = normalizeHash(hash)
	if hash == "" {
		return true
	}

	node := fm.blockTree.Get(hash)
	if node == nil {
		return true
	}

	header := fm.getNodeBlockHeader(hash)
	if header == nil {
		_, nodeOp, err := fm.nodeManager.GetBestNode(0)
		if err != nil {
			return false
		}
		header = fm.blockFetcher.FetchBlockHeaderByHash(ctx, nodeOp, int(node.Height), hash)
		if header == nil {
			return false
		}
		fm.setNodeBlockHeader(hash, header)
	}

	height, err := hexutil.DecodeUint64(header.Number)
	if err != nil {
		height = node.Height
	}

	_, nodeOp, err := fm.nodeManager.GetBestNode(height)
	if err != nil {
		return false
	}

	startTime := time.Now()
	fullBlock := fm.blockFetcher.FetchFullBlock(ctx, nodeOp, int(height), header)
	costTime := time.Since(startTime)
	if fullBlock == nil {
		fm.nodeManager.UpdateNodeState(nodeOp.ID(), costTime.Microseconds(), false)
		return false
	}

	blockData := &EventBlockData{
		StorageFullBlock: ConvertStorageFullBlock(fullBlock, node.Irreversible),
	}
	fm.setNodeBlockBody(hash, blockData)
	fm.nodeManager.UpdateNodeState(nodeOp.ID(), costTime.Microseconds(), true)
	return true
}

func (fm *FetchManager) insertHeader(header *BlockHeaderJson) {
	if header == nil {
		return
	}

	height, err := hexutil.DecodeUint64(header.Number)
	if err != nil {
		return
	}

	weight := headerWeight(header)
	key := normalizeHash(header.Hash)
	parentKey := normalizeHash(header.ParentHash)
	fm.blockTree.Insert(height, key, parentKey, weight, header, nil)
	fm.pendingPayloadStore.SetBlockHeader(key, header)
}

func headerWeight(header *BlockHeaderJson) uint64 {
	if header == nil || header.Difficulty == "" {
		return 0
	}
	v, err := hexutil.DecodeBig(header.Difficulty)
	if err != nil || v == nil || v.Sign() <= 0 {
		return 0
	}
	if !v.IsUint64() {
		return ^uint64(0)
	}
	return v.Uint64()
}
