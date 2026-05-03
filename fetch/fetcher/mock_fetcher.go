package fetcher

import (
	"context"
	"scanner_eth/data"
	nodepkg "scanner_eth/fetch/node"
)

// MockFetcher adapts injected test functions into the shared fetch interface.
type MockFetcher struct {
	fetchHeaderByHeightFn func(context.Context, nodepkg.NodeOperator, int, uint64) *BlockHeaderJson
	fetchHeaderByHashFn   func(context.Context, nodepkg.NodeOperator, int, string) *BlockHeaderJson
	fetchFullBlockFn      func(context.Context, nodepkg.NodeOperator, int, *BlockHeaderJson) *data.FullBlock
}

func NewMockFetcher(
	fetchHeaderByHeightFn func(context.Context, nodepkg.NodeOperator, int, uint64) *BlockHeaderJson,
	fetchHeaderByHashFn func(context.Context, nodepkg.NodeOperator, int, string) *BlockHeaderJson,
	fetchFullBlockFn func(context.Context, nodepkg.NodeOperator, int, *BlockHeaderJson) *data.FullBlock,
) *MockFetcher {
	return &MockFetcher{
		fetchHeaderByHeightFn: fetchHeaderByHeightFn,
		fetchHeaderByHashFn:   fetchHeaderByHashFn,
		fetchFullBlockFn:      fetchFullBlockFn,
	}
}

func (f *MockFetcher) FetchBlockHeaderByHeight(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, height uint64) *BlockHeaderJson {
	if f == nil || f.fetchHeaderByHeightFn == nil {
		return nil
	}
	return f.fetchHeaderByHeightFn(ctx, nodeOp, taskId, height)
}

func (f *MockFetcher) FetchBlockHeaderByHash(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, hash string) *BlockHeaderJson {
	if f == nil || f.fetchHeaderByHashFn == nil {
		return nil
	}
	return f.fetchHeaderByHashFn(ctx, nodeOp, taskId, hash)
}

func (f *MockFetcher) FetchFullBlock(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, header *BlockHeaderJson) *data.FullBlock {
	if f == nil || f.fetchFullBlockFn == nil {
		return nil
	}
	return f.fetchFullBlockFn(ctx, nodeOp, taskId, header)
}
