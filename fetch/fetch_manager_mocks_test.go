package fetch

import (
	"context"
	"scanner_eth/data"
	"scanner_eth/model"

	fetcherpkg "scanner_eth/fetch/fetcher"
)

type mockDbOperator struct {
	loadFn  func(context.Context) ([]model.Block, error)
	storeFn func(context.Context, *EventBlockData) error
}

func (m *mockDbOperator) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
	if m != nil && m.loadFn != nil {
		return m.loadFn(ctx)
	}
	return nil, nil
}

func (m *mockDbOperator) StoreBlockData(ctx context.Context, blockData *EventBlockData) error {
	if m != nil && m.storeFn != nil {
		return m.storeFn(ctx, blockData)
	}
	return nil
}

type mockBlockFetcher struct {
	fetchByHeightFn func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson
	fetchByHashFn   func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson
	fetchFullFn     func(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock
}

func (m *mockBlockFetcher) FetchBlockHeaderByHeight(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
	if m != nil && m.fetchByHeightFn != nil {
		return m.fetchByHeightFn(ctx, nodeOp, taskID, height)
	}
	return nil
}

func (m *mockBlockFetcher) FetchBlockHeaderByHash(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson {
	if m != nil && m.fetchByHashFn != nil {
		return m.fetchByHashFn(ctx, nodeOp, taskID, hash)
	}
	return nil
}

func (m *mockBlockFetcher) FetchFullBlock(ctx context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
	if m != nil && m.fetchFullFn != nil {
		return m.fetchFullFn(ctx, nodeOp, taskID, header)
	}
	return nil
}
