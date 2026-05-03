package fetch

import (
	"context"
	"scanner_eth/model"
	fetchstore "scanner_eth/fetch/store"
)

type mockDbOperator struct {
	loadFn  func(context.Context) ([]model.Block, error)
	storeFn func(context.Context, *fetchstore.EventBlockData) error
}

func (m *mockDbOperator) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
	if m != nil && m.loadFn != nil {
		return m.loadFn(ctx)
	}
	return nil, nil
}

func (m *mockDbOperator) StoreBlockData(ctx context.Context, blockData *fetchstore.EventBlockData) error {
	if m != nil && m.storeFn != nil {
		return m.storeFn(ctx, blockData)
	}
	return nil
}
