package store

import (
	"context"
	"scanner_eth/model"
)

type BlockRuntimeDeps[T any] struct {
	LoadBlockWindow func(context.Context) ([]model.Block, error)
	StoreBlock      func(context.Context, T) error
}
