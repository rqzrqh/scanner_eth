package store

import (
	"context"
	"database/sql"
	"errors"
	"scanner_eth/model"

	"gorm.io/gorm"
)

type StoreBlockFunc[T any] func(context.Context, *gorm.DB, int64, T) error

type Operator[T any] struct {
	db                 *gorm.DB
	chainID            int64
	irreversibleBlocks int
	storeBlock         StoreBlockFunc[T]
}

func NewDbOperator[T any](db *gorm.DB, chainID int64, irreversibleBlocks int, storeBlock StoreBlockFunc[T]) *Operator[T] {
	return &Operator[T]{
		db:                 db,
		chainID:            chainID,
		irreversibleBlocks: irreversibleBlocks,
		storeBlock:         storeBlock,
	}
}

func (op *Operator[T]) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
	if op.db == nil {
		return nil, errors.New("db is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	dbc := op.db.WithContext(ctx)

	var maxHeight sql.NullInt64
	if err := dbc.Model(&model.Block{}).Select("MAX(height)").Scan(&maxHeight).Error; err != nil {
		return nil, err
	}
	if !maxHeight.Valid {
		return nil, nil
	}

	lastHeight := uint64(maxHeight.Int64)
	minHeight := uint64(0)
	if op.irreversibleBlocks > 0 && lastHeight > uint64(op.irreversibleBlocks) {
		minHeight = lastHeight - uint64(op.irreversibleBlocks)
	}

	var blocks []model.Block
	if err := dbc.Where("height >= ?", minHeight).Order("height asc").Find(&blocks).Error; err != nil {
		return nil, err
	}
	return blocks, nil
}

func (op *Operator[T]) StoreBlockData(ctx context.Context, blockData T) error {
	if op.db == nil {
		return errors.New("db is nil")
	}
	if op.storeBlock == nil {
		return errors.New("store block handler is nil")
	}
	return op.storeBlock(ctx, op.db, op.chainID, blockData)
}
