package store

import (
	"context"
	"database/sql"
	"errors"
	"scanner_eth/model"

	"gorm.io/gorm"
)

type BlockWindowLoader interface {
	LoadBlockWindowFromDB(context.Context) ([]model.Block, error)
}

type BlockDataStorer interface {
	StoreBlockData(context.Context, *EventBlockData) error
}

type DBOperator interface {
	BlockWindowLoader
	BlockDataStorer
}

type Operator struct {
	db                 *gorm.DB
	chainID            int64
	irreversibleBlocks int
}

func NewDbOperator(db *gorm.DB, chainID int64, irreversibleBlocks int) *Operator {
	return &Operator{
		db:                 db,
		chainID:            chainID,
		irreversibleBlocks: irreversibleBlocks,
	}
}

func (op *Operator) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
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
	if op.irreversibleBlocks > 0 {
		windowSpan := uint64(2 * op.irreversibleBlocks)
		if lastHeight > windowSpan {
			minHeight = lastHeight - windowSpan
		}
	}

	var blocks []model.Block
	if err := dbc.Where("height >= ?", minHeight).Order("height asc").Find(&blocks).Error; err != nil {
		return nil, err
	}
	return blocks, nil
}

func (op *Operator) StoreBlockData(ctx context.Context, blockData *EventBlockData) error {
	if op.db == nil {
		return errors.New("db is nil")
	}
	if blockData == nil || blockData.StorageFullBlock == nil {
		return errors.New("block data is nil")
	}
	_, err := StoreFullBlock(ctx, op.db, op.chainID, DefaultRuntime(), blockData.StorageFullBlock)
	return err
}
