package fetch

import (
	"context"
	"database/sql"
	"errors"
	"scanner_eth/model"

	"gorm.io/gorm"
)

type DbOperator interface {
	LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error)
	StoreBlockData(ctx context.Context, blockData *EventBlockData) error
}

type fetchManagerDbOperator struct {
	db                 *gorm.DB
	chainId            int64
	irreversibleBlocks int
}

func NewDbOperator(db *gorm.DB, chainId int64, irreversibleBlocks int) DbOperator {
	return newFetchManagerDbOperator(db, chainId, irreversibleBlocks)
}

func newFetchManagerDbOperator(db *gorm.DB, chainId int64, irreversibleBlocks int) DbOperator {
	return &fetchManagerDbOperator{db: db, chainId: chainId, irreversibleBlocks: irreversibleBlocks}
}

func (op *fetchManagerDbOperator) LoadBlockWindowFromDB(ctx context.Context) ([]model.Block, error) {
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

func (op *fetchManagerDbOperator) StoreBlockData(ctx context.Context, blockData *EventBlockData) error {
	if op.db == nil {
		return errors.New("db is nil")
	}

	if blockData == nil || blockData.StorageFullBlock == nil {
		return errors.New("block data is nil")
	}

	_, err := StoreFullBlock(ctx, op.db, op.chainId, blockData.StorageFullBlock)
	return err
}
