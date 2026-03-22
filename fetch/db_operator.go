package fetch

import (
	"database/sql"
	"errors"
	"scanner_eth/model"

	"gorm.io/gorm"
)

type DbOperator interface {
	LoadBlockWindowFromDB() ([]model.Block, error)
	StoreBlockData(blockData *EventBlockData) error
}

type fetchManagerDbOperator struct {
	db                 *gorm.DB
	irreversibleBlocks int
}

func NewDbOperator(db *gorm.DB, irreversibleBlocks int) DbOperator {
	return newFetchManagerDbOperator(db, irreversibleBlocks)
}

func newFetchManagerDbOperator(db *gorm.DB, irreversibleBlocks int) DbOperator {
	return &fetchManagerDbOperator{db: db, irreversibleBlocks: irreversibleBlocks}
}

func (op *fetchManagerDbOperator) LoadBlockWindowFromDB() ([]model.Block, error) {
	if op.db == nil {
		return nil, errors.New("db is nil")
	}

	var maxHeight sql.NullInt64
	if err := op.db.Model(&model.Block{}).Select("MAX(height)").Scan(&maxHeight).Error; err != nil {
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
	if err := op.db.Where("height >= ?", minHeight).Order("height asc").Find(&blocks).Error; err != nil {
		return nil, err
	}
	return blocks, nil
}

func (op *fetchManagerDbOperator) StoreBlockData(blockData *EventBlockData) error {
	if op.db == nil {
		return errors.New("db is nil")
	}

	if blockData == nil || blockData.StorageFullBlock == nil || blockData.ProtocolFullBlock == nil {
		return errors.New("block data is nil")
	}

	_, err := StoreFullBlock(op.db, blockData.StorageFullBlock, blockData.ProtocolFullBlock)
	return err
}
