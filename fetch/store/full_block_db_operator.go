package store

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

func NewFullBlockDbOperator(db *gorm.DB, chainID int64, irreversibleBlocks int) *Operator {
	return NewDbOperator(db, chainID, irreversibleBlocks, func(ctx context.Context, db *gorm.DB, chainID int64, blockData *EventBlockData) error {
		if blockData == nil || blockData.StorageFullBlock == nil {
			return errors.New("block data is nil")
		}
		_, err := StoreFullBlock(ctx, db, chainID, DefaultRuntime(), blockData.StorageFullBlock)
		return err
	})
}
