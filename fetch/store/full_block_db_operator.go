package store

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

func NewFullBlockDbOperator[T any](db *gorm.DB, chainID int64, irreversibleBlocks int, extractStorageFullBlock func(T) *StorageFullBlock) *Operator[T] {
	return NewDbOperator[T](db, chainID, irreversibleBlocks, func(ctx context.Context, db *gorm.DB, chainID int64, blockData T) error {
		if extractStorageFullBlock == nil {
			return errors.New("extract storage full block handler is nil")
		}
		fullBlock := extractStorageFullBlock(blockData)
		if fullBlock == nil {
			return errors.New("block data is nil")
		}
		_, err := StoreFullBlock(ctx, db, chainID, DefaultRuntime(), fullBlock)
		return err
	})
}
