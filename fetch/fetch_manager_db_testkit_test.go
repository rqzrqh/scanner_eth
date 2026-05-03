package fetch

import (
	"context"
	"fmt"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newRuntimeDbOperator(db *gorm.DB, chainId int64, irreversibleBlocks int) *fetchstore.Operator {
	return fetchstore.NewDbOperator(db, chainId, irreversibleBlocks)
}

func loadTestBlockWindowMaybeError(t *testing.T, fm *FetchManager, ctx context.Context) ([]model.Block, error) {
	t.Helper()
	if fm == nil {
		t.Fatal("fetch manager is nil")
	}
	if fm.dbOperator == nil {
		return nil, fmt.Errorf("db operator is nil")
	}
	return fm.dbOperator.LoadBlockWindowFromDB(ctx)
}

func loadTestBlockWindow(t *testing.T, fm *FetchManager, ctx context.Context) []model.Block {
	t.Helper()
	blocks, err := loadTestBlockWindowMaybeError(t, fm, ctx)
	if err != nil {
		t.Fatalf("load block window failed: %v", err)
	}
	return blocks
}

func newTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.Block{}); err != nil {
		t.Fatalf("auto migrate block failed: %v", err)
	}
	return db
}
