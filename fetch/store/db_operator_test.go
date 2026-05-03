package store

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"

	"scanner_eth/model"
)

func TestNewDbOperatorAndStoreBlockDataGuards(t *testing.T) {
	if op := NewDbOperator(nil, 0, 6); op == nil {
		t.Fatal("NewDbOperator should return non-nil operator")
	}

	op := NewDbOperator(nil, 0, 6)
	if _, err := op.LoadBlockWindowFromDB(context.Background()); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}
	if err := op.StoreBlockData(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}

	db := newStoreWorkerTestDB(t)
	op2 := NewDbOperator(db, 0, 6)
	if err := op2.StoreBlockData(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "block data is nil") {
		t.Fatalf("expected block data is nil error, got %v", err)
	}
}

func TestLoadBlockWindowFromDBReturnsEarlyWhenContextCancelled(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	if err := db.Create(&model.Block{Height: 1, Hash: "0x01", ParentHash: "", Complete: true}).Error; err != nil {
		t.Fatalf("seed block: %v", err)
	}
	op := NewDbOperator(db, 0, 6)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := op.LoadBlockWindowFromDB(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

func TestLoadBlockWindowFromDBLoadsDoubleIrreversiblePlusOne(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	for h := uint64(1); h <= 10; h++ {
		block := model.Block{Height: h, Hash: "0x" + strconv.FormatUint(h, 16), ParentHash: "", Complete: true}
		if err := db.Create(&block).Error; err != nil {
			t.Fatalf("seed block height %d: %v", h, err)
		}
	}

	op := NewDbOperator(db, 0, 2)
	blocks, err := op.LoadBlockWindowFromDB(context.Background())
	if err != nil {
		t.Fatalf("LoadBlockWindowFromDB failed: %v", err)
	}
	if len(blocks) != 5 {
		t.Fatalf("expected 2N+1 blocks, got %d", len(blocks))
	}
	if blocks[0].Height != 6 || blocks[len(blocks)-1].Height != 10 {
		t.Fatalf("unexpected recovery window heights: first=%d last=%d", blocks[0].Height, blocks[len(blocks)-1].Height)
	}
}
