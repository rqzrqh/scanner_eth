package store

import (
	"context"
	"errors"
	"strings"
	"testing"

	"scanner_eth/model"
)

func TestNewDbOperatorAndStoreBlockDataGuards(t *testing.T) {
	if op := NewFullBlockDbOperator(nil, 0, 6); op == nil {
		t.Fatal("NewDbOperator should return non-nil operator")
	}

	op := NewFullBlockDbOperator(nil, 0, 6)
	if _, err := op.LoadBlockWindowFromDB(context.Background()); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}
	if err := op.StoreBlockData(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}

	db := newStoreWorkerTestDB(t)
	op2 := NewFullBlockDbOperator(db, 0, 6)
	if err := op2.StoreBlockData(context.Background(), nil); err == nil || !strings.Contains(err.Error(), "block data is nil") {
		t.Fatalf("expected block data is nil error, got %v", err)
	}
}

func TestLoadBlockWindowFromDBReturnsEarlyWhenContextCancelled(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	if err := db.Create(&model.Block{Height: 1, Hash: "0x01", ParentHash: "", Complete: true}).Error; err != nil {
		t.Fatalf("seed block: %v", err)
	}
	op := NewFullBlockDbOperator(db, 0, 6)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := op.LoadBlockWindowFromDB(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}
