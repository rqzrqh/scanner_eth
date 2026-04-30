package store

import (
	"context"
	"errors"
	"fmt"
	"scanner_eth/model"
	"testing"
	"time"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
)

func newStoreWorkerTestDB(t *testing.T) *gorm.DB {
	t.Helper()
	dsn := fmt.Sprintf("file:%s-%d?mode=memory&cache=shared", t.Name(), time.Now().UnixNano())
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open sqlite db failed: %v", err)
	}
	if err := db.AutoMigrate(&model.Block{}, &model.ScannerInfo{}, &model.ScannerMessage{}, &model.Tx{}); err != nil {
		t.Fatalf("auto migrate store tables failed: %v", err)
	}
	return db
}

func prepareStoreWorkerGlobals() {
	DefaultRuntime().ResetForTest(128, make(chan *Task, 4), make(chan *Complete, 4))
}

// prepareStoreWorkerTestEnv resets global store channels, sets batchSize, and starts workers
// bound to db. Use for tests that exercise StoreFullBlock worker + wait paths.
func prepareStoreWorkerTestEnv(t *testing.T, db *gorm.DB, workerCount int, bs int) {
	t.Helper()
	if workerCount <= 0 {
		workerCount = 2
	}
	if bs <= 0 {
		bs = 128
	}
	DefaultRuntime().ResetForTest(bs, make(chan *Task, workerCount*2), make(chan *Complete, workerCount*2))
	DefaultRuntime().StartWorkers(db, workerCount)
}

func makeStorageBlock(height uint64, hash string, parentHash string) *StorageFullBlock {
	return &StorageFullBlock{
		Block: model.Block{
			Height:     height,
			Hash:       hash,
			ParentHash: parentHash,
		},
	}
}

func TestStoreFullBlockReusesIncompleteExistingBlock(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerGlobals()

	seedBlock := model.Block{Height: 5, Hash: "0x05", ParentHash: "0x04", Complete: false}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis"}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(5, "0x05", "0x04")

	storedID, err := StoreFullBlock(context.Background(), db, 1, DefaultRuntime(), fullblock)
	if err != nil {
		t.Fatalf("store full block failed: %v", err)
	}
	var sm model.ScannerMessage
	if err := db.First(&sm).Error; err != nil {
		t.Fatalf("load scanner_message: %v", err)
	}
	if sm.Id != 1 {
		t.Fatalf("expected scanner_message id 1, got=%v", sm.Id)
	}
	if storedID != sm.Id || storedID == 0 {
		t.Fatalf("expected StoreFullBlock to return row id %v, got=%v", sm.Id, storedID)
	}
	if fullblock.Block.Id != seedBlock.Id {
		t.Fatalf("expected existing block id to be reused, got=%v want=%v", fullblock.Block.Id, seedBlock.Id)
	}

	var storedBlock model.Block
	if err := db.Where("height = ?", 5).First(&storedBlock).Error; err != nil {
		t.Fatalf("query stored block failed: %v", err)
	}
	if storedBlock.Id != seedBlock.Id {
		t.Fatalf("expected same block row after conflict reuse, got=%v want=%v", storedBlock.Id, seedBlock.Id)
	}
	if !storedBlock.Complete {
		t.Fatalf("expected reused block row to be marked complete")
	}

	var msgCount int64
	if err := db.Model(&model.ScannerMessage{}).Count(&msgCount).Error; err != nil {
		t.Fatalf("count scanner_message failed: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("expected one scanner_message row, got=%v", msgCount)
	}
}

func TestStoreFullBlockFailsWhenExistingBlockAlreadyComplete(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerGlobals()

	seedBlock := model.Block{Height: 6, Hash: "0x06", ParentHash: "0x05", Complete: true}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed complete block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis-2"}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(6, "0x06", "0x05")

	storedID, err := StoreFullBlock(context.Background(), db, 1, DefaultRuntime(), fullblock)
	if err == nil {
		t.Fatalf("expected store full block to fail when existing block is already complete")
	}
	if storedID != 0 {
		t.Fatalf("expected no message id on failure, got=%v", storedID)
	}

	var msgCount int64
	if err := db.Model(&model.ScannerMessage{}).Count(&msgCount).Error; err != nil {
		t.Fatalf("count scanner_message failed: %v", err)
	}
	if msgCount != 0 {
		t.Fatalf("expected no scanner_message rows on failure, got=%v", msgCount)
	}

}

func TestScannerMessageSchemaUsesUniqueHashOnly(t *testing.T) {
	db := newStoreWorkerTestDB(t)

	first := model.ScannerMessage{Height: 100, Hash: "0x100", ParentHash: "0x099"}
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("seed scanner_message failed: %v", err)
	}

	sameHeightNewHash := model.ScannerMessage{Height: 100, Hash: "0x101", ParentHash: "0x100"}
	if err := db.Create(&sameHeightNewHash).Error; err != nil {
		t.Fatalf("expected duplicate scanner_message height with new hash to succeed, got: %v", err)
	}

	dupHash := model.ScannerMessage{Height: 101, Hash: "0x100", ParentHash: "0x099"}
	if err := db.Create(&dupHash).Error; err == nil {
		t.Fatal("expected duplicate scanner_message hash to fail")
	}
}

func TestTxInternalSchemaUsesUniqueBlockIDTxHashAndIndex(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	if err := db.AutoMigrate(&model.TxInternal{}); err != nil {
		t.Fatalf("auto migrate tx_internal failed: %v", err)
	}

	first := model.TxInternal{BlockId: 1, Height: 100, TxHash: "0xtx", TraceAddress: "call", Index: 0}
	if err := db.Create(&first).Error; err != nil {
		t.Fatalf("seed tx_internal failed: %v", err)
	}

	sameTxNewIndex := model.TxInternal{BlockId: 1, Height: 100, TxHash: "0xtx", TraceAddress: "call_0", Index: 1}
	if err := db.Create(&sameTxNewIndex).Error; err != nil {
		t.Fatalf("expected duplicate block_id and tx_hash with new index to succeed, got: %v", err)
	}

	sameTxNewBlock := model.TxInternal{BlockId: 2, Height: 100, TxHash: "0xtx", TraceAddress: "call", Index: 0}
	if err := db.Create(&sameTxNewBlock).Error; err != nil {
		t.Fatalf("expected duplicate tx_hash and index in different block to succeed, got: %v", err)
	}

	dup := model.TxInternal{BlockId: 1, Height: 100, TxHash: "0xtx", TraceAddress: "call_1", Index: 0}
	if err := db.Create(&dup).Error; err == nil {
		t.Fatal("expected duplicate tx_internal (block_id, tx_hash, index) to fail")
	}
}

func TestTxInternalSchemaRequiresNonNullUniqueKeyColumns(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	if err := db.AutoMigrate(&model.TxInternal{}); err != nil {
		t.Fatalf("auto migrate tx_internal failed: %v", err)
	}

	if err := db.Exec("INSERT INTO tx_internal (block_id, height, `index`) VALUES (?, ?, ?)", 1, 100, 0).Error; err == nil {
		t.Fatal("expected tx_internal insert with null tx_hash to fail")
	}

	if err := db.Exec("INSERT INTO tx_internal (block_id, height, tx_hash) VALUES (?, ?, ?)", 1, 100, "0xtx").Error; err == nil {
		t.Fatal("expected tx_internal insert with null index to fail")
	}
}

func TestStoreFullBlockReusesExistingScannerMessageByHash(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerGlobals()

	seedBlock := model.Block{Height: 7, Hash: "0x07", ParentHash: "0x06", Complete: false}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis-7"}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}
	existingMsg := model.ScannerMessage{Height: 7, Hash: "0x07", ParentHash: "0x06", Pushed: false}
	if err := db.Create(&existingMsg).Error; err != nil {
		t.Fatalf("seed scanner_message failed: %v", err)
	}

	fullblock := makeStorageBlock(7, "0x07", "0x06")

	storedID, err := StoreFullBlock(context.Background(), db, 1, DefaultRuntime(), fullblock)
	if err != nil {
		t.Fatalf("store full block failed: %v", err)
	}
	if storedID != existingMsg.Id {
		t.Fatalf("expected existing scanner_message id %v, got %v", existingMsg.Id, storedID)
	}

	var msgCount int64
	if err := db.Model(&model.ScannerMessage{}).Count(&msgCount).Error; err != nil {
		t.Fatalf("count scanner_message failed: %v", err)
	}
	if msgCount != 1 {
		t.Fatalf("expected one scanner_message row, got=%v", msgCount)
	}

	var storedBlock model.Block
	if err := db.Where("id = ?", seedBlock.Id).First(&storedBlock).Error; err != nil {
		t.Fatalf("query stored block failed: %v", err)
	}
	if !storedBlock.Complete {
		t.Fatal("expected block to be marked complete when scanner_message already exists")
	}
}

func TestStoreFullBlockReturnsEarlyWhenContextAlreadyCancelled(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerGlobals()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	fullblock := makeStorageBlock(1, "0x01", "")
	_, err := StoreFullBlock(ctx, db, 1, DefaultRuntime(), fullblock)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// TestStoreFullBlockManyTasksSmallChannelNoDeadlock guards the interleaved
// submit/receive loop: with batchSize=1 there are far more StoreTasks than
// cap(storeTaskChannel); the legacy "enqueue all tasks then wait" ordering
// deadlocked when buffers were small.
func TestStoreFullBlockManyTasksSmallChannelNoDeadlock(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerTestEnv(t, db, 2, 1)

	seedBlock := model.Block{Height: 11, Hash: "0x0b", ParentHash: "0x0a", Complete: false}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgen11"}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(11, "0x0b", "0x0a")
	const nTx = 64
	fullblock.TxList = make([]model.Tx, nTx)
	for i := range fullblock.TxList {
		fullblock.TxList[i] = model.Tx{
			Height:  11,
			TxHash:  fmt.Sprintf("0x%064x", uint64(i)+0xbb0000),
			TxIndex: i,
		}
	}
	errCh := make(chan error, 1)
	go func() {
		_, err := StoreFullBlock(context.Background(), db, 1, DefaultRuntime(), fullblock)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("StoreFullBlock: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("StoreFullBlock did not finish (possible channel deadlock)")
	}

	var storedBlock model.Block
	if err := db.Where("height = ?", 11).First(&storedBlock).Error; err != nil {
		t.Fatalf("query stored block failed: %v", err)
	}
	if !storedBlock.Complete {
		t.Fatal("expected block marked complete after successful store")
	}

	var txCount int64
	if err := db.Model(&model.Tx{}).Where("height = ?", 11).Count(&txCount).Error; err != nil {
		t.Fatalf("count tx failed: %v", err)
	}
	if txCount != nTx {
		t.Fatalf("expected %d tx rows, got %v", nTx, txCount)
	}

}

func TestStoreFullBlockSkipsFinalizeWhenContextCancelledDuringWorkers(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerTestEnv(t, db, 1, 1)

	seedBlock := model.Block{Height: 9, Hash: "0x09", ParentHash: "0x08", Complete: false}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis9"}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(9, "0x09", "0x08")
	fullblock.TxList = make([]model.Tx, 12)
	for i := range fullblock.TxList {
		fullblock.TxList[i] = model.Tx{
			Height:  9,
			TxHash:  fmt.Sprintf("0x%064x", uint64(i)+1),
			TxIndex: i,
		}
	}
	entered := make(chan struct{})
	unblock := make(chan struct{})
	DefaultRuntime().SetAfterFirstTaskSendHook(func() {
		close(entered)
		<-unblock
	})
	defer DefaultRuntime().ClearAfterFirstTaskSendHook()

	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		_, err := StoreFullBlock(ctx, db, 1, DefaultRuntime(), fullblock)
		errCh <- err
	}()

	<-entered
	cancel()
	close(unblock)

	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("expected context.Canceled, got %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("timeout waiting for StoreFullBlock")
	}

	var storedBlock model.Block
	if err := db.Where("height = ?", 9).First(&storedBlock).Error; err != nil {
		t.Fatalf("query stored block failed: %v", err)
	}
	if storedBlock.Complete {
		t.Fatal("expected block to stay incomplete when store is cancelled before finalize")
	}

	var msgCount int64
	if err := db.Model(&model.ScannerMessage{}).Count(&msgCount).Error; err != nil {
		t.Fatalf("count scanner_message failed: %v", err)
	}
	if msgCount != 0 {
		t.Fatalf("expected no scanner_message when finalize is skipped, got %v", msgCount)
	}

}
