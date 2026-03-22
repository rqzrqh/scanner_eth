package fetch

import (
	"fmt"
	"scanner_eth/model"
	"scanner_eth/protocol"
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
	if err := db.AutoMigrate(&model.Block{}, &model.ScannerInfo{}, &model.ChainBinlog{}); err != nil {
		t.Fatalf("auto migrate store tables failed: %v", err)
	}
	return db
}

func prepareStoreWorkerGlobals() {
	storeTaskMu.Lock()
	defer storeTaskMu.Unlock()

	taskCounter = 0
	batchSize = 128
	storeTaskChannel = make(chan *StoreTask, 4)
	storeCompleteChannel = make(chan *StoreComplete, 4)
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

func makeProtocolBlock(height uint64, hash string, parentHash string) *protocol.FullBlock {
	return &protocol.FullBlock{
		ChainId: 1,
		Block: &protocol.Block{
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
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis", MessageId: 4}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(5, "0x05", "0x04")
	protocolBlock := makeProtocolBlock(5, "0x05", "0x04")

	storedID, err := StoreFullBlock(db, fullblock, protocolBlock)
	if err != nil {
		t.Fatalf("store full block failed: %v", err)
	}
	if storedID == 0 {
		t.Fatalf("expected chain binlog id to be assigned")
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

	var scannerInfo model.ScannerInfo
	if err := db.Where("chain_id = ?", 1).First(&scannerInfo).Error; err != nil {
		t.Fatalf("query scanner info failed: %v", err)
	}
	if scannerInfo.MessageId != 5 {
		t.Fatalf("expected scanner message id update to 5, got=%v", scannerInfo.MessageId)
	}

	var binlogCount int64
	if err := db.Model(&model.ChainBinlog{}).Count(&binlogCount).Error; err != nil {
		t.Fatalf("count chain binlog failed: %v", err)
	}
	if binlogCount != 1 {
		t.Fatalf("expected one chain binlog row, got=%v", binlogCount)
	}
}

func TestStoreFullBlockFailsWhenExistingBlockAlreadyComplete(t *testing.T) {
	db := newStoreWorkerTestDB(t)
	prepareStoreWorkerGlobals()

	seedBlock := model.Block{Height: 6, Hash: "0x06", ParentHash: "0x05", Complete: true}
	if err := db.Create(&seedBlock).Error; err != nil {
		t.Fatalf("seed complete block failed: %v", err)
	}
	if err := db.Create(&model.ScannerInfo{ChainId: 1, GenesisBlockHash: "0xgenesis-2", MessageId: 5}).Error; err != nil {
		t.Fatalf("seed scanner info failed: %v", err)
	}

	fullblock := makeStorageBlock(6, "0x06", "0x05")
	protocolBlock := makeProtocolBlock(6, "0x06", "0x05")

	storedID, err := StoreFullBlock(db, fullblock, protocolBlock)
	if err == nil {
		t.Fatalf("expected store full block to fail when existing block is already complete")
	}
	if storedID != 0 {
		t.Fatalf("expected no chain binlog id on failure, got=%v", storedID)
	}

	var binlogCount int64
	if err := db.Model(&model.ChainBinlog{}).Count(&binlogCount).Error; err != nil {
		t.Fatalf("count chain binlog failed: %v", err)
	}
	if binlogCount != 0 {
		t.Fatalf("expected no chain binlog rows on failure, got=%v", binlogCount)
	}

	var scannerInfo model.ScannerInfo
	if err := db.Where("chain_id = ?", 1).First(&scannerInfo).Error; err != nil {
		t.Fatalf("query scanner info failed: %v", err)
	}
	if scannerInfo.MessageId != 5 {
		t.Fatalf("scanner info should remain unchanged on failure, got=%v", scannerInfo.MessageId)
	}
}