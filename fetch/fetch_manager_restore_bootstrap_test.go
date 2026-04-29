package fetch

import (
	"context"
	"database/sql"
	"testing"

	"scanner_eth/model"
	fetcherpkg "scanner_eth/fetch/fetcher"
)

// C3, I1. FormalVerification.md §3.7.3, I8: Complete=true rows MarkStored; incomplete do not. DB row integrity is not revalidated on restore.
func TestRestoreBlockTreeLoadsWindowAndCompleteState(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 7, Hash: "0x07", ParentHash: "0x06", Difficulty: "7", Complete: true},
		{Height: 8, Hash: "0x08", ParentHash: "", Difficulty: "8", Complete: false},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true},
		{Height: 10, Hash: "0x0A", ParentHash: "0x09", Difficulty: "10", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	setTestDbOperator(fm, newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks))

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load latest blocks failed: %v", err)
	}
	if loaded != 3 {
		t.Fatalf("loaded block count mismatch: got=%v want=3", loaded)
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected loaded height range: ok=%v start=%v end=%v", ok, start, end)
	}

	if fm.blockTree.Get("0x07") != nil {
		t.Fatalf("block below recovery window should not be loaded")
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if fm.blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s to be loaded into tree", hash)
		}
	}

	if fm.storedBlocks.IsStored("0x08") {
		t.Fatalf("incomplete block should not be marked stored")
	}
	if !fm.storedBlocks.IsStored("0x09") || !fm.storedBlocks.IsStored("0x0a") {
		t.Fatalf("complete blocks in recovery window should be marked stored")
	}

	node := fm.blockTree.Get("0x0a")
	if node == nil || node.Weight != 10 {
		t.Fatalf("unexpected stored weight for height 10 node: %+v", node)
	}
	if getTestNodeBlockHeader(t, fm, "0x0a") != nil {
		t.Fatalf("expected nil header after restore until RPC body/header fetch fills it")
	}
	if node.Irreversible.Key == "" {
		t.Fatalf("expected non-empty irreversible key on inserted node")
	}
}

func TestRestoreBlockTreeInsertsWhenParentMissingOutsideWindow(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 8, Hash: "0x08", ParentHash: "0x07", Difficulty: "8", Complete: true},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true},
		{Height: 10, Hash: "0x0A", ParentHash: "0x09", Difficulty: "10", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	setTestDbOperator(fm, newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks))

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("restore with missing off-window parent should succeed: %v", err)
	}
	if loaded != 3 {
		t.Fatalf("loaded block count mismatch: got=%v want=3", loaded)
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if fm.blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s in tree", hash)
		}
	}
	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected height range: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestRestoreBlockTreeHandlesEmptyTableAndHeightZero(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	setTestDbOperator(fm, newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks))

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load empty table failed: %v", err)
	}
	if loaded != 0 {
		t.Fatalf("expected no blocks loaded from empty table, got=%v", loaded)
	}

	block := model.Block{Height: 0, Hash: "0xgenesis", ParentHash: "", Difficulty: "11", Complete: true}
	if err := db.Create(&block).Error; err != nil {
		t.Fatalf("seed height zero block failed: %v", err)
	}

	fm.deleteRuntimeState()
	fm.createRuntimeState()
	windowBlocks = loadTestBlockWindow(t, fm, context.Background())

	loaded, err = fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load height zero block failed: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("expected one block loaded, got=%v", loaded)
	}
	node := fm.blockTree.Get("0xgenesis")
	if node == nil || node.Height != 0 {
		t.Fatalf("expected height zero block in tree, got=%+v", node)
	}
	if !fm.storedBlocks.IsStored("0xgenesis") {
		t.Fatalf("complete height zero block should be marked stored")
	}

	var maxHeight sql.NullInt64
	if err := db.Model(&model.Block{}).Select("MAX(height)").Scan(&maxHeight).Error; err != nil {
		t.Fatalf("query max height failed: %v", err)
	}
	if !maxHeight.Valid || maxHeight.Int64 != 0 {
		t.Fatalf("expected valid max height 0, got valid=%v value=%v", maxHeight.Valid, maxHeight.Int64)
	}
}

func TestOnBecameLeaderUsesDBBootstrapWithoutRemoteFetch(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 20, Hash: "0x20", ParentHash: "", Difficulty: "20", Complete: true},
		{Height: 21, Hash: "0x21", ParentHash: "0x20", Difficulty: "21", Complete: false},
		{Height: 22, Hash: "0x22", ParentHash: "0x21", Difficulty: "22", Complete: true},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed bootstrap blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	setTestDbOperator(fm, newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks))
	fm.storedBlocks.MarkStored("stale")
	fm.taskPool.AddTask("stale")
	remoteCalls := 0
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		remoteCalls++
		return makeHeader(height, "0xremote", "")
	}})

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader failed: %v", err)
	}
	if remoteCalls != 0 {
		t.Fatalf("expected db bootstrap to avoid remote calls, got=%v", remoteCalls)
	}
	if fm.taskPool.HasTask("stale") || fm.storedBlocks.IsStored("stale") {
		t.Fatalf("runtime state should be reset before db bootstrap")
	}
	if fm.blockTree.Get("0x20") == nil || fm.blockTree.Get("0x21") == nil || fm.blockTree.Get("0x22") == nil {
		t.Fatalf("expected db bootstrap blocks to populate block tree")
	}
	if !fm.storedBlocks.IsStored("0x20") || fm.storedBlocks.IsStored("0x21") != false || !fm.storedBlocks.IsStored("0x22") {
		t.Fatalf("stored markers should follow complete state after db bootstrap")
	}
}

func TestOnBecameLeaderFallsBackToRemoteBootstrapWhenDBEmpty(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	setTestDbOperator(fm, newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks))
	setTestScanStartHeight(fm, 30)
	fm.storedBlocks.MarkStored("stale")
	fm.taskPool.AddTask("stale")
	fetchHeaderCalls := 0
	setNodeLatestHeight(fm, 30)
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		fetchHeaderCalls++
		if height != 30 {
			t.Fatalf("unexpected header fetch height: %v", height)
		}
		return makeHeader(30, "0x30", "0x29")
	}})

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader fallback failed: %v", err)
	}
	if fetchHeaderCalls != 1 {
		t.Fatalf("expected one remote bootstrap header fetch, got=%v", fetchHeaderCalls)
	}
	if fm.taskPool.HasTask("stale") || fm.storedBlocks.IsStored("stale") {
		t.Fatalf("runtime state should be reset before remote bootstrap")
	}
	root := fm.blockTree.Root()
	if root == nil || root.Height != 30 || root.Key != "0x30" {
		t.Fatalf("expected remote bootstrap root at height 30, got=%+v", root)
	}
}
