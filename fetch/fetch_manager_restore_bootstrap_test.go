package fetch

import (
	"context"
	"database/sql"
	"testing"

	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	"scanner_eth/model"
)

// C3, I1, I10. FormalVerification.md §3.7.3, I8/I10:
// restore loads 2N+1 blocks so every final N+1 retained node can recompute
// irreversible metadata, then prunes the first N context-only blocks.
func TestRestoreBlockTreeLoadsWindowAndCompleteState(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		// The stale irreversible fields prove restore does not copy DB
		// irreversible metadata into the rebuilt tree.
		{Height: 6, Hash: "0x06", ParentHash: "", Difficulty: "6", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 7, Hash: "0x07", ParentHash: "0x06", Difficulty: "7", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 8, Hash: "0x08", ParentHash: "0x07", Difficulty: "8", Complete: false, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 10, Hash: "0x0A", ParentHash: "0x09", Difficulty: "10", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed blocks failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	blockTree := mustTestBlockTree(t, fm)
	storedBlocks := mustTestStoredBlocks(t, fm)

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load latest blocks failed: %v", err)
	}
	if loaded != 5 {
		t.Fatalf("loaded block count mismatch: got=%v want=5", loaded)
	}

	start, end, ok := blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected loaded height range: ok=%v start=%v end=%v", ok, start, end)
	}

	for _, hash := range []string{"0x06", "0x07"} {
		if blockTree.Get(hash) != nil {
			t.Fatalf("old block %s in recovery window should be pruned after restore", hash)
		}
		if getTestNodeBlockHeader(t, fm, hash) != nil {
			t.Fatalf("context-only block %s should not remain in staging store after restore prune", hash)
		}
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s to be loaded into tree", hash)
		}
	}

	if storedBlocks.IsStored("0x08") {
		t.Fatalf("incomplete block should not be marked stored")
	}
	for _, hash := range []string{"0x06"} {
		if storedBlocks.IsStored(hash) {
			t.Fatalf("pruned block %s should not remain marked stored", hash)
		}
	}
	if !storedBlocks.IsStored("0x07") {
		t.Fatal("parent of retained root should remain marked ready after restore prune")
	}
	if !storedBlocks.IsStored("0x09") || !storedBlocks.IsStored("0x0a") {
		t.Fatalf("complete blocks in recovery window should be marked stored")
	}

	node := blockTree.Get("0x0a")
	if node == nil || node.Weight != 10 {
		t.Fatalf("unexpected stored weight for height 10 node: %+v", node)
	}
	if getTestNodeBlockHeader(t, fm, "0x0a") != nil {
		t.Fatalf("expected nil header after restore until RPC body/header fetch fills it")
	}

	expectedIrreversible := map[string]struct {
		key    string
		height uint64
	}{
		"0x08": {key: "0x06", height: 6},
		"0x09": {key: "0x07", height: 7},
		"0x0a": {key: "0x08", height: 8},
	}
	for hash, want := range expectedIrreversible {
		n := blockTree.Get(hash)
		if n == nil {
			t.Fatalf("expected retained block %s in tree", hash)
		}
		if n.Irreversible.Key != want.key || n.Irreversible.Height != want.height {
			t.Fatalf("retained block %s irreversible mismatch: got=%+v want={Key:%s Height:%d}", hash, n.Irreversible, want.key, want.height)
		}
	}
}

func TestRestoreBlockTreePrunesAvailableShortWindowSuffix(t *testing.T) {
	db := newTestDB(t)
	blocks := []model.Block{
		{Height: 2, Hash: "0x02", ParentHash: "", Difficulty: "2", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 3, Hash: "0x03", ParentHash: "0x02", Difficulty: "3", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 4, Hash: "0x04", ParentHash: "0x03", Difficulty: "4", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
		{Height: 5, Hash: "0x05", ParentHash: "0x04", Difficulty: "5", Complete: true, IrreversibleHeight: 999, IrreversibleHash: "0xstale"},
	}
	if err := db.Create(&blocks).Error; err != nil {
		t.Fatalf("seed short recovery window failed: %v", err)
	}

	fm := newTestFetchManager(t, 2)
	fm.db = db
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	blockTree := mustTestBlockTree(t, fm)
	storedBlocks := mustTestStoredBlocks(t, fm)

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())
	if len(windowBlocks) != 4 {
		t.Fatalf("expected all available short-window rows to load, got=%d", len(windowBlocks))
	}

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("restore short recovery window failed: %v", err)
	}
	if loaded != 4 {
		t.Fatalf("loaded block count mismatch: got=%v want=4", loaded)
	}

	start, end, ok := blockTree.HeightRange()
	if !ok || start != 3 || end != 5 {
		t.Fatalf("unexpected retained short-window suffix: ok=%v start=%v end=%v", ok, start, end)
	}
	if blockTree.Get("0x02") != nil {
		t.Fatal("context-only block 0x02 should be pruned from tree")
	}
	if !storedBlocks.IsStored("0x02") {
		t.Fatal("parent of retained root should remain marked ready after restore prune")
	}
	for _, hash := range []string{"0x03", "0x04", "0x05"} {
		if blockTree.Get(hash) == nil {
			t.Fatalf("expected retained block %s in tree", hash)
		}
		if !storedBlocks.IsStored(hash) {
			t.Fatalf("expected retained complete block %s in stored state", hash)
		}
	}

	root := blockTree.Get("0x03")
	if root == nil || root.Irreversible.Height != 2 || root.Irreversible.Key != "0x02" {
		t.Fatalf("retained root should recompute irreversible from loaded context, got=%+v", root)
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
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	blockTree := mustTestBlockTree(t, fm)
	storedBlocks := mustTestStoredBlocks(t, fm)

	windowBlocks := loadTestBlockWindow(t, fm, context.Background())

	loaded, err := fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("restore with missing off-window parent should succeed: %v", err)
	}
	if loaded != 3 {
		t.Fatalf("loaded block count mismatch: got=%v want=3", loaded)
	}
	for _, hash := range []string{"0x08", "0x09", "0x0a"} {
		if blockTree.Get(hash) == nil {
			t.Fatalf("expected block %s in tree", hash)
		}
	}
	start, end, ok := blockTree.HeightRange()
	if !ok || start != 8 || end != 10 {
		t.Fatalf("unexpected height range: ok=%v start=%v end=%v", ok, start, end)
	}
	if !storedBlocks.IsStored("0x07") {
		t.Fatal("off-window parent of restored root should be marked ready")
	}
}

func TestRestoreBlockTreeHandlesEmptyTableAndHeightZero(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	blockTree := mustTestBlockTree(t, fm)
	storedBlocks := mustTestStoredBlocks(t, fm)

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
	blockTree = mustTestBlockTree(t, fm)
	storedBlocks = mustTestStoredBlocks(t, fm)
	windowBlocks = loadTestBlockWindow(t, fm, context.Background())

	loaded, err = fm.restoreBlockTree(windowBlocks)
	if err != nil {
		t.Fatalf("load height zero block failed: %v", err)
	}
	if loaded != 1 {
		t.Fatalf("expected one block loaded, got=%v", loaded)
	}
	node := blockTree.Get("0xgenesis")
	if node == nil || node.Height != 0 {
		t.Fatalf("expected height zero block in tree, got=%+v", node)
	}
	if !storedBlocks.IsStored("0xgenesis") {
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
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	storedBlocks := mustTestStoredBlocks(t, fm)
	taskPool := mustTestTaskPool(t, fm)
	blockTree := mustTestBlockTree(t, fm)
	storedBlocks.MarkStored("stale")
	taskPool.AddTask("stale")
	remoteCalls := 0
	setTestFetcher(fm, fetcherpkg.NewMockFetcher(
		func(_ context.Context, _ nodepkg.NodeOperator, _ int, height uint64) *fetcherpkg.BlockHeaderJson {
			remoteCalls++
			return makeHeader(height, "0xremote", "")
		},
		nil,
		nil,
	))

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader failed: %v", err)
	}
	if remoteCalls != 0 {
		t.Fatalf("expected db bootstrap to avoid remote calls, got=%v", remoteCalls)
	}
	if mustTestTaskPool(t, fm).HasTask("stale") || mustTestStoredBlocks(t, fm).IsStored("stale") {
		t.Fatalf("runtime state should be reset before db bootstrap")
	}
	blockTree = mustTestBlockTree(t, fm)
	if blockTree.Get("0x20") == nil || blockTree.Get("0x21") == nil || blockTree.Get("0x22") == nil {
		t.Fatalf("expected db bootstrap blocks to populate block tree")
	}
	storedBlocks = mustTestStoredBlocks(t, fm)
	if !storedBlocks.IsStored("0x20") || storedBlocks.IsStored("0x21") != false || !storedBlocks.IsStored("0x22") {
		t.Fatalf("stored markers should follow complete state after db bootstrap")
	}
}

func TestOnBecameLeaderFallsBackToRemoteBootstrapWhenDBEmpty(t *testing.T) {
	db := newTestDB(t)
	fm := newTestFetchManager(t, 2)
	fm.db = db
	op := newRuntimeDbOperator(fm.db, fm.chainId, fm.irreversibleBlocks)
	setTestDbOperator(fm, op)
	setTestScanStartHeight(fm, 30)
	storedBlocks := mustTestStoredBlocks(t, fm)
	taskPool := mustTestTaskPool(t, fm)
	storedBlocks.MarkStored("stale")
	taskPool.AddTask("stale")
	fetchHeaderCalls := 0
	setNodeLatestHeight(fm, 30)
	setTestFetcher(fm, fetcherpkg.NewMockFetcher(
		func(_ context.Context, _ nodepkg.NodeOperator, _ int, height uint64) *fetcherpkg.BlockHeaderJson {
			fetchHeaderCalls++
			if height != 30 {
				t.Fatalf("unexpected header fetch height: %v", height)
			}
			return makeHeader(30, "0x30", "0x29")
		},
		nil,
		nil,
	))

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader fallback failed: %v", err)
	}
	if fetchHeaderCalls != 1 {
		t.Fatalf("expected one remote bootstrap header fetch, got=%v", fetchHeaderCalls)
	}
	if mustTestTaskPool(t, fm).HasTask("stale") || mustTestStoredBlocks(t, fm).IsStored("stale") {
		t.Fatalf("runtime state should be reset before remote bootstrap")
	}
	root := mustTestBlockTree(t, fm).Root()
	if root == nil || root.Height != 30 || root.Key != "0x30" {
		t.Fatalf("expected remote bootstrap root at height 30, got=%+v", root)
	}
	if !mustTestStoredBlocks(t, fm).IsStored("0x29") {
		t.Fatal("remote bootstrap parent should be marked ready")
	}
}

func TestOnBecameLeaderFallsBackToRemoteWhenDBRestoreHasNoRoot(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	setTestScanStartHeight(fm, 40)
	setTestDbOperator(fm, &mockDbOperator{
		loadFn: func(context.Context) ([]model.Block, error) {
			return []model.Block{{Height: 40, Hash: "", ParentHash: "0x39", Complete: true}}, nil
		},
	})

	fetchHeaderCalls := 0
	setTestFetcher(fm, fetcherpkg.NewMockFetcher(
		func(_ context.Context, _ nodepkg.NodeOperator, _ int, height uint64) *fetcherpkg.BlockHeaderJson {
			fetchHeaderCalls++
			if height != 40 {
				t.Fatalf("unexpected header fetch height: %v", height)
			}
			return makeHeader(40, "0x40", "0x39")
		},
		nil,
		nil,
	))

	if err := fm.onBecameLeader(context.Background()); err != nil {
		t.Fatalf("onBecameLeader fallback failed: %v", err)
	}
	if fetchHeaderCalls != 1 {
		t.Fatalf("expected remote fallback after unusable db restore, got calls=%v", fetchHeaderCalls)
	}
	root := mustTestBlockTree(t, fm).Root()
	if root == nil || root.Height != 40 || root.Key != "0x40" {
		t.Fatalf("expected remote bootstrap root at height 40, got=%+v", root)
	}
}

func TestOnBecameLeaderReleasesRuntimeWhenDBAndRemoteBootstrapFail(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	setTestScanStartHeight(fm, 50)
	setTestDbOperator(fm, &mockDbOperator{
		loadFn: func(context.Context) ([]model.Block, error) {
			return []model.Block{{Height: 50, Hash: "", ParentHash: "0x49", Complete: true}}, nil
		},
	})
	setTestFetcher(fm, fetcherpkg.NewMockFetcher(
		func(context.Context, nodepkg.NodeOperator, int, uint64) *fetcherpkg.BlockHeaderJson {
			return nil
		},
		nil,
		nil,
	))

	if err := fm.onBecameLeader(context.Background()); err == nil {
		t.Fatal("expected onBecameLeader to fail when db restore and remote bootstrap both fail")
	}
	if fm.currentRuntime() != nil {
		t.Fatal("runtime should be released when leader bootstrap cannot restore from db or remote")
	}
}
