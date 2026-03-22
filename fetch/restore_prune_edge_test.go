package fetch

import (
	"testing"

	"scanner_eth/model"
)

func TestCapturePruneStateSnapshotNilReceiverAndLogPaths(t *testing.T) {
	var nilFM *FetchManager
	s := nilFM.capturePruneStateSnapshot()
	if s == nil {
		t.Fatal("snapshot should not be nil")
	}
	if s.Root != nil || len(s.Branches) != 0 {
		t.Fatalf("unexpected snapshot for nil fm: %+v", s)
	}

	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	// Ensure log branches execute without panic.
	fm.logPruneSnapshot("nil", nil)
	fm.logPruneSnapshot("empty", &pruneStateSnapshot{})
}

func TestPruneStoredBlocksIrreversibleDisabledNoop(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })
	fm.irreversibleBlocks = 0

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.storedBlocks.MarkStored("a")
	fm.taskPool.addTask("a")

	fm.pruneStoredBlocks()
	if fm.blockTree.Get("a") == nil || !fm.storedBlocks.IsStored("a") || !fm.taskPool.hasTask("a") {
		t.Fatal("prune should be noop when irreversibleBlocks <= 0")
	}
}

func TestParseStoredBlockWeightEdges(t *testing.T) {
	if got := parseStoredBlockWeight(""); got != 0 {
		t.Fatalf("expected 0 for empty difficulty, got=%d", got)
	}
	if got := parseStoredBlockWeight("   "); got != 0 {
		t.Fatalf("expected 0 for blank difficulty, got=%d", got)
	}
	if got := parseStoredBlockWeight("not-number"); got != 0 {
		t.Fatalf("expected 0 for parse error, got=%d", got)
	}
	if got := parseStoredBlockWeight("123"); got != 123 {
		t.Fatalf("expected 123, got=%d", got)
	}
}

func TestRestoreBlockTreeSkipsInvalidHashAndRepeatedRestore(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	blocks := []model.Block{
		{Height: 1, Hash: "", ParentHash: "", Difficulty: "1", Complete: true},
		{Height: 1, Hash: "0x01", ParentHash: "", Difficulty: "1", Complete: true},
		{Height: 2, Hash: "0x02", ParentHash: "0x01", Difficulty: "2", Complete: false},
		{Height: 2, Hash: "0x03", ParentHash: "0x01", Difficulty: "2", Complete: true},
	}

	loaded, err := fm.restoreBlockTree(blocks)
	if err != nil {
		t.Fatalf("restoreBlockTree failed: %v", err)
	}
	if loaded != len(blocks) {
		t.Fatalf("loaded count mismatch: got=%d want=%d", loaded, len(blocks))
	}
	if fm.blockTree.Get("") != nil {
		t.Fatal("invalid empty hash block must not be inserted")
	}
	if fm.blockTree.Get("0x01") == nil || fm.blockTree.Get("0x02") == nil || fm.blockTree.Get("0x03") == nil {
		t.Fatal("expected valid blocks to be inserted")
	}
	if !fm.storedBlocks.IsStored("0x01") || !fm.storedBlocks.IsStored("0x03") {
		t.Fatal("complete blocks should be marked stored")
	}
	if fm.storedBlocks.IsStored("0x02") {
		t.Fatal("incomplete block should not be marked stored")
	}

	// Re-run restore should hit existing-node skip path and remain stable.
	loaded2, err := fm.restoreBlockTree(blocks)
	if err != nil {
		t.Fatalf("repeat restoreBlockTree failed: %v", err)
	}
	if loaded2 != len(blocks) {
		t.Fatalf("repeat loaded count mismatch: got=%d want=%d", loaded2, len(blocks))
	}
}