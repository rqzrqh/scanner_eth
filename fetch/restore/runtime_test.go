package restore

import (
	"scanner_eth/blocktree"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"testing"
)

func testRuntimeDeps() RuntimeDeps {
	stored := fetchstore.NewStoredBlockState()
	return RuntimeDeps{
		BlockTree:    blocktree.NewBlockTree(2),
		StagingStore: fetchstore.NewStagingStore(),
		StoredBlocks: &stored,
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
	deps := testRuntimeDeps()
	blocks := []model.Block{
		{Height: 1, Hash: "", ParentHash: "", Difficulty: "1", Complete: true},
		{Height: 1, Hash: " 0x01 ", ParentHash: "", Difficulty: "1", Complete: true},
		{Height: 2, Hash: "0x02", ParentHash: " 0x01 ", Difficulty: "2", Complete: false},
		{Height: 2, Hash: "0x03", ParentHash: "0X01", Difficulty: "2", Complete: true},
	}

	loaded, err := deps.RestoreBlockTree(blocks)
	if err != nil {
		t.Fatalf("RestoreBlockTree failed: %v", err)
	}
	if loaded != len(blocks) {
		t.Fatalf("loaded count mismatch: got=%d want=%d", loaded, len(blocks))
	}
	if deps.BlockTree.Get("") != nil {
		t.Fatal("invalid empty hash block must not be inserted")
	}
	if deps.BlockTree.Get("0x01") == nil || deps.BlockTree.Get("0x02") == nil || deps.BlockTree.Get("0x03") == nil {
		t.Fatal("expected valid blocks to be inserted")
	}
	stored := deps.StoredBlocks
	if !stored.IsStored("0x01") || !stored.IsStored("0x03") {
		t.Fatal("complete blocks should be marked stored")
	}
	if stored.IsStored("0x02") {
		t.Fatal("incomplete block should not be marked stored")
	}

	loaded2, err := deps.RestoreBlockTree(blocks)
	if err != nil {
		t.Fatalf("repeat RestoreBlockTree failed: %v", err)
	}
	if loaded2 != len(blocks) {
		t.Fatalf("repeat loaded count mismatch: got=%d want=%d", loaded2, len(blocks))
	}
}
