package restore

import (
	"context"
	"scanner_eth/blocktree"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
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

func TestRestoreBlockTreeMarksRootParentReady(t *testing.T) {
	deps := testRuntimeDeps()
	blocks := []model.Block{
		{Height: 8, Hash: "0x08", ParentHash: "0x07", Difficulty: "8", Complete: true},
		{Height: 9, Hash: "0x09", ParentHash: "0x08", Difficulty: "9", Complete: true},
	}

	if _, err := deps.RestoreBlockTree(blocks); err != nil {
		t.Fatalf("RestoreBlockTree failed: %v", err)
	}
	if !deps.StoredBlocks.IsStored("0x07") {
		t.Fatal("off-window root parent should be marked ready")
	}
}

func TestRestoreRemoteRootInsertsHeaderAndMarksParentReady(t *testing.T) {
	deps := testRuntimeDeps()
	deps.NodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	deps.Fetcher = fetcherpkg.NewMockFetcher(
		func(_ context.Context, _ nodepkg.NodeOperator, _ int, height uint64) *fetcherpkg.BlockHeaderJson {
			if height != 30 {
				t.Fatalf("unexpected restore height: %d", height)
			}
			return &fetcherpkg.BlockHeaderJson{
				Number:     hexutil.EncodeUint64(30),
				Hash:       "0x30",
				ParentHash: "0x29",
			}
		},
		nil,
		nil,
	)

	if !deps.RestoreRemoteRoot(context.Background(), 30) {
		t.Fatal("expected remote root restore to succeed")
	}
	root := deps.BlockTree.Root()
	if root == nil || root.Height != 30 || root.Key != "0x30" {
		t.Fatalf("unexpected restored root: %+v", root)
	}
	if !deps.StoredBlocks.IsStored("0x29") {
		t.Fatal("remote root parent should be marked ready")
	}
}

func TestRestoreRemoteRootFailsWithoutHeader(t *testing.T) {
	deps := testRuntimeDeps()
	deps.NodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	deps.Fetcher = fetcherpkg.NewMockFetcher(nil, nil, nil)

	if deps.RestoreRemoteRoot(context.Background(), 30) {
		t.Fatal("expected remote root restore to fail without a header")
	}
	if deps.BlockTree.Root() != nil {
		t.Fatal("failed remote restore must not create a root")
	}
}
