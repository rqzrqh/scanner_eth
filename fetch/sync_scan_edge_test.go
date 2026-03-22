package fetch

import (
	"context"
	"testing"

	"scanner_eth/data"

	"github.com/ethereum/go-ethereum/ethclient"
)

func TestSyncNodeDataByHashEarlyReturnAndFailures(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	if !fm.syncNodeDataByHash("") {
		t.Fatal("empty hash should return true (noop)")
	}
	if !fm.syncNodeDataByHash("0xnotfound") {
		t.Fatal("missing node should return true (noop)")
	}

	// Insert node with no header to force header-by-hash fetch path.
	fm.blockTree.Insert(10, "0x10", "", 1, nil, nil)

	// No ready node -> GetBestNode error.
	fm.nodeManager.SetNodeNotReady(0)
	if fm.syncNodeDataByHash("0x10") {
		t.Fatal("expected false when no available node for header fetch")
	}

	// Ready node but header fetch returns nil.
	fm.nodeManager.SetNodeReady(0)
	fm.blockFetcher = &mockBlockFetcher{fetchByHashFn: func(nodeID int, taskID int, client *ethclient.Client, hash string) *BlockHeaderJson {
		return nil
	}}
	if fm.syncNodeDataByHash("0x10") {
		t.Fatal("expected false when header-by-hash fetch fails")
	}
}

func TestSyncNodeDataByHashDecodeFallbackAndFullBlockFail(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	// Node height=12 with invalid header number to hit decode fallback branch.
	fm.blockTree.Insert(12, "0x12", "", 1, &BlockHeaderJson{Hash: "0x12", Number: "invalid"}, nil)
	setNodeLatestHeight(fm, 12)

	fm.blockFetcher = &mockBlockFetcher{
		fetchFullFn: func(nodeID int, taskID int, client *ethclient.Client, header *BlockHeaderJson) *data.FullBlock {
			return nil
		},
	}

	if fm.syncNodeDataByHash("0x12") {
		t.Fatal("expected false when full block fetch fails")
	}
}

func TestInsertHeaderAndHeaderWeightEdges(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.insertHeader(nil)
	if fm.blockTree.Len() != 0 {
		t.Fatal("nil header should not change tree")
	}

	fm.insertHeader(&BlockHeaderJson{Number: "bad", Hash: "0x1", ParentHash: "", Difficulty: "0x1"})
	if fm.blockTree.Len() != 0 {
		t.Fatal("invalid number header should not be inserted")
	}

	fm.insertHeader(&BlockHeaderJson{Number: "0x1", Hash: "0x1", ParentHash: "", Difficulty: "0x10000000000000000"})
	n := fm.blockTree.Get("0x1")
	if n == nil {
		t.Fatal("valid header should be inserted")
	}
	if n.Weight != ^uint64(0) {
		t.Fatalf("expected saturated weight on overflow difficulty, got=%d", n.Weight)
	}

	if headerWeight(nil) != 0 || headerWeight(&BlockHeaderJson{}) != 0 || headerWeight(&BlockHeaderJson{Difficulty: "0x0"}) != 0 || headerWeight(&BlockHeaderJson{Difficulty: "bad"}) != 0 {
		t.Fatal("expected zero header weight for nil/invalid/non-positive difficulty")
	}
}

func TestScanFlowEdgeBranches(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if fm.runScanCoordinator(ctx) {
		t.Fatal("canceled context should stop scan coordinator")
	}
	fm.scanEvents(ctx) // should be no-op

	if ok, msg := fm.syncHeaderByHeightTarget("bad"); ok || msg == "" {
		t.Fatal("expected invalid header-by-height target error")
	}
	if ok, msg := fm.syncHeaderByHashTarget(" "); ok || msg == "" {
		t.Fatal("expected invalid header-by-hash target error")
	}
	if ok, msg := fm.syncBodyTarget("   "); ok || msg == "" {
		t.Fatal("expected invalid body target error")
	}

	fm.irreversibleBlocks = 0
	if sz, ok := fm.headerWindowTargetSize(); ok || sz != 0 {
		t.Fatalf("expected disabled header window target size, got size=%d ok=%v", sz, ok)
	}

	fm.irreversibleBlocks = 2
	_ = fm.taskPool.tryStartHeaderHeightSync(15)
	_ = fm.taskPool.tryStartHeaderHashSync("0xabc")
	if got := fm.fetchAndInsertHeaderByHeight(15); got != nil {
		t.Fatal("expected nil when height already syncing")
	}
	if fm.fetchAndInsertHeaderByHash("0xabc") {
		t.Fatal("expected false when hash already syncing")
	}

	fm.blockTree.Insert(20, "0x20", "", 1, nil, nil)
	if fm.shouldSyncOrphanParent("") {
		t.Fatal("empty hash should not be sync target")
	}
	if fm.shouldSyncOrphanParent("0x20") {
		t.Fatal("existing node hash should not be sync target")
	}

	fm.processBranchNode(nil) // no panic
}