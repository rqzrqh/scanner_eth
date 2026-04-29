package scan

import (
	"context"
	"testing"
)

func TestSyncNodeDataByHashEarlyReturnAndFailures(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	if !env.flow.SyncNodeDataByHash(context.Background(), "") {
		t.Fatal("empty hash should return true (noop)")
	}
	if !env.flow.SyncNodeDataByHash(context.Background(), "0xnotfound") {
		t.Fatal("missing node should return true (noop)")
	}

	env.blockTree.Insert(10, "0x10", "", 1, nil)
	if env.flow.SyncNodeDataByHash(context.Background(), "0x10") {
		t.Fatal("expected false when header fetch is unavailable")
	}

	env.fetchHeaderByHashFn = func(_ context.Context, hash string) any {
		return nil
	}
	if env.flow.SyncNodeDataByHash(context.Background(), "0x10") {
		t.Fatal("expected false when header-by-hash fetch fails")
	}
}

func TestSyncNodeDataByHashDecodeFallbackAndFullBlockFail(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(12, "0x12", "", 1, nil)
	env.payloads.SetNodeBlockHeader("0x12", &testHeader{Number: "bad", Hash: "0x12", ParentHash: "", Difficulty: "0x1"})
	env.fetchBodyByHashFn = func(_ context.Context, hash string, height uint64, header any) (any, int, int64, bool) {
		if hash != "0x12" || height != 12 {
			t.Fatalf("expected decode fallback to node height=12, got hash=%q height=%d", hash, height)
		}
		return nil, -1, 0, false
	}

	if env.flow.SyncNodeDataByHash(context.Background(), "0x12") {
		t.Fatal("expected false when full block fetch fails")
	}
}

func TestInsertHeaderAndHeaderWeightEdges(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.InsertHeader(nil)
	if env.blockTree.Len() != 0 {
		t.Fatal("nil header should not change tree")
	}

	env.flow.InsertHeader(&testHeader{Number: "bad", Hash: "0x1", ParentHash: "", Difficulty: "0x1"})
	if env.blockTree.Len() != 0 {
		t.Fatal("invalid number header should not be inserted")
	}

	env.flow.InsertHeader(&testHeader{Number: "0x1", Hash: "0x1", ParentHash: "", Difficulty: "0x10000000000000000"})
	n := env.blockTree.Get("0x1")
	if n == nil {
		t.Fatal("valid header should be inserted")
	}
	if n.Weight != ^uint64(0) {
		t.Fatalf("expected saturated weight on overflow difficulty, got=%d", n.Weight)
	}

	if testHeaderWeight(nil) != 0 ||
		testHeaderWeight(&testHeader{}) != 0 ||
		testHeaderWeight(&testHeader{Difficulty: "0x0"}) != 0 ||
		testHeaderWeight(&testHeader{Difficulty: "bad"}) != 0 {
		t.Fatal("expected zero header weight for nil/invalid/non-positive difficulty")
	}
}

func TestScanFlowEdgeBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if env.flow.RunScanCoordinator(ctx) {
		t.Fatal("canceled context should stop scan coordinator")
	}
	env.flow.RunScanCycle(ctx)

	if ok, msg := env.flow.SyncHeaderByHeightTarget(context.Background(), "bad"); ok || msg == "" {
		t.Fatal("expected invalid header-by-height target error")
	}
	if ok, msg := env.flow.SyncHeaderByHashTarget(context.Background(), " "); ok || msg == "" {
		t.Fatal("expected invalid header-by-hash target error")
	}
	if ok, msg := env.flow.SyncBodyTarget(context.Background(), "   "); ok || msg == "" {
		t.Fatal("expected invalid body target error")
	}

	env.setIrreversible(0)
	if sz, ok := env.flow.HeaderWindowTargetSize(); ok || sz != 0 {
		t.Fatalf("expected disabled header window target size, got size=%d ok=%v", sz, ok)
	}

	env.setIrreversible(2)
	_ = env.taskPool.TryStartHeaderHeightSync(15)
	_ = env.taskPool.TryStartHeaderHashSync("0xabc")
	if got := env.flow.FetchAndInsertHeaderByHeight(15); got != nil {
		t.Fatal("expected nil when height already syncing")
	}
	if env.flow.FetchAndInsertHeaderByHash("0xabc") {
		t.Fatal("expected false when hash already syncing")
	}

	env.blockTree.Insert(20, "0x20", "", 1, nil)
	if env.flow.ShouldSyncOrphanParent("") {
		t.Fatal("empty hash should not be sync target")
	}
	if env.flow.ShouldSyncOrphanParent("0x20") {
		t.Fatal("existing node hash should not be sync target")
	}

	env.flow.ProcessBranchNode(context.Background(), nil)
}

func TestSyncBodyTargetStopsWhenContextCancelled(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "h1", "", 1, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, msg := env.flow.SyncBodyTarget(ctx, "h1")
	if ok || msg == "" {
		t.Fatalf("expected cancelled context to abort body sync, ok=%v msg=%q", ok, msg)
	}
}
