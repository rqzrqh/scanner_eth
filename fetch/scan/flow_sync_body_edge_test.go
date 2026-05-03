package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchstore "scanner_eth/fetch/store"
	"testing"
)

func TestSyncNodeDataByHashEarlyReturnAndFailures(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	if !env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "") {
		t.Fatal("empty hash should return true (noop)")
	}
	if !env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "0xnotfound") {
		t.Fatal("missing node should return true (noop)")
	}

	env.blockTree.Insert(10, "0x10", "", 1)
	if env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "0x10") {
		t.Fatal("expected false when header fetch is unavailable")
	}

	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		return nil
	}
	if env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "0x10") {
		t.Fatal("expected false when header-by-hash fetch fails")
	}
}

func TestSyncNodeDataByHashDecodeFallbackAndFullBlockFail(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(12, "0x12", "", 1)
	env.stagingStore.SetPendingHeader("0x12", &testHeader{Number: "bad", Hash: "0x12", ParentHash: "", Difficulty: "0x1"})
	env.fetchBodyByHashFn = func(_ context.Context, hash string, height uint64, header *fetcherpkg.BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
		if hash != "0x12" || height != 12 {
			t.Fatalf("expected decode fallback to node height=12, got hash=%q height=%d", hash, height)
		}
		return nil, -1, 0, false
	}

	if env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "0x12") {
		t.Fatal("expected false when full block fetch fails")
	}
}

func TestSyncBodyBranchTargetStopsWhenContextCancelled(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "h1", "", 1)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	ok, msg := env.flow.SyncStoreBranchTarget(ctx, "h1")
	if ok || msg == "" {
		t.Fatalf("expected cancelled context to abort body sync, ok=%v msg=%q", ok, msg)
	}
}
