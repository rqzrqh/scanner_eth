package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchstore "scanner_eth/fetch/store"
	"testing"
)

func TestInsertTreeHeaderDoesNotCacheHeaderDirectly(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x10", "0x0f"))
	if env.blockTree.Get("0x10") == nil {
		t.Fatal("expected block in tree")
	}
	if env.stagingStore.GetPendingHeader("0x10") != nil {
		t.Fatal("insertTreeHeader must not cache header directly")
	}
	if env.stagingStore.GetPendingBody("0x10") != nil {
		t.Fatal("insertTreeHeader must not cache body directly")
	}
}

func TestInvariantHeaderInsertRejectedTreeUnchanged(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0xroot", ""))
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x10", "0x0f"))

	if env.blockTree.Get("0x10") != nil {
		t.Fatal("expected 0x10 to be rejected by blocktree insert guard")
	}
	if env.stagingStore.GetPendingHeader("0x10") != nil {
		t.Fatal("expected no cached header for rejected insert")
	}
	if env.stagingStore.GetPendingBody("0x10") != nil {
		t.Fatal("expected no cached body for rejected insert")
	}
}

func TestInsertTreeHeaderEnqueuesBodyTaskWithoutCachingPayload(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.attachStoreWorker(nil)

	before := env.taskPool.Stats().EnqueuedBody
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x10", ""))
	after := env.taskPool.Stats().EnqueuedBody

	if after != before+1 {
		t.Fatalf("expected inserted header to enqueue one body task, before=%d after=%d", before, after)
	}
	if env.stagingStore.GetPendingHeader("0x10") != nil {
		t.Fatal("inserted header must not be cached as a pending full header")
	}
	if env.stagingStore.GetPendingBody("0x10") != nil {
		t.Fatal("inserted header must not create pending body data")
	}
}

func TestSyncHeaderByHashCachesFullHeaderForBodySync(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setLatestRemote(11)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))

	fullHeader := makeTestHeader(11, "0x0b", "0x0a")
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		if normalizeTestHash(hash) != "0x0b" {
			t.Fatalf("unexpected header hash fetch: %s", hash)
		}
		return fullHeader
	}

	if !env.flow.taskRuntime.SyncHeaderByHash(context.Background(), "0x0b") {
		t.Fatal("expected header-by-hash sync to succeed")
	}
	if env.blockTree.Get("0x0b") == nil {
		t.Fatal("expected full header to be inserted into blocktree")
	}
	if got := env.stagingStore.GetPendingHeader("0x0b"); got != fullHeader {
		t.Fatalf("expected full header cached for body sync, got %+v", got)
	}
	if env.stagingStore.GetPendingBody("0x0b") != nil {
		t.Fatal("header sync must not create pending body data")
	}

	env.fetchHeaderByHashFn = func(context.Context, string) *fetcherpkg.BlockHeaderJson {
		t.Fatal("body sync must use cached full header instead of fetching header again")
		return nil
	}
	env.fetchBodyByHashFn = func(_ context.Context, hash string, height uint64, header *fetcherpkg.BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
		if hash != "0x0b" || height != 11 || header != fullHeader {
			t.Fatalf("unexpected body fetch args hash=%q height=%d header=%+v", hash, height, header)
		}
		return makeTestEventBlockData(11, "0x0b", "0x0a"), 0, 1, true
	}
	if !env.flow.taskRuntime.SyncNodeDataByHash(context.Background(), "0x0b") {
		t.Fatal("expected body sync to use cached header")
	}
}
