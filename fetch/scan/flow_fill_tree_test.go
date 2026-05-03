package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	"testing"
)

func TestFillTreeMissingParents(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(7, "0x07", ""))
	env.blockTree.Insert(17, "0x11", "0x10", 1)
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *fetcherpkg.BlockHeaderJson {
		if normalizeTestHash(hash) == "0x10" {
			return makeTestHeader(16, "0x10", "0x07")
		}
		return nil
	}

	env.flow.FillTreeMissingParents()
	if env.blockTree.Get("0x10") == nil || env.blockTree.Get("0x11") == nil {
		t.Fatal("expected orphan parent and child to be linked after fillTreeMissingParents")
	}
}

func TestGetHeaderByHashSyncTargetsFormalPredicates(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))
	env.blockTree.Insert(11, "0x0b", "0x0f", 1)
	env.blockTree.Insert(12, "0x0c", "0x10", 1)

	targets := env.flow.GetFillTreeTargets()
	if len(targets) != 2 {
		t.Fatalf("expected two missing parent targets, got=%v", targets)
	}
	targetSet := make(map[string]struct{}, len(targets))
	for _, hash := range targets {
		targetSet[hash] = struct{}{}
		if hash == "" {
			t.Fatal("empty hash must not appear in hash sync targets")
		}
		if env.blockTree.Get(hash) != nil {
			t.Fatalf("existing node hash must not appear as sync target: %s", hash)
		}
	}
	if _, ok := targetSet["0x0f"]; !ok {
		t.Fatalf("expected normalized missing parent 0x0f in targets: %v", targets)
	}
	if _, ok := targetSet["0x10"]; !ok {
		t.Fatalf("expected normalized missing parent 0x10 in targets: %v", targets)
	}

	if !env.taskPool.TryStartHeaderHashSync("0x0f") {
		t.Fatal("failed to seed header-hash syncing state")
	}
	targets = env.flow.GetFillTreeTargets()
	if len(targets) != 1 || targets[0] != "0x10" {
		t.Fatalf("expected syncing hash to be filtered out, got=%v", targets)
	}
	env.taskPool.FinishHeaderHashSync("0x0f")
}

func TestFillTreeHashSyncFailureLeavesTargetRetryable(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x0a", ""))
	env.blockTree.Insert(11, "0x0b", "0x0f", 1)
	env.fetchHeaderByHashFn = func(context.Context, string) *fetcherpkg.BlockHeaderJson { return nil }

	if env.flow.taskRuntime.FetchAndInsertHeaderByHash("0x0f") {
		t.Fatal("expected hash sync failure when fetcher returns nil")
	}
	if env.blockTree.Get("0x0f") != nil {
		t.Fatal("failed hash sync must not insert parent into tree")
	}
	if env.stored.IsStored("0x0f") {
		t.Fatal("failed hash sync must not mark parent as stored")
	}

	targets := env.flow.GetFillTreeTargets()
	if len(targets) != 1 || targets[0] != "0x0f" {
		t.Fatalf("failed hash sync should remain retryable in next target set, got=%v", targets)
	}
}
