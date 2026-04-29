package scan

import (
	"context"
	"testing"
)

func TestSyncHeaderWindowAndSyncOrphanParents(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)
	env.latestRemote = 7
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) any {
		switch height {
		case 5:
			return makeTestHeader(5, "0x05", "")
		case 6:
			return makeTestHeader(6, "0x06", "0x05")
		case 7:
			return makeTestHeader(7, "0x07", "0x06")
		default:
			return nil
		}
	}
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) any {
		if normalizeTestHash(hash) == "0x10" {
			return makeTestHeader(16, "0x10", "0x07")
		}
		return nil
	}

	env.flow.SyncHeaderWindow()
	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 5 || end != 7 {
		t.Fatalf("unexpected height range after syncHeaderWindow: ok=%v start=%v end=%v", ok, start, end)
	}

	env.blockTree.Insert(17, "0x11", "0x10", 1, nil)
	env.flow.SyncOrphanParents()
	if env.blockTree.Get("0x10") == nil || env.blockTree.Get("0x11") == nil {
		t.Fatal("expected orphan parent and child to be linked after syncOrphanParents")
	}
}

func TestGetHeaderByHeightSyncTargetsFormalPredicates(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)

	targets := env.flow.GetHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 5 {
		t.Fatalf("unexpected bootstrap targets: %v", targets)
	}

	if !env.taskPool.TryStartHeaderHeightSync(5) {
		t.Fatal("failed to seed header-height syncing state")
	}
	if targets := env.flow.GetHeaderByHeightSyncTargets(); len(targets) != 0 {
		t.Fatalf("expected no bootstrap targets while startHeight is syncing, got=%v", targets)
	}
	env.taskPool.FinishHeaderHeightSync(5)

	env.flow.InsertHeader(makeTestHeader(5, "0x05", ""))
	env.flow.InsertHeader(makeTestHeader(6, "0x06", "0x05"))
	env.latestRemote = 10

	targets = env.flow.GetHeaderByHeightSyncTargets()
	want := []uint64{7, 8}
	if len(targets) != len(want) {
		t.Fatalf("unexpected target count: got=%v want=%v", targets, want)
	}
	for i, h := range want {
		if targets[i] != h {
			t.Fatalf("unexpected target at index %d: got=%d want=%d all=%v", i, targets[i], h, targets)
		}
	}

	if !env.taskPool.TryStartHeaderHeightSync(7) {
		t.Fatal("failed to seed syncing state for height 7")
	}
	targets = env.flow.GetHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 8 {
		t.Fatalf("expected only non-syncing height to remain, got=%v", targets)
	}
	env.taskPool.FinishHeaderHeightSync(7)

	env.latestRemote = 7
	targets = env.flow.GetHeaderByHeightSyncTargets()
	if len(targets) != 1 || targets[0] != 7 {
		t.Fatalf("expected latest height bound to limit targets to [7], got=%v", targets)
	}
}

func TestGetHeaderByHashSyncTargetsFormalPredicates(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.InsertHeader(makeTestHeader(10, "0x0a", ""))
	env.blockTree.Insert(11, "0x0b", "0x0f", 1, nil)
	env.blockTree.Insert(12, "0x0c", "0x10", 1, nil)

	targets := env.flow.GetHeaderByHashSyncTargets()
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
	targets = env.flow.GetHeaderByHashSyncTargets()
	if len(targets) != 1 || targets[0] != "0x10" {
		t.Fatalf("expected syncing hash to be filtered out, got=%v", targets)
	}
	env.taskPool.FinishHeaderHashSync("0x0f")
}

func TestHeightSyncAdvancesExactlyByDerivedTargets(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)

	env.flow.InsertHeader(makeTestHeader(5, "0x05", ""))
	env.flow.InsertHeader(makeTestHeader(6, "0x06", "0x05"))
	env.latestRemote = 10
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) any {
		switch height {
		case 7:
			return makeTestHeader(7, "0x07", "0x06")
		case 8:
			return makeTestHeader(8, "0x08", "0x07")
		default:
			return nil
		}
	}

	start, initialEnd, ok := env.blockTree.HeightRange()
	if !ok || start != 5 || initialEnd != 6 {
		t.Fatalf("unexpected initial height range: ok=%v start=%v end=%v", ok, start, initialEnd)
	}

	targets := env.flow.GetHeaderByHeightSyncTargets()
	if len(targets) != 2 {
		t.Fatalf("expected two derived height targets, got=%v", targets)
	}

	env.flow.SyncHeaderWindow()

	_, finalEnd, ok := env.blockTree.HeightRange()
	if !ok {
		t.Fatal("expected non-empty height range after syncHeaderWindow")
	}
	wantEnd := initialEnd + uint64(len(targets))
	if finalEnd != wantEnd {
		t.Fatalf("unexpected height advancement: got end=%d want=%d targets=%v", finalEnd, wantEnd, targets)
	}
}

func TestHeaderHashSyncFailureLeavesTargetRetryable(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.InsertHeader(makeTestHeader(10, "0x0a", ""))
	env.blockTree.Insert(11, "0x0b", "0x0f", 1, nil)
	env.fetchHeaderByHashFn = func(context.Context, string) any { return nil }

	if env.flow.FetchAndInsertHeaderByHash("0x0f") {
		t.Fatal("expected hash sync failure when fetcher returns nil")
	}
	if env.blockTree.Get("0x0f") != nil {
		t.Fatal("failed hash sync must not insert parent into tree")
	}
	if env.stored.IsStored("0x0f") {
		t.Fatal("failed hash sync must not mark parent as stored")
	}

	targets := env.flow.GetHeaderByHashSyncTargets()
	if len(targets) != 1 || targets[0] != "0x0f" {
		t.Fatalf("failed hash sync should remain retryable in next target set, got=%v", targets)
	}
}

func TestCountActionableAndStoredLinkedNodes(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(10, "0xaa", "", 1, nil)
	env.blockTree.Insert(11, "0xbb", "0xaa", 1, nil)
	env.payloads.SetNodeBlockBody("0xaa", &testBody{storable: true})

	if got := env.flow.CountActionableBodyNodes(); got != 2 {
		t.Fatalf("expected 2 actionable nodes (root storable + child nil data), got=%d", got)
	}

	env.stored.markStored("0xaa")
	if got := env.flow.CountStoredLinkedNodes(); got != 1 {
		t.Fatalf("expected 1 stored linked node, got=%d", got)
	}
}
