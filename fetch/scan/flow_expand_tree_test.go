package scan

import (
	"context"
	fetcherpkg "scanner_eth/fetch/fetcher"
	"testing"
)

func TestExpandTreeWindow(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)
	env.setLatestRemote(7)
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
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

	env.flow.ExpandTreeWindow()
	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 5 || end != 7 {
		t.Fatalf("unexpected height range after expandTreeWindow: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestGetHeaderByHeightSyncTargetsFormalPredicates(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)

	targets := env.flow.GetExpandTreeTargets()
	if len(targets) != 1 || targets[0] != 5 {
		t.Fatalf("unexpected bootstrap targets: %v", targets)
	}

	if !env.taskPool.TryStartHeaderHeightSync(5) {
		t.Fatal("failed to seed header-height syncing state")
	}
	if targets := env.flow.GetExpandTreeTargets(); len(targets) != 0 {
		t.Fatalf("expected no bootstrap targets while startHeight is syncing, got=%v", targets)
	}
	env.taskPool.FinishHeaderHeightSync(5)

	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(5, "0x05", ""))
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(6, "0x06", "0x05"))
	env.setLatestRemote(10)

	targets = env.flow.GetExpandTreeTargets()
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
	targets = env.flow.GetExpandTreeTargets()
	if len(targets) != 1 || targets[0] != 8 {
		t.Fatalf("expected only non-syncing height to remain, got=%v", targets)
	}
	env.taskPool.FinishHeaderHeightSync(7)

	env.setLatestRemote(7)
	targets = env.flow.GetExpandTreeTargets()
	if len(targets) != 1 || targets[0] != 7 {
		t.Fatalf("expected latest height bound to limit targets to [7], got=%v", targets)
	}
}

func TestExpandTreeAdvancesExactlyByDerivedTargets(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(5)

	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(5, "0x05", ""))
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(6, "0x06", "0x05"))
	env.setLatestRemote(10)
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *fetcherpkg.BlockHeaderJson {
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

	targets := env.flow.GetExpandTreeTargets()
	if len(targets) != 2 {
		t.Fatalf("expected two derived height targets, got=%v", targets)
	}

	env.flow.ExpandTreeWindow()

	_, finalEnd, ok := env.blockTree.HeightRange()
	if !ok {
		t.Fatal("expected non-empty height range after expandTreeWindow")
	}
	wantEnd := initialEnd + uint64(len(targets))
	if finalEnd != wantEnd {
		t.Fatalf("unexpected height advancement: got end=%d want=%d targets=%v", finalEnd, wantEnd, targets)
	}
}
