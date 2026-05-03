package fetch

import (
	"context"
	"testing"
)

func TestFetchManagerRunStopNilSafe(t *testing.T) {
	var nilFM *FetchManager
	nilFM.Run(context.Background())

	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	// No election set: Run should still be safe and return.
	fm.Run(context.Background())
	fm.Stop()
	if fm.currentRuntime() != nil {
		t.Fatal("Stop should release runtime state")
	}
}

func TestFetchManagerOnLostLeaderAndHelpers(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	mustTestRuntime(t, fm).scanWorker = fm.newScanWorker(mustTestScanFlow(t, fm))
	mustTestScanWorker(t, fm).SetEnabled(true)
	if !taskPool.TryStartHeaderHeightSync(10) {
		t.Fatal("failed to seed height syncing state")
	}
	if !taskPool.TryStartHeaderHashSync("0x10") {
		t.Fatal("failed to seed hash syncing state")
	}

	mustTestScanWorker(t, fm).Start()

	if err := fm.onLostLeader(context.Background()); err != nil {
		t.Fatalf("onLostLeader returned error: %v", err)
	}
	if scanWorker := fm.runtimeScanWorker(); scanWorker != nil && scanWorker.IsEnabled() {
		t.Fatal("scan should be disabled after onLostLeader")
	}
	if fm.runtimeScanWorker() != nil {
		t.Fatal("scan worker should be cleared after onLostLeader")
	}

	if fm.runtimeTaskPool() != nil {
		t.Fatal("task pool should be cleared after onLostLeader")
	}
}

func TestFetchManagerOnBecameLeaderReleasesRuntimeOnFailure(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	mustTestStoredBlocks(t, fm).MarkStored("stale")
	taskPool.AddTask("stale")
	setTestDbOperator(fm, nil)

	if err := fm.onBecameLeader(context.Background()); err == nil {
		t.Fatal("expected onBecameLeader to fail when db operator is nil")
	}
	if fm.currentRuntime() != nil {
		t.Fatal("runtime should be released when onBecameLeader fails")
	}
}
