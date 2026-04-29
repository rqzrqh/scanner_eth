package fetch

import (
	"context"
	"testing"
)

func TestFetchManagerRunStopNilSafe(t *testing.T) {
	var nilFM *FetchManager
	nilFM.Run()

	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.Stop() })

	// No election set: Run should still be safe and return.
	fm.Run()
	fm.Stop()
}

func TestFetchManagerOnLostLeaderAndHelpers(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.Stop() })

	fm.scanWorker = fm.newScanWorker(mustTestScanFlow(t, fm))
	attachTestRuntime(fm)
	mustTestScanWorker(t, fm).SetEnabled(true)
	if !fm.taskPool.TryStartHeaderHeightSync(10) {
		t.Fatal("failed to seed height syncing state")
	}
	if !fm.taskPool.TryStartHeaderHashSync("0x10") {
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

	heightCount, hashCount := fm.taskPool.HeaderSyncCounts()
	if heightCount != 0 || hashCount != 0 {
		t.Fatalf("header syncing state should be reset, got heights=%d hashes=%d", heightCount, hashCount)
	}
}
