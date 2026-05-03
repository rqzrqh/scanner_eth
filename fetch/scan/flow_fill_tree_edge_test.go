package scan

import "testing"

func TestFillTreeEdgeBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	if ok, msg := env.flow.SyncFillTreeTarget(" "); ok || msg == "" {
		t.Fatal("expected invalid header-by-hash target error")
	}

	_ = env.taskPool.TryStartHeaderHashSync("0xabc")
	if env.flow.taskRuntime.FetchAndInsertHeaderByHash("0xabc") {
		t.Fatal("expected false when hash already syncing")
	}

	env.blockTree.Insert(20, "0x20", "", 1)
	if env.flow.ShouldSyncOrphanParent("") {
		t.Fatal("empty hash should not be sync target")
	}
	if env.flow.ShouldSyncOrphanParent("0x20") {
		t.Fatal("existing node hash should not be sync target")
	}
}
