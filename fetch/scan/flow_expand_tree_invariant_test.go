package scan

import "testing"

func TestInsertTreeHeaderDoesNotCacheForBodySync(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.taskRuntime.InsertTreeHeader(makeTestHeader(10, "0x10", "0x0f"))
	if env.blockTree.Get("0x10") == nil {
		t.Fatal("expected block in tree")
	}
	if env.stagingStore.GetPendingHeader("0x10") != nil {
		t.Fatal("insertTreeHeader must not cache header; body sync refetches by hash")
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
}
