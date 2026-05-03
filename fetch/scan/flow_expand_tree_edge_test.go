package scan

import "testing"

func TestInsertTreeHeaderAndHeaderWeightEdges(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.flow.taskRuntime.InsertTreeHeader(nil)
	if env.blockTree.Len() != 0 {
		t.Fatal("nil header should not change tree")
	}

	env.flow.taskRuntime.InsertTreeHeader(&testHeader{Number: "bad", Hash: "0x1", ParentHash: "", Difficulty: "0x1"})
	if env.blockTree.Len() != 0 {
		t.Fatal("invalid number header should not be inserted")
	}

	env.flow.taskRuntime.InsertTreeHeader(&testHeader{Number: "0x1", Hash: "0x1", ParentHash: "", Difficulty: "0x10000000000000000"})
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

func TestExpandTreeEdgeBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	if ok, msg := env.flow.SyncExpandTreeTarget("bad"); ok || msg == "" {
		t.Fatal("expected invalid header-by-height target error")
	}

	env.setIrreversible(0)
	if sz, ok := env.flow.HeaderWindowTargetSize(); ok || sz != 0 {
		t.Fatalf("expected disabled header window target size, got size=%d ok=%v", sz, ok)
	}

	env.setIrreversible(2)
	_ = env.taskPool.TryStartHeaderHeightSync(15)
	if got := env.flow.taskRuntime.FetchAndInsertHeaderByHeight(15); got != nil {
		t.Fatal("expected nil when height already syncing")
	}
}
