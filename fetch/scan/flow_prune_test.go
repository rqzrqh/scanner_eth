package scan

import (
	"context"
	"fmt"
	"scanner_eth/blocktree"
	fetchstore "scanner_eth/fetch/store"
	"strings"
	"testing"
)

func testPruneRuntime(env *testFlowEnv) PruneRuntimeDeps {
	env.t.Helper()
	env.flow.BindRuntimeDeps()
	return env.flow.pruneRuntime
}

func TestPruneRuntimeSnapshotNilReceiverAndLogPaths(t *testing.T) {
	s := (PruneRuntimeDeps{}).CaptureStateSnapshot()
	if s == nil {
		t.Fatal("snapshot should not be nil")
	}
	if s.Root != nil || len(s.Branches) != 0 {
		t.Fatalf("unexpected snapshot for zero deps: %+v", s)
	}

	logPruneSnapshot("nil", nil)
	logPruneSnapshot("empty", &pruneStateSnapshot{})
}

func TestPruneStoredBlocksIrreversibleDisabledNoop(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.stored.MarkStored("a")
	env.taskPool.AddTask("a")

	testPruneRuntime(env).PruneStoredBlocks(context.Background(), 0)
	if env.blockTree.Get("a") == nil || !env.stored.IsStored("a") || !env.taskPool.HasTask("a") {
		t.Fatal("prune should be noop when irreversibleBlocks <= 0")
	}
}

func TestPruneStoredBlocksReturnsEarlyWhenContextCancelled(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	testPruneRuntime(env).PruneStoredBlocks(ctx, env.irreversible)
}

func TestComplexOrphanCascadeInsertionTracksTreeAndStoredRange(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.stored.MarkStored("ghost")
	env.stored.MarkStored("a")
	env.stored.MarkStored("g")
	env.stored.MarkStored("y")

	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(5, "e", "d", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.blockTree.Insert(7, "g", "f", 1)
	env.blockTree.Insert(6, "f", "e", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(4, "d", "c", 1)
	env.blockTree.Insert(4, "x", "c", 1)
	env.blockTree.Insert(5, "y", "x", 1)

	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "x", "y"} {
		if env.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to exist after cascade insert", key)
		}
	}

	root := env.blockTree.Root()
	if root == nil || root.Key != "a" || root.Height != 1 {
		t.Fatalf("unexpected root after cascade insert: %+v", root)
	}
	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 1 || end != 7 {
		t.Fatalf("unexpected height range after cascade insert: ok=%v start=%v end=%v", ok, start, end)
	}

	branches := env.blockTree.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header == nil || branches[0].Header.Key != "g" {
		t.Fatalf("expected first branch header g, got=%+v", branches[0].Header)
	}
	if branches[1].Header == nil || branches[1].Header.Key != "y" {
		t.Fatalf("expected second branch header y, got=%+v", branches[1].Header)
	}

	branch0 := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		branch0 = append(branch0, node.Key)
	}
	if got := strings.Join(branch0, ","); got != "g,f,e,d,c,b,a" {
		t.Fatalf("unexpected main branch path: %s", got)
	}

	branch1 := make([]string, 0, len(branches[1].Nodes))
	for _, node := range branches[1].Nodes {
		branch1 = append(branch1, node.Key)
	}
	if got := strings.Join(branch1, ","); got != "y,x,c,b,a" {
		t.Fatalf("unexpected fork branch path: %s", got)
	}

	storedStart, storedEnd, storedOK := testPruneRuntime(env).StoredHeightRangeOnTree()
	if !storedOK || storedStart != 1 || storedEnd != 7 {
		t.Fatalf("unexpected stored height range: ok=%v start=%v end=%v", storedOK, storedStart, storedEnd)
	}
}

func TestInvariantPruneDeletesPendingAndStored(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		env.blockTree.Insert(h, hash, parent, 1)
		env.stored.MarkStored(hash)
		env.stagingStore.SetPendingHeader(hash, makeTestHeader(h, hash, parent))
		env.stagingStore.SetPendingBody(hash, makeTestBody(true))
		parent = hash
	}
	for _, hash := range []string{"h1", "h2", "h3"} {
		env.taskPool.AddTask(hash)
	}

	testPruneRuntime(env).PruneStoredBlocks(context.Background(), env.irreversible)

	for _, hash := range []string{"h1", "h2", "h3"} {
		if env.stored.IsStored(hash) {
			t.Fatalf("expected %s removed from stored state after prune", hash)
		}
		if env.stagingStore.GetPendingHeader(hash) != nil {
			t.Fatalf("expected %s header removed from pending blocks after prune", hash)
		}
		if env.stagingStore.GetPendingBody(hash) != nil {
			t.Fatalf("expected %s body removed from pending blocks after prune", hash)
		}
		if env.taskPool.HasTask(hash) {
			t.Fatalf("expected %s task removed from taskPool after prune", hash)
		}
	}
}

func TestPruneComplexForkRemovesPrunedBranchesAndStoredState(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.blockTree.Insert(4, "d", "c", 1)
	env.blockTree.Insert(5, "e", "d", 1)
	env.blockTree.Insert(3, "x", "b", 1)
	env.blockTree.Insert(4, "y", "x", 1)

	for _, key := range []string{"a", "b", "c", "d", "e", "x", "y"} {
		env.stored.MarkStored(key)
		env.taskPool.AddTask(key)
	}

	testPruneRuntime(env).PruneStoredBlocks(context.Background(), env.irreversible)

	root := env.blockTree.Root()
	if root == nil || root.Key != "c" || root.Height != 3 {
		t.Fatalf("unexpected root after prune: %+v", root)
	}
	for _, key := range []string{"a", "b", "x", "y"} {
		if env.blockTree.Get(key) != nil {
			t.Fatalf("expected pruned node %s to be removed from tree", key)
		}
		if env.taskPool.HasTask(key) {
			t.Fatalf("expected pruned node %s to be removed from taskPool", key)
		}
		if env.stored.IsStored(key) {
			t.Fatalf("expected pruned node %s to be removed from storedBlocks", key)
		}
	}
	for _, key := range []string{"c", "d", "e"} {
		if env.blockTree.Get(key) == nil {
			t.Fatalf("expected kept node %s to remain in tree", key)
		}
		if !env.taskPool.HasTask(key) {
			t.Fatalf("expected kept node %s task to remain", key)
		}
		if !env.stored.IsStored(key) {
			t.Fatalf("expected kept node %s to remain in storedBlocks", key)
		}
	}

	branches := env.blockTree.Branches()
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch after prune, got=%d", len(branches))
	}
	path := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		path = append(path, node.Key)
	}
	if got := strings.Join(path, ","); got != "e,d,c" {
		t.Fatalf("unexpected remaining branch path after prune: %s", got)
	}
}

func TestPruneFrequentPruneAndStoreAlternationConsistency(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	parent := ""
	for h := uint64(1); h <= 12; h++ {
		hash := fmt.Sprintf("h%d", h)
		env.blockTree.Insert(h, hash, parent, 1)
		env.stagingStore.SetPendingHeader(hash, makeTestHeader(h, hash, parent))
		env.stagingStore.SetPendingBody(hash, makeTestEventBlockData(h, hash, parent))
		parent = hash
	}
	env.stored.MarkStored("h1")
	env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error { return nil })

	for i := 0; i < 8; i++ {
		env.flow.SubmitStoreBranchesLowToHigh(context.Background())
		testPruneRuntime(env).PruneStoredBlocks(context.Background(), env.irreversible)
	}

	root := env.blockTree.Root()
	if root == nil {
		t.Fatal("expected non-nil root after alternating prune/store")
	}
	if root.Height <= 1 {
		t.Fatalf("expected root to advance under alternating prune/store, got=%d", root.Height)
	}

	for h := uint64(1); h < root.Height; h++ {
		hash := fmt.Sprintf("h%d", h)
		if env.blockTree.Get(hash) != nil {
			t.Fatalf("expected pruned hash %s to be absent from tree", hash)
		}
		if env.stored.IsStored(hash) {
			t.Fatalf("expected pruned hash %s to be absent from stored state", hash)
		}
		if env.stagingStore.GetPendingHeader(hash) != nil {
			t.Fatalf("expected pruned hash %s header to be removed from pending blocks", hash)
		}
		if env.stagingStore.GetPendingBody(hash) != nil {
			t.Fatalf("expected pruned hash %s body to be removed from pending blocks", hash)
		}
	}

	for hash := range snapshotTestStoredHashes(env) {
		if env.blockTree.Get(hash) == nil {
			t.Fatalf("stored hash %s should still exist in tree", hash)
		}
	}
}

func TestPruneNoopWhenStoredSpanWithinKeepEvenWithStaleStoredHashes(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(10, "a", "", 1)
	env.blockTree.Insert(11, "b", "a", 1)
	env.blockTree.Insert(12, "c", "b", 1)
	env.stored.MarkStored("a")
	env.stored.MarkStored("b")
	env.stored.MarkStored("c")
	env.stored.MarkStored("ghost-1")
	env.stored.MarkStored("ghost-2")
	env.taskPool.AddTask("a")
	env.taskPool.AddTask("b")
	env.taskPool.AddTask("c")

	testPruneRuntime(env).PruneStoredBlocks(context.Background(), env.irreversible)

	for _, key := range []string{"a", "b", "c"} {
		if env.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to remain when prune is noop", key)
		}
		if !env.stored.IsStored(key) {
			t.Fatalf("expected stored node %s to remain stored when prune is noop", key)
		}
	}
	for _, key := range []string{"ghost-1", "ghost-2"} {
		if !env.stored.IsStored(key) {
			t.Fatalf("expected stale stored hash %s to be untouched by noop prune", key)
		}
	}
	if !env.taskPool.HasTask("a") || !env.taskPool.HasTask("b") || !env.taskPool.HasTask("c") {
		t.Fatalf("expected tasks to remain untouched when prune is noop")
	}
}

func TestPruneOrphanCapacityPressureConverges(t *testing.T) {
	bt := blocktree.NewBlockTree(2)
	parent := ""
	for h := uint64(1); h <= 30; h++ {
		hash := fmt.Sprintf("h%d", h)
		bt.Insert(h, hash, parent, 1)
		parent = hash
	}
	for h := uint64(2); h <= 21; h++ {
		bt.Insert(h, fmt.Sprintf("oa%d", h), fmt.Sprintf("ma%d", h), 1)
	}
	for h := uint64(40); h <= 99; h++ {
		bt.Insert(h, fmt.Sprintf("ob%d", h), fmt.Sprintf("pb%d", h), 1)
	}

	initialOrphanParents := len(bt.UnlinkedNodes())
	if initialOrphanParents != 80 {
		t.Fatalf("unexpected initial orphan parent count: got=%d want=80", initialOrphanParents)
	}

	for i := 0; i < 10; i++ {
		bt.Prune(1)
	}
	for h := uint64(40); h <= 69; h++ {
		bt.Insert(h-1, fmt.Sprintf("pb%d", h), "h30", 1)
		bt.Prune(1)
	}

	afterOrphanParents := bt.UnlinkedNodes()
	if len(afterOrphanParents) >= initialOrphanParents {
		t.Fatalf("expected orphan parent count to decrease, initial=%d after=%d", initialOrphanParents, len(afterOrphanParents))
	}

	afterSet := make(map[string]struct{}, len(afterOrphanParents))
	for _, key := range afterOrphanParents {
		afterSet[key] = struct{}{}
	}
	for h := uint64(40); h <= 69; h++ {
		if _, exists := afterSet[fmt.Sprintf("pb%d", h)]; exists {
			t.Fatalf("resolved orphan parent should not remain unlinked: pb%d", h)
		}
	}
	for h := uint64(70); h <= 99; h++ {
		if _, exists := afterSet[fmt.Sprintf("pb%d", h)]; !exists {
			t.Fatalf("unresolved orphan parent should remain unlinked: pb%d", h)
		}
	}
}
