// Formal verification I9: runtime identity of T/P/D bindings for prune vs scan (`buildTreeRuntimeDeps`).
package fetch

import (
	"reflect"
	"testing"
)

// TestFormalRuntimePointerIdentity verifies §2.2: runtime accessors return the same *StoredBlockState /
// *pool / payload store / block tree pointers as flattened FetchManager fields (test harness + leader runtime wiring).
func TestFormalRuntimePointerIdentity(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	if got, want := fm.runtimeStoredBlocks(), fm.storedBlocks; got != want {
		t.Fatalf("runtimeStoredBlocks %p != fm.storedBlocks %p", got, want)
	}
	if got, want := fm.runtimeTaskPool(), fm.taskPool; got != want {
		t.Fatalf("runtimeTaskPool %p != fm.taskPool %p", got, want)
	}
	if got, want := fm.runtimePayloadStore(), fm.pendingPayloadStore; got != want {
		t.Fatalf("runtimePayloadStore %p != fm.pendingPayloadStore %p", got, want)
	}
	if got, want := fm.runtimeBlockTree(), fm.blockTree; got != want {
		t.Fatalf("runtimeBlockTree %p != fm.blockTree %p", got, want)
	}
}

// TestFormalBuildTreeRuntimeDepsStablePointers checks successive buildTreeRuntimeDeps() calls share pointers
// for BlockTree / StoredBlocks / task pool — prune and scan derive from one wiring (FormalVerification §10.5).
func TestFormalBuildTreeRuntimeDepsStablePointers(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	a := fm.buildTreeRuntimeDeps()
	b := fm.buildTreeRuntimeDeps()
	if a.BlockTree != b.BlockTree || a.StoredBlocks != b.StoredBlocks || a.TaskPool != b.TaskPool {
		t.Fatal("consecutive buildTreeRuntimeDeps must share BlockTree / StoredBlocks / TreeTaskPool pointers")
	}
	if a.StoredBlocks != fm.storedBlocks || a.TaskPool != fm.runtimeTaskPool() {
		t.Fatal("deps must reference flattened leader fields")
	}
}

// TestFormalCapturePruneEqualsBuildTreeDepsSnapshot locks the prune_state contract: snapshots from
// capturePruneStateSnapshot must equal buildTreeRuntimeDeps().CapturePruneStateSnapshot().
func TestFormalCapturePruneEqualsBuildTreeDepsSnapshot(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.storedBlocks.MarkStored("a")

	snapViaFM := fm.capturePruneStateSnapshot()
	deps := fm.buildTreeRuntimeDeps()
	snapViaDeps := deps.CapturePruneStateSnapshot()

	if snapViaFM == nil || snapViaDeps == nil {
		t.Fatal("snapshots must be non-nil")
	}
	if !reflect.DeepEqual(snapViaFM, snapViaDeps) {
		t.Fatalf("capturePruneStateSnapshot != buildTreeRuntimeDeps().CapturePruneStateSnapshot\nfm:%#v\ndeps:%#v", snapViaFM, snapViaDeps)
	}

	min1, max1, ok1 := fm.storedHeightRangeOnTree()
	min2, max2, ok2 := deps.StoredHeightRangeOnTree()
	if ok1 != ok2 || min1 != min2 || max1 != max2 {
		t.Fatalf("storedHeightRangeOnTree mismatch: fm=(%v,%v,%v) deps=(%v,%v,%v)", min1, max1, ok1, min2, max2, ok2)
	}
	if !ok1 {
		t.Fatal("expected stored height range after MarkStored on linked node")
	}
}
