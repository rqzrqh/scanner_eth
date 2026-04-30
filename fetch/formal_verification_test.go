// Formal verification I9: runtime identity of T/P/D bindings for prune vs scan.
package fetch

import (
	"reflect"
	fetchscan "scanner_eth/fetch/scan"
	"testing"
)

// TestFormalRuntimePointerIdentity verifies §2.2: runtime accessors return the same *StoredBlockState /
// *pool / pending block store / block tree pointers as flattened FetchManager fields (test harness + leader runtime wiring).
func TestFormalRuntimePointerIdentity(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	if got, want := fm.runtimeStoredBlocks(), fm.storedBlocks; got != want {
		t.Fatalf("runtimeStoredBlocks %p != fm.storedBlocks %p", got, want)
	}
	if got, want := fm.runtimeTaskPool(), fm.taskPool; got != want {
		t.Fatalf("runtimeTaskPool %p != fm.taskPool %p", got, want)
	}
	if got, want := fm.runtimePendingBlockStore(), fm.pendingBlockStore; got != want {
		t.Fatalf("runtimePendingBlockStore %p != fm.pendingBlockStore %p", got, want)
	}
	if got, want := fm.runtimeBlockTree(), fm.blockTree; got != want {
		t.Fatalf("runtimeBlockTree %p != fm.blockTree %p", got, want)
	}
}

// TestFormalCapturePruneSnapshot matches direct prune-runtime reads against the fetch-manager wiring.
func TestFormalCapturePruneSnapshot(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.storedBlocks.MarkStored("a")

	pruneDeps := fm.scanFlowRuntimeDeps().PruneRuntime
	directDeps := fetchscan.PruneRuntimeDeps{
		BlockTree:         fm.runtimeBlockTree(),
		PendingBlockStore: fm.runtimePendingBlockStore(),
		StoredBlocks:      fm.runtimeStoredBlocks(),
		TaskPool:          fm.runtimeTaskPool(),
		NormalizeHash:     normalizeHash,
	}
	snapViaFM := pruneDeps.CaptureStateSnapshot()
	snapViaDeps := directDeps.CaptureStateSnapshot()

	if snapViaFM == nil || snapViaDeps == nil {
		t.Fatal("snapshots must be non-nil")
	}
	if !reflect.DeepEqual(snapViaFM, snapViaDeps) {
		t.Fatalf("scanFlowRuntimeDeps().PruneRuntime.CaptureStateSnapshot mismatch\nfm:%#v\ndeps:%#v", snapViaFM, snapViaDeps)
	}

	min1, max1, ok1 := pruneDeps.StoredHeightRangeOnTree()
	min2, max2, ok2 := directDeps.StoredHeightRangeOnTree()
	if ok1 != ok2 || min1 != min2 || max1 != max2 {
		t.Fatalf("scanFlowRuntimeDeps().PruneRuntime.StoredHeightRangeOnTree mismatch: fm=(%v,%v,%v) deps=(%v,%v,%v)", min1, max1, ok1, min2, max2, ok2)
	}
	if !ok1 {
		t.Fatal("expected stored height range after MarkStored on linked node")
	}
}

// TestFormalScanFlowPruneRuntimeIdentity locks the new wiring: scan flow should receive the same
// prune runtime pointers that fetch manager assembles for standalone prune helpers.
func TestFormalScanFlowPruneRuntimeIdentity(t *testing.T) {
	t.Parallel()
	fm := newTestFetchManager(t, 2)
	scanDeps := fm.scanFlowRuntimeDeps()
	pruneDeps := fetchscan.PruneRuntimeDeps{
		BlockTree:         fm.runtimeBlockTree(),
		PendingBlockStore: fm.runtimePendingBlockStore(),
		StoredBlocks:      fm.runtimeStoredBlocks(),
		TaskPool:          fm.runtimeTaskPool(),
		NormalizeHash:     normalizeHash,
	}

	if scanDeps.PruneRuntime.BlockTree != pruneDeps.BlockTree {
		t.Fatal("scan flow prune runtime must share blockTree pointer with runtime deps")
	}
	if scanDeps.PruneRuntime.StoredBlocks != pruneDeps.StoredBlocks {
		t.Fatal("scan flow prune runtime must share storedBlocks pointer with runtime deps")
	}
	if scanDeps.PruneRuntime.TaskPool != pruneDeps.TaskPool {
		t.Fatal("scan flow prune runtime must share taskPool pointer with runtime deps")
	}
	if scanDeps.PruneRuntime.PendingBlockStore != pruneDeps.PendingBlockStore {
		t.Fatal("scan flow prune runtime must share pendingBlockStore pointer with runtime deps")
	}
}
