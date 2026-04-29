package fetch

import (
	"context"
	"fmt"
	"testing"
)

func TestPlanP5FrequentPruneAndStoreAlternationConsistency(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	// Build a single linked chain and preload header/body payloads.
	parent := ""
	for h := uint64(1); h <= 12; h++ {
		hash := fmt.Sprintf("h%d", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil)
		setTestNodeBlockHeader(t, fm, hash, makeHeader(h, hash, parent))
		setTestNodeBlockBody(t, fm, hash, makeEventBlockData(h, hash, parent))
		parent = hash
	}

	// Seed genesis as already stored to allow downstream parentReady progression.
	fm.storedBlocks.MarkStored("h1")
	setTestDbOperator(fm, &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
		return nil
	}})

	// Alternate write/prune multiple rounds to emulate high-frequency interleaving.
	for i := 0; i < 8; i++ {
		mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
		fm.pruneStoredBlocks(context.Background())
	}

	root := fm.blockTree.Root()
	if root == nil {
		t.Fatal("expected non-nil root after alternating prune/store")
	}
	if root.Height <= 1 {
		t.Fatalf("expected root to advance under alternating prune/store, got=%d", root.Height)
	}

	// For all hashes below root height, state must be consistently cleaned.
	for h := uint64(1); h < root.Height; h++ {
		hash := fmt.Sprintf("h%d", h)
		if fm.blockTree.Get(hash) != nil {
			t.Fatalf("expected pruned hash %s to be absent from tree", hash)
		}
		if fm.storedBlocks.IsStored(hash) {
			t.Fatalf("expected pruned hash %s to be absent from stored state", hash)
		}
		if fm.pendingPayloadStore.GetHeader(hash) != nil {
			t.Fatalf("expected pruned hash %s header to be removed from pending", hash)
		}
		if fm.pendingPayloadStore.GetBody(hash) != nil {
			t.Fatalf("expected pruned hash %s body to be removed from pending", hash)
		}
	}

	// Stored hashes must still exist in tree (no stale stored entries).
	storedSnapshot := snapshotTestStoredHashes(t, fm)

	for hash := range storedSnapshot {
		if fm.blockTree.Get(hash) == nil {
			t.Fatalf("stored hash %s should still exist in tree", hash)
		}
	}
}

func TestPlanP4OrphanCapacityPressureConverges(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	// Build linked main chain first so prune can continuously advance root.
	parent := ""
	for h := uint64(1); h <= 30; h++ {
		hash := fmt.Sprintf("h%d", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil)
		parent = hash
	}

	// Group A: low-height orphans that should be removed by prune as root advances.
	for h := uint64(2); h <= 21; h++ {
		childKey := fmt.Sprintf("oa%d", h)
		missingParentKey := fmt.Sprintf("ma%d", h)
		fm.blockTree.Insert(h, childKey, missingParentKey, 1, nil)
	}

	// Group B: high-height orphans for parent backfill convergence checks.
	for h := uint64(40); h <= 99; h++ {
		childKey := fmt.Sprintf("ob%d", h)
		missingParentKey := fmt.Sprintf("pb%d", h)
		fm.blockTree.Insert(h, childKey, missingParentKey, 1, nil)
	}

	initialOrphanParents := len(fm.blockTree.UnlinkedNodes())
	if initialOrphanParents != 80 {
		t.Fatalf("unexpected initial orphan parent count: got=%d want=80", initialOrphanParents)
	}

	// Periodic prune should remove a portion of low-height orphan group.
	for i := 0; i < 10; i++ {
		fm.blockTree.Prune(1)
	}

	// Backfill half of high-height orphan parents and prune in-between to emulate real load.
	for h := uint64(40); h <= 69; h++ {
		parentKey := fmt.Sprintf("pb%d", h)
		fm.blockTree.Insert(h-1, parentKey, "h30", 1, nil)
		fm.blockTree.Prune(1)
	}

	afterOrphanParents := fm.blockTree.UnlinkedNodes()
	afterCount := len(afterOrphanParents)
	if afterCount >= initialOrphanParents {
		t.Fatalf("expected orphan parent count to decrease, initial=%d after=%d", initialOrphanParents, afterCount)
	}

	afterSet := make(map[string]struct{}, len(afterOrphanParents))
	for _, key := range afterOrphanParents {
		afterSet[key] = struct{}{}
	}

	// Resolved parent keys should not remain in unlinked set.
	for h := uint64(40); h <= 69; h++ {
		resolvedParent := fmt.Sprintf("pb%d", h)
		if _, exists := afterSet[resolvedParent]; exists {
			t.Fatalf("resolved orphan parent should not remain unlinked: %s", resolvedParent)
		}
	}

	// Unresolved high parents should still exist (no incorrect deletion).
	for h := uint64(70); h <= 99; h++ {
		unresolvedParent := fmt.Sprintf("pb%d", h)
		if _, exists := afterSet[unresolvedParent]; !exists {
			t.Fatalf("unresolved orphan parent should remain unlinked: %s", unresolvedParent)
		}
	}
}
