package fetch

import (
	"errors"
	"fmt"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
)

func TestPlanP2DBIntermittentFailureThenRecovery(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
	fm.storedBlocks.MarkStored("a")
	fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
	fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))

	attempts := 0
	fm.dbOperator = &mockDbOperator{storeFn: func(blockData *EventBlockData) error {
		attempts++
		if attempts <= 2 {
			return errors.New("db temporary unavailable")
		}
		return nil
	}}

	fm.processBranchesLowToHigh()
	if fm.storedBlocks.IsStored("b") {
		t.Fatal("b should not be marked stored after first failed write")
	}

	fm.processBranchesLowToHigh()
	if fm.storedBlocks.IsStored("b") {
		t.Fatal("b should not be marked stored after second failed write")
	}

	fm.processBranchesLowToHigh()
	if !fm.storedBlocks.IsStored("b") {
		t.Fatal("b should be marked stored after recovery write succeeds")
	}
	if attempts != 3 {
		t.Fatalf("unexpected store attempts: got=%d want=3", attempts)
	}
}

func TestPlanP3HeaderFetchTimeoutAndDisorderConverges(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.insertHeader(makeHeader(10, "0x0a", ""))
	setNodeLatestHeight(fm, 12)

	height11Calls := 0
	fm.blockFetcher = &mockBlockFetcher{
		fetchByHashFn: func(nodeID int, taskID int, client *ethclient.Client, hash string) *BlockHeaderJson {
			if hash != "0x0c" {
				return nil
			}
			// Child arrives before parent (disorder case).
			return makeHeader(12, "0x0c", "0x0b")
		},
		fetchByHeightFn: func(nodeID int, taskID int, client *ethclient.Client, height uint64) *BlockHeaderJson {
			if height != 11 {
				return nil
			}
			height11Calls++
			if height11Calls == 1 {
				// Simulate timeout/failure on first attempt.
				return nil
			}
			// Parent eventually arrives.
			return makeHeader(11, "0x0b", "0x0a")
		},
	}

	if ok := fm.fetchAndInsertHeaderByHash("0x0c"); !ok {
		t.Fatal("expected child header insertion by hash to succeed")
	}
	if fm.blockTree.Get("0x0c") != nil {
		t.Fatal("child should stay orphan before parent arrives")
	}

	if got := fm.fetchAndInsertHeaderByHeight(11); got != nil {
		t.Fatal("expected first height-11 fetch to fail (timeout simulation)")
	}
	if got := fm.fetchAndInsertHeaderByHeight(11); got == nil {
		t.Fatal("expected second height-11 fetch to recover and succeed")
	}

	n11 := fm.blockTree.Get("0x0b")
	if n11 == nil {
		t.Fatal("expected height-11 header to converge into blocktree")
	}
	n12 := fm.blockTree.Get("0x0c")
	if n12 == nil {
		t.Fatal("expected height-12 header to converge into blocktree")
	}
	if n12.ParentKey != "0x0b" {
		t.Fatalf("unexpected parent for 0x0c: got=%s want=0x0b", n12.ParentKey)
	}

	if h := fm.pendingPayloadStore.GetBlockHeader("0x0b"); h == nil {
		t.Fatal("expected pending header entry for 0x0b")
	}
	if h := fm.pendingPayloadStore.GetBlockHeader("0x0c"); h == nil {
		t.Fatal("expected pending header entry for 0x0c")
	}

	if n11.Height != 11 || n12.Height != 12 {
		t.Fatalf("unexpected linked heights: h11=%d h12=%d", n11.Height, n12.Height)
	}

	if got := fm.pendingPayloadStore.GetBlockHeader("0x0b").Number; got != hexutil.EncodeUint64(11) {
		t.Fatalf("unexpected number for 0x0b: %s", got)
	}
	if got := fm.pendingPayloadStore.GetBlockHeader("0x0c").Number; got != hexutil.EncodeUint64(12) {
		t.Fatalf("unexpected number for 0x0c: %s", got)
	}
}

func TestPlanP5FrequentPruneAndStoreAlternationConsistency(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	// Build a single linked chain and preload header/body payloads.
	parent := ""
	for h := uint64(1); h <= 12; h++ {
		hash := fmt.Sprintf("h%d", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil, nil)
		fm.setNodeBlockHeader(hash, makeHeader(h, hash, parent))
		fm.setNodeBlockBody(hash, makeEventBlockData(h, hash, parent))
		parent = hash
	}

	// Seed genesis as already stored to allow downstream parentReady progression.
	fm.storedBlocks.MarkStored("h1")
	fm.dbOperator = &mockDbOperator{storeFn: func(blockData *EventBlockData) error {
		return nil
	}}

	// Alternate write/prune multiple rounds to emulate high-frequency interleaving.
	for i := 0; i < 8; i++ {
		fm.processBranchesLowToHigh()
		fm.pruneStoredBlocks()
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
		if fm.pendingPayloadStore.GetBlockHeader(hash) != nil {
			t.Fatalf("expected pruned hash %s header to be removed from pending", hash)
		}
		if fm.pendingPayloadStore.GetBlockBody(hash) != nil {
			t.Fatalf("expected pruned hash %s body to be removed from pending", hash)
		}
	}

	// Stored hashes must still exist in tree (no stale stored entries).
	fm.storedBlocks.mu.Lock()
	storedSnapshot := make(map[string]struct{}, len(fm.storedBlocks.hashes))
	for k := range fm.storedBlocks.hashes {
		storedSnapshot[k] = struct{}{}
	}
	fm.storedBlocks.mu.Unlock()

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
		fm.blockTree.Insert(h, hash, parent, 1, nil, nil)
		parent = hash
	}

	// Group A: low-height orphans that should be removed by prune as root advances.
	for h := uint64(2); h <= 21; h++ {
		childKey := fmt.Sprintf("oa%d", h)
		missingParentKey := fmt.Sprintf("ma%d", h)
		fm.blockTree.Insert(h, childKey, missingParentKey, 1, nil, nil)
	}

	// Group B: high-height orphans for parent backfill convergence checks.
	for h := uint64(40); h <= 99; h++ {
		childKey := fmt.Sprintf("ob%d", h)
		missingParentKey := fmt.Sprintf("pb%d", h)
		fm.blockTree.Insert(h, childKey, missingParentKey, 1, nil, nil)
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
		fm.blockTree.Insert(h-1, parentKey, "h30", 1, nil, nil)
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
