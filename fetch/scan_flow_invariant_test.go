// Formal verification: global S=(T,P,D) checks C1/C1b/C3 and coordinated with FormalVerification.md.
package fetch

import (
	"context"
	"errors"
	"testing"
)

// C1. See FormalVerification.md §3.7.2: insertHeader never caches a full header, so
// newHeads-only slim headers and DB restore cannot skip eth_getBlockByHash on the body path.
func TestInsertHeaderDoesNotCacheForBodySync(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0x10", "0x0f"))
	if fm.blockTree.Get("0x10") == nil {
		t.Fatal("expected block in tree")
	}
	if fm.pendingPayloadStore.GetHeader("0x10") != nil {
		t.Fatal("insertHeader must not cache header; body sync refetches by hash")
	}
}

func TestInvariantHeaderInsertThenPending(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0x10", "0x0f"))

	if fm.blockTree.Get("0x10") == nil {
		t.Fatal("expected block to be inserted into blocktree")
	}
	// C1 + FormalVerification.md §3.7.2: payload header is not filled by insertHeader.
	if fm.pendingPayloadStore.GetHeader("0x10") != nil {
		t.Fatalf("expected no pending header from insertHeader alone, got=%+v", fm.pendingPayloadStore.GetHeader("0x10"))
	}
}

func TestInvariantHeaderInsertRejectedTreeUnchanged(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	// Build root at height 10.
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0xroot", ""))

	// Same height as root but different key: Insert will be rejected by blocktree.
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0x10", "0x0f"))

	if fm.blockTree.Get("0x10") != nil {
		t.Fatal("expected 0x10 to be rejected by blocktree insert guard")
	}
	if fm.pendingPayloadStore.GetHeader("0x10") != nil {
		t.Fatal("expected no cached header for rejected insert")
	}
}

func TestInvariantStoredOnlyAfterSuccessfulStore(t *testing.T) {
	t.Run("success path adds stored hash", func(t *testing.T) {
		fm := newTestFetchManager(t, 2)
		fm.blockTree.Insert(1, "a", "", 1, nil)
		fm.blockTree.Insert(2, "b", "a", 1, nil)
		fm.storedBlocks.MarkStored("a")
		setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
		setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))

		setTestDbOperator(fm, &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
			return nil
		}})

		mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
		if !fm.storedBlocks.IsStored("b") {
			t.Fatal("expected b to be marked stored after successful DB write")
		}
	})

	t.Run("failure path does not add stored hash", func(t *testing.T) {
		fm := newTestFetchManager(t, 2)
		fm.blockTree.Insert(1, "a", "", 1, nil)
		fm.blockTree.Insert(2, "b", "a", 1, nil)
		fm.storedBlocks.MarkStored("a")
		setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
		setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))

		setTestDbOperator(fm, &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
			return errors.New("db write failed")
		}})

		mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
		if fm.storedBlocks.IsStored("b") {
			t.Fatal("expected b to remain not stored when DB write fails")
		}
	})
}
