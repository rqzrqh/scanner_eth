package fetch

import (
	"context"
	"errors"
	"testing"

	fetcherpkg "scanner_eth/fetch/fetcher"
)

func TestPlanP2DBIntermittentFailureThenRecovery(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.storedBlocks.MarkStored("a")
	setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
	setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))

	attempts := 0
	setTestDbOperator(fm, &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
		attempts++
		if attempts <= 2 {
			return errors.New("db temporary unavailable")
		}
		return nil
	}})

	mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
	if fm.storedBlocks.IsStored("b") {
		t.Fatal("b should not be marked stored after first failed write")
	}
	if fm.pendingPayloadStore.GetHeader("b") == nil || fm.pendingPayloadStore.GetBody("b") == nil {
		t.Fatal("pending header/body for b must remain after failed DB write for retry")
	}

	mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
	if fm.storedBlocks.IsStored("b") {
		t.Fatal("b should not be marked stored after second failed write")
	}
	if fm.pendingPayloadStore.GetHeader("b") == nil || fm.pendingPayloadStore.GetBody("b") == nil {
		t.Fatal("pending header/body for b must remain after second failed DB write")
	}

	mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())
	if !fm.storedBlocks.IsStored("b") {
		t.Fatal("b should be marked stored after recovery write succeeds")
	}
	if attempts != 3 {
		t.Fatalf("unexpected store attempts: got=%d want=3", attempts)
	}
}

func TestPlanP3HeaderFetchTimeoutAndDisorderConverges(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0x0a", ""))
	setNodeLatestHeight(fm, 12)

	height11Calls := 0
	setTestBlockFetcher(fm, &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson {
			if hash != "0x0c" {
				return nil
			}
			// Child arrives before parent (disorder case).
			return makeHeader(12, "0x0c", "0x0b")
		},
		fetchByHeightFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
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
	})

	if ok := mustTestScanFlow(t, fm).FetchAndInsertHeaderByHash("0x0c"); !ok {
		t.Fatal("expected child header insertion by hash to succeed")
	}
	if fm.blockTree.Get("0x0c") != nil {
		t.Fatal("child should stay orphan before parent arrives")
	}

	if got := mustTestScanFlow(t, fm).FetchAndInsertHeaderByHeight(11); got != nil {
		t.Fatal("expected first height-11 fetch to fail (timeout simulation)")
	}
	if got := mustTestScanFlow(t, fm).FetchAndInsertHeaderByHeight(11); got == nil {
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

	if n11.Height != 11 || n12.Height != 12 {
		t.Fatalf("unexpected linked heights: h11=%d h12=%d", n11.Height, n12.Height)
	}
}
