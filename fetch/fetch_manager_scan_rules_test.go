package fetch

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"

	"scanner_eth/data"
	fetcherpkg "scanner_eth/fetch/fetcher"
)

func TestScanEventsRule1BootstrapOnEmptyTree(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	setTestScanStartHeight(fm, 100)
	var calls atomic.Int32
	setNodeLatestHeight(fm, 100)
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		if height != 100 {
			t.Fatalf("unexpected bootstrap height: %v", height)
		}
		calls.Add(1)
		return makeHeader(height, "0xaaa", "")
	}})

	runScanAndWait(t, fm)
	if calls.Load() != 1 {
		t.Fatalf("bootstrap header fetch count mismatch: got=%v want=1", calls.Load())
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 100 || end != 100 {
		t.Fatalf("bootstrap range mismatch: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestScanEventsRule2WindowExpandsToDoubleIrreversible(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0xa", ""))
	setNodeLatestHeight(fm, 13)
	setTestDbOperator(fm, &mockDbOperator{})
	var calls atomic.Int32
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchByHeightFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, height uint64) *BlockHeaderJson {
		calls.Add(1)
		switch height {
		case 11:
			return makeHeader(11, "0xb", "0xa")
		case 12:
			return makeHeader(12, "0xc", "0xb")
		case 13:
			return makeHeader(13, "0xd", "0xc")
		default:
			return nil
		}
	}, fetchFullFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		if header == nil {
			return nil
		}
		h := uint64(0)
		if header.Number != "" {
			if v, err := hexutil.DecodeUint64(header.Number); err == nil {
				h = v
			}
		}
		return makeMinimalFullBlock(h, normalizeHash(header.Hash), normalizeHash(header.ParentHash))
	}})

	runScanAndWait(t, fm)
	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 10 || end < 11 {
		t.Fatalf("range mismatch after window sync: ok=%v start=%v end=%v", ok, start, end)
	}
	if calls.Load() < 1 {
		t.Fatalf("window sync call mismatch: got=%v want>=1", calls.Load())
	}
}

func TestScanEventsRule3SyncOrphanParentsByHash(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	mustTestScanFlow(t, fm).InsertHeader(makeHeader(10, "0xroot", ""))
	fm.blockTree.Insert(12, "0xchild", "0xmissing", 1, nil)
	setNodeLatestHeight(fm, 10)
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchByHashFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson {
		if hash != "0xmissing" {
			return nil
		}
		return makeHeader(11, "0xmissing", "0xroot")
	}})

	runScanAndWait(t, fm)
	if fm.blockTree.Get("0xmissing") == nil {
		t.Fatalf("missing parent should be inserted")
	}
	if fm.blockTree.Get("0xchild") == nil {
		t.Fatalf("orphan child should be linked after parent sync")
	}
}

func TestScanEventsRule4BranchWriteCondition(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil)
	fm.blockTree.Insert(2, "y", "a", 1, nil)
	fm.blockTree.Insert(3, "z", "y", 1, nil)
	setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
	setTestNodeBlockHeader(t, fm, "c", makeHeader(3, "c", "b"))
	setTestNodeBlockHeader(t, fm, "y", makeHeader(2, "y", "a"))
	setTestNodeBlockHeader(t, fm, "z", makeHeader(3, "z", "y"))

	setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))
	setTestNodeBlockBody(t, fm, "c", makeEventBlockData(3, "c", "b"))
	setTestNodeBlockBody(t, fm, "y", makeEventBlockData(2, "y", "a"))
	setTestNodeBlockBody(t, fm, "z", makeEventBlockData(3, "z", "y"))

	fm.storedBlocks.MarkStored("a")
	setNodeLatestHeight(fm, 3)

	writeOrder := make([]string, 0)
	var writeOrderMu sync.Mutex
	setTestDbOperator(fm, &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			if blockData != nil && blockData.StorageFullBlock != nil {
				writeOrderMu.Lock()
				writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
				writeOrderMu.Unlock()
			}
			return nil
		},
	})

	runScanAndWait(t, fm)
	writeOrderMu.Lock()
	defer writeOrderMu.Unlock()

	want := []string{"b", "c", "y", "z"}
	if len(writeOrder) != len(want) {
		t.Fatalf("write count mismatch: got=%v want=%v order=%v", len(writeOrder), len(want), writeOrder)
	}
	for i := range want {
		if writeOrder[i] != want[i] {
			t.Fatalf("write order mismatch at %v: got=%v want=%v full=%v", i, writeOrder[i], want[i], writeOrder)
		}
	}
}

func TestScanEventsRule5PruneRemovesStoredAndTasks(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil)
		fm.storedBlocks.MarkStored(hash)
		fm.taskPool.AddTask(hash)
		parent = hash
	}
	setNodeLatestHeight(fm, 6)

	runScanAndWait(t, fm)

	for h := uint64(1); h <= 3; h++ {
		hash := fmt.Sprintf("h%v", h)
		if fm.storedBlocks.IsStored(hash) {
			t.Fatalf("pruned hash should be removed from storagedBlock: %v", hash)
		}
		if fm.taskPool.HasTask(hash) {
			t.Fatalf("pruned hash should be removed from taskManager: %v", hash)
		}
	}
	for h := uint64(4); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		if !fm.storedBlocks.IsStored(hash) {
			t.Fatalf("kept hash should remain stored: %v", hash)
		}
	}
}
