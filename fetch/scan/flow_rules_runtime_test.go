package scan

import (
	"context"
	"fmt"
	fetchstore "scanner_eth/fetch/store"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

func TestRunScanCycleRule1BootstrapOnEmptyTree(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.setStartHeight(100)
	var calls atomic.Int32
	env.latestRemote = 100
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *BlockHeaderJson {
		if height != 100 {
			t.Fatalf("unexpected bootstrap height: %v", height)
		}
		calls.Add(1)
		return makeTestHeader(height, "0xaaa", "")
	}

	env.runScanAndWait()
	if calls.Load() != 1 {
		t.Fatalf("bootstrap header fetch count mismatch: got=%v want=1", calls.Load())
	}

	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 100 || end != 100 {
		t.Fatalf("bootstrap range mismatch: ok=%v start=%v end=%v", ok, start, end)
	}
}

func TestRunScanCycleRule2WindowExpandsToDoubleIrreversible(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.InsertHeader(makeTestHeader(10, "0xa", ""))
	env.latestRemote = 13
	var calls atomic.Int32
	env.fetchHeaderByHeightFn = func(_ context.Context, height uint64) *BlockHeaderJson {
		calls.Add(1)
		switch height {
		case 11:
			return makeTestHeader(11, "0xb", "0xa")
		case 12:
			return makeTestHeader(12, "0xc", "0xb")
		case 13:
			return makeTestHeader(13, "0xd", "0xc")
		default:
			return nil
		}
	}
	env.fetchBodyByHashFn = func(_ context.Context, hash string, _ uint64, header *BlockHeaderJson) (*fetchstore.EventBlockData, int, int64, bool) {
		if header == nil {
			return nil, -1, 0, false
		}
		h := uint64(0)
		if header.Number != "" {
			if v, err := hexutil.DecodeUint64(header.Number); err == nil {
				h = v
			}
		}
		return makeTestEventBlockData(h, normalizeTestHash(header.Hash), normalizeTestHash(header.ParentHash)), -1, 0, true
	}

	env.runScanAndWait()
	start, end, ok := env.blockTree.HeightRange()
	if !ok || start != 10 || end < 11 {
		t.Fatalf("range mismatch after window sync: ok=%v start=%v end=%v", ok, start, end)
	}
	if calls.Load() < 1 {
		t.Fatalf("window sync call mismatch: got=%v want>=1", calls.Load())
	}
}

func TestRunScanCycleRule3SyncOrphanParentsByHash(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.InsertHeader(makeTestHeader(10, "0xroot", ""))
	env.blockTree.Insert(12, "0xchild", "0xmissing", 1, nil)
	env.latestRemote = 10
	env.fetchHeaderByHashFn = func(_ context.Context, hash string) *BlockHeaderJson {
		if hash != "0xmissing" {
			return nil
		}
		return makeTestHeader(11, "0xmissing", "0xroot")
	}

	env.runScanAndWait()
	if env.blockTree.Get("0xmissing") == nil {
		t.Fatalf("missing parent should be inserted")
	}
	if env.blockTree.Get("0xchild") == nil {
		t.Fatalf("orphan child should be linked after parent sync")
	}
}

func TestRunScanCycleRule4BranchWriteCondition(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1, nil)
	env.blockTree.Insert(2, "b", "a", 1, nil)
	env.blockTree.Insert(3, "c", "b", 1, nil)
	env.blockTree.Insert(2, "y", "a", 1, nil)
	env.blockTree.Insert(3, "z", "y", 1, nil)
	env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
	env.pending.SetNodeBlockHeader("c", makeTestHeader(3, "c", "b"))
	env.pending.SetNodeBlockHeader("y", makeTestHeader(2, "y", "a"))
	env.pending.SetNodeBlockHeader("z", makeTestHeader(3, "z", "y"))
	env.pending.SetNodeBlockBody("b", makeTestEventBlockData(2, "b", "a"))
	env.pending.SetNodeBlockBody("c", makeTestEventBlockData(3, "c", "b"))
	env.pending.SetNodeBlockBody("y", makeTestEventBlockData(2, "y", "a"))
	env.pending.SetNodeBlockBody("z", makeTestEventBlockData(3, "z", "y"))
	env.stored.MarkStored("a")
	env.latestRemote = 3

	writeOrder := make([]string, 0)
	var writeOrderMu sync.Mutex
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		if blockData != nil && blockData.StorageFullBlock != nil {
			writeOrderMu.Lock()
			writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
			writeOrderMu.Unlock()
		}
		return nil
	})

	env.runScanAndWait()
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

func TestRunScanCycleRule5PruneRemovesStoredAndTasks(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.attachAsyncTaskPool()
	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		env.blockTree.Insert(h, hash, parent, 1, nil)
		env.stored.MarkStored(hash)
		env.taskPool.AddTask(hash)
		parent = hash
	}
	env.latestRemote = 6
	env.runScanAndWait()

	for h := uint64(1); h <= 3; h++ {
		hash := fmt.Sprintf("h%v", h)
		if env.stored.IsStored(hash) {
			t.Fatalf("pruned hash should be removed from storagedBlock: %v", hash)
		}
		if env.taskPool.HasTask(hash) {
			t.Fatalf("pruned hash should be removed from taskManager: %v", hash)
		}
	}
	for h := uint64(4); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		if !env.stored.IsStored(hash) {
			t.Fatalf("kept hash should remain stored: %v", hash)
		}
	}
}
