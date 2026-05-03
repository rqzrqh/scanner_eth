package scan

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
	"sync"
	"testing"
)

func TestRunScanCycleRule4BranchWriteCondition(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.blockTree.Insert(2, "y", "a", 1)
	env.blockTree.Insert(3, "z", "y", 1)
	env.stagingStore.SetPendingHeader("b", makeTestHeader(2, "b", "a"))
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingHeader("y", makeTestHeader(2, "y", "a"))
	env.stagingStore.SetPendingHeader("z", makeTestHeader(3, "z", "y"))
	env.stagingStore.SetPendingBody("b", makeTestEventBlockData(2, "b", "a"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))
	env.stagingStore.SetPendingBody("y", makeTestEventBlockData(2, "y", "a"))
	env.stagingStore.SetPendingBody("z", makeTestEventBlockData(3, "z", "y"))
	env.stored.MarkStored("a")
	env.setLatestRemote(3)

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
