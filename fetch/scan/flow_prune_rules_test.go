package scan

import (
	"fmt"
	"testing"
)

func TestRunScanCycleRule5PruneRemovesStoredAndTasks(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.attachAsyncTaskPool()
	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		env.blockTree.Insert(h, hash, parent, 1)
		env.stored.MarkStored(hash)
		env.taskPool.AddTask(hash)
		parent = hash
	}
	env.setLatestRemote(6)
	env.runScanAndWait()

	for h := uint64(1); h <= 3; h++ {
		hash := fmt.Sprintf("h%v", h)
		if env.stored.IsStored(hash) {
			t.Fatalf("pruned hash should be removed from storedBlocks: %v", hash)
		}
		if env.taskPool.HasTask(hash) {
			t.Fatalf("pruned hash should be removed from taskPool: %v", hash)
		}
	}
	for h := uint64(4); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		if !env.stored.IsStored(hash) {
			t.Fatalf("kept hash should remain stored: %v", hash)
		}
	}
}
