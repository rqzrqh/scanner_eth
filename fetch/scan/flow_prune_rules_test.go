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

	for h := uint64(1); h <= 2; h++ {
		hash := fmt.Sprintf("h%v", h)
		if env.stored.IsStored(hash) {
			t.Fatalf("pruned hash should be removed from storedBlocks: %v", hash)
		}
		if env.taskPool.HasTask(hash) {
			t.Fatalf("pruned hash should be removed from taskPool: %v", hash)
		}
	}
	if !env.stored.IsStored("h3") {
		t.Fatal("root parent readiness marker should remain stored after runtime prune")
	}
	if env.taskPool.HasTask("h3") {
		t.Fatal("root parent readiness marker should not keep a task for the pruned block")
	}
	for h := uint64(4); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		if !env.stored.IsStored(hash) {
			t.Fatalf("kept hash should remain stored: %v", hash)
		}
	}
}
