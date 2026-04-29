package fetch

import (
	"context"
	"fmt"
	"testing"
)

func TestInvariantPruneDeletesPendingAndStored(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil)
		fm.storedBlocks.MarkStored(hash)
		setTestNodeBlockHeader(t, fm, hash, makeHeader(h, hash, parent))
		setTestNodeBlockBody(t, fm, hash, makeEventBlockData(h, hash, parent))
		parent = hash
	}

	// C4 / I3: pruned hashes must also disappear from taskPool (prune_state delTask).
	for _, hash := range []string{"h1", "h2", "h3"} {
		fm.taskPool.AddTask(hash)
	}

	fm.pruneStoredBlocks(context.Background())

	for _, hash := range []string{"h1", "h2", "h3"} {
		if fm.storedBlocks.IsStored(hash) {
			t.Fatalf("expected %s removed from pending stored state after prune", hash)
		}
		if fm.pendingPayloadStore.GetHeader(hash) != nil {
			t.Fatalf("expected %s header removed from pending after prune", hash)
		}
		if fm.pendingPayloadStore.GetBody(hash) != nil {
			t.Fatalf("expected %s body removed from pending after prune", hash)
		}
		if fm.taskPool.HasTask(hash) {
			t.Fatalf("expected %s task removed from taskPool after prune", hash)
		}
	}
}
