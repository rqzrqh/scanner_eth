package fetch

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

func TestInvariantHeaderInsertThenPending(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	header := makeHeader(10, "0x10", "0x0f")

	fm.insertHeader(header)

	if fm.blockTree.Get("0x10") == nil {
		t.Fatal("expected block to be inserted into blocktree")
	}
	got := fm.pendingPayloadStore.GetBlockHeader("0x10")
	if got == nil || normalizeHash(got.Hash) != "0x10" {
		t.Fatalf("expected pending header for 0x10, got=%+v", got)
	}
}

func TestInvariantHeaderInsertRejectedStillWritesPendingHeader(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	// Build root at height 10.
	fm.insertHeader(makeHeader(10, "0xroot", ""))

	// Same height as root but different key: Insert will be rejected by blocktree.
	rejected := makeHeader(10, "0x10", "0x0f")
	fm.insertHeader(rejected)

	if fm.blockTree.Get("0x10") != nil {
		t.Fatal("expected 0x10 to be rejected by blocktree insert guard")
	}
	if got := fm.pendingPayloadStore.GetBlockHeader("0x10"); got == nil {
		t.Fatal("expected pending header to still be written on insert rejection path")
	}
}

func TestInvariantSetBlockBodyNoImplicitCreate(t *testing.T) {
	store := NewBlockPayloadStore()
	body := makeEventBlockData(10, "0x10", "0x0f")

	store.SetBlockBody("0x10", body)

	if got := store.GetBlockBody("0x10"); got != nil {
		t.Fatalf("expected nil body for missing payload key, got=%+v", got)
	}
}

func TestInvariantStoredOnlyAfterSuccessfulStore(t *testing.T) {
	t.Run("success path adds stored hash", func(t *testing.T) {
		fm := newTestFetchManager(t, 2)
		fm.blockTree.Insert(1, "a", "", 1, nil, nil)
		fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
		fm.storedBlocks.MarkStored("a")
		fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
		fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))

		fm.dbOperator = &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
			return nil
		}}

		fm.processBranchesLowToHigh(context.Background())
		if !fm.storedBlocks.IsStored("b") {
			t.Fatal("expected b to be marked stored after successful DB write")
		}
	})

	t.Run("failure path does not add stored hash", func(t *testing.T) {
		fm := newTestFetchManager(t, 2)
		fm.blockTree.Insert(1, "a", "", 1, nil, nil)
		fm.blockTree.Insert(2, "b", "a", 1, nil, nil)
		fm.storedBlocks.MarkStored("a")
		fm.setNodeBlockHeader("b", makeHeader(2, "b", "a"))
		fm.setNodeBlockBody("b", makeEventBlockData(2, "b", "a"))

		fm.dbOperator = &mockDbOperator{storeFn: func(context.Context, *EventBlockData) error {
			return errors.New("db write failed")
		}}

		fm.processBranchesLowToHigh(context.Background())
		if fm.storedBlocks.IsStored("b") {
			t.Fatal("expected b to remain not stored when DB write fails")
		}
	})
}

func TestInvariantPruneDeletesPendingAndStored(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := fmt.Sprintf("h%v", h)
		fm.blockTree.Insert(h, hash, parent, 1, nil, nil)
		fm.storedBlocks.MarkStored(hash)
		fm.setNodeBlockHeader(hash, makeHeader(h, hash, parent))
		fm.setNodeBlockBody(hash, makeEventBlockData(h, hash, parent))
		parent = hash
	}

	// C4 / I3: pruned hashes must also disappear from taskPool (prune_state delTask).
	for _, hash := range []string{"h1", "h2", "h3"} {
		fm.taskPool.addTask(hash)
	}

	fm.pruneStoredBlocks(context.Background())

	for _, hash := range []string{"h1", "h2", "h3"} {
		if fm.storedBlocks.IsStored(hash) {
			t.Fatalf("expected %s removed from pending stored state after prune", hash)
		}
		if fm.pendingPayloadStore.GetBlockHeader(hash) != nil {
			t.Fatalf("expected %s header removed from pending after prune", hash)
		}
		if fm.pendingPayloadStore.GetBlockBody(hash) != nil {
			t.Fatalf("expected %s body removed from pending after prune", hash)
		}
		if fm.taskPool.hasTask(hash) {
			t.Fatalf("expected %s task removed from taskPool after prune", hash)
		}
	}
}

// TestInvariantI5StoredBlockStateNormalizedClosure locks §4 I5: keys in D are normalized and non-empty.
func TestInvariantI5StoredBlockStateNormalizedClosure(t *testing.T) {
	s := newStoredBlockState()
	s.MarkStored("")
	s.MarkStored("0xAaBb")

	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.hashes) != 1 {
		t.Fatalf("I5: expected exactly one key in D, got %d", len(s.hashes))
	}
	for k := range s.hashes {
		if k == "" {
			t.Fatal("I5: empty key must not appear in D")
		}
		if k != normalizeHash(k) {
			t.Fatalf("I5: key %q must equal normalizeHash(key)", k)
		}
		if k != "0xaabb" {
			t.Fatalf("I5: expected lowercase canonical hex, got %q", k)
		}
	}
}
