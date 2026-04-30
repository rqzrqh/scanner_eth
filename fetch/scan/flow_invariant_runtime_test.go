package scan

import (
	"context"
	"errors"
	fetchstore "scanner_eth/fetch/store"
	"sync"
	"testing"
)

func TestInsertHeaderDoesNotCacheForBodySync(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.InsertHeader(makeTestHeader(10, "0x10", "0x0f"))
	if env.blockTree.Get("0x10") == nil {
		t.Fatal("expected block in tree")
	}
	if env.pending.GetNodeBlockHeader("0x10") != nil {
		t.Fatal("insertHeader must not cache header; body sync refetches by hash")
	}
}

func TestInvariantHeaderInsertRejectedTreeUnchanged(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.flow.InsertHeader(makeTestHeader(10, "0xroot", ""))
	env.flow.InsertHeader(makeTestHeader(10, "0x10", "0x0f"))

	if env.blockTree.Get("0x10") != nil {
		t.Fatal("expected 0x10 to be rejected by blocktree insert guard")
	}
	if env.pending.GetNodeBlockHeader("0x10") != nil {
		t.Fatal("expected no cached header for rejected insert")
	}
}

func TestInvariantStoredOnlyAfterSuccessfulStore(t *testing.T) {
	t.Run("success path adds stored hash", func(t *testing.T) {
		env := newTestFlowEnv(t, 2)
		env.blockTree.Insert(1, "a", "", 1, nil)
		env.blockTree.Insert(2, "b", "a", 1, nil)
		env.stored.MarkStored("a")
		env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
		env.pending.SetNodeBlockBody("b", makeTestBody(true))
		env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error { return nil })

		env.flow.ProcessBranchesLowToHigh(context.Background())
		if !env.stored.IsStored("b") {
			t.Fatal("expected b to be marked stored after successful DB write")
		}
	})

	t.Run("failure path does not add stored hash", func(t *testing.T) {
		env := newTestFlowEnv(t, 2)
		env.blockTree.Insert(1, "a", "", 1, nil)
		env.blockTree.Insert(2, "b", "a", 1, nil)
		env.stored.MarkStored("a")
		env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
		env.pending.SetNodeBlockBody("b", makeTestBody(true))
		env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error {
			return errors.New("db write failed")
		})

		env.flow.ProcessBranchesLowToHigh(context.Background())
		if env.stored.IsStored("b") {
			t.Fatal("expected b to remain not stored when DB write fails")
		}
	})
}

func TestProcessBranchesDoesNotStoreWhenParentNotStored(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1, nil)
	env.blockTree.Insert(2, "b", "a", 1, nil)
	env.blockTree.Insert(3, "c", "b", 1, nil)
	env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
	env.pending.SetNodeBlockHeader("c", makeTestHeader(3, "c", "b"))
	env.pending.SetNodeBlockBody("b", makeTestEventBlockData(2, "b", "a"))
	env.pending.SetNodeBlockBody("c", makeTestEventBlockData(3, "c", "b"))

	writeOrder := make([]string, 0)
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		if blockData != nil && blockData.StorageFullBlock != nil {
			writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
		}
		return nil
	})

	env.flow.ProcessBranchesLowToHigh(context.Background())
	if len(writeOrder) != 0 {
		t.Fatalf("expected no writes when parent is not stored, got=%v", writeOrder)
	}
	if env.stored.IsStored("b") || env.stored.IsStored("c") {
		t.Fatalf("child nodes should not be marked stored when parent is not stored")
	}
	if env.taskPool.HasTask("b") || env.taskPool.HasTask("c") {
		t.Fatalf("nodes with data but blocked parent should be skipped without auto-enqueueing tasks")
	}
}

func TestProcessBranchesMarksStoredOnlyOnSuccessfulWrite(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1, nil)
	env.blockTree.Insert(2, "b", "a", 1, nil)
	env.stored.MarkStored("a")
	env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
	env.pending.SetNodeBlockBody("b", makeTestEventBlockData(2, "b", "a"))

	called := 0
	env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error {
		called++
		return errors.New("write failed")
	})

	env.flow.ProcessBranchesLowToHigh(context.Background())
	if called != 1 {
		t.Fatalf("expected one write attempt, got=%d", called)
	}
	if env.stored.IsStored("b") {
		t.Fatalf("block should not be marked stored on write failure")
	}
}

func TestStoreNodeBodyDataDedupesConcurrentStoreRequests(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1, nil)
	env.blockTree.Insert(2, "b", "a", 1, nil)
	env.stored.MarkStored("a")
	env.pending.SetNodeBlockHeader("b", makeTestHeader(2, "b", "a"))
	nodeData := makeTestEventBlockData(2, "b", "a")
	env.pending.SetNodeBlockBody("b", nodeData)

	node := env.blockTree.Get("b")
	if node == nil {
		t.Fatal("expected blocktree node for b")
	}
	state, ok := env.flow.BuildBranchProcessState(node)
	if !ok {
		t.Fatal("expected branch state for b")
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls int
	var mu sync.Mutex
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		mu.Lock()
		calls++
		mu.Unlock()
		started <- struct{}{}
		<-release
		return nil
	})

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		defer close(done1)
		env.flow.StoreNodeBodyData(context.Background(), node, nodeData, state)
	}()
	<-started
	go func() {
		defer close(done2)
		env.flow.StoreNodeBodyData(context.Background(), node, nodeData, state)
	}()

	close(release)
	<-done1
	<-done2

	mu.Lock()
	gotCalls := calls
	mu.Unlock()
	if gotCalls != 1 {
		t.Fatalf("expected one serialized store attempt, got=%d", gotCalls)
	}
	if !env.stored.IsStored("b") {
		t.Fatal("expected b to be marked stored after worker write succeeds")
	}
	if env.store != nil && env.store.IsInflight("b") {
		t.Fatal("expected b to be removed from in-flight store state after success")
	}
}
