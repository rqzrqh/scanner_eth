package scan

import (
	"context"
	"errors"
	fetchstore "scanner_eth/fetch/store"
	"strings"
	"sync"
	"testing"
)

func TestStoreNodeBodyDataDedupesConcurrentStoreRequests(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.seedStoredLinearBranch("a", "b")
	nodeData := makeTestEventBlockData(2, "b", "a")
	env.stagingStore.SetPendingBody("b", nodeData)

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

func TestSyncBodyBranchTargetStopsFailedBranchButContinuesOtherBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.blockTree.Insert(2, "d", "a", 1)
	env.stored.MarkStored("a")
	env.stagingStore.SetPendingHeader("b", makeTestHeader(2, "b", "a"))
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingHeader("d", makeTestHeader(2, "d", "a"))
	env.stagingStore.SetPendingBody("b", makeTestEventBlockData(2, "b", "a"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))
	env.stagingStore.SetPendingBody("d", makeTestEventBlockData(2, "d", "a"))

	writeOrder := make([]string, 0)
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		hash := ""
		if blockData != nil && blockData.StorageFullBlock != nil {
			hash = blockData.StorageFullBlock.Block.Hash
		}
		writeOrder = append(writeOrder, hash)
		if hash == "b" {
			return errors.New("db write failed")
		}
		return nil
	})

	target := strings.Join(env.flow.GetStoreBranchTargets(), bodyTargetBranchSep)
	ok, msg := env.flow.SyncStoreBranchTarget(context.Background(), target)
	if !ok || msg != "" {
		t.Fatalf("expected branch-aware body sync to succeed, ok=%v msg=%q", ok, msg)
	}
	if got := strings.Join(writeOrder, ","); got != "b,d" {
		t.Fatalf("expected failed branch to stop before c while sibling branch continues, got=%q", got)
	}
	if env.stored.IsStored("b") || env.stored.IsStored("c") {
		t.Fatal("failed branch nodes must remain unstored")
	}
	if !env.stored.IsStored("d") {
		t.Fatal("independent branch should continue storing after sibling branch failure")
	}
}
