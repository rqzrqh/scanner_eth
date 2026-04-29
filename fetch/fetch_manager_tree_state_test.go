package fetch

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
)

func TestComplexOrphanCascadeInsertionTracksTreeAndStoredRange(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.storedBlocks.MarkStored("ghost")
	fm.storedBlocks.MarkStored("a")
	fm.storedBlocks.MarkStored("g")
	fm.storedBlocks.MarkStored("y")

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(5, "e", "d", 1, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil)
	fm.blockTree.Insert(7, "g", "f", 1, nil)
	fm.blockTree.Insert(6, "f", "e", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.blockTree.Insert(4, "d", "c", 1, nil)
	fm.blockTree.Insert(4, "x", "c", 1, nil)
	fm.blockTree.Insert(5, "y", "x", 1, nil)

	for _, key := range []string{"a", "b", "c", "d", "e", "f", "g", "x", "y"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to exist after cascade insert", key)
		}
	}

	root := fm.blockTree.Root()
	if root == nil || root.Key != "a" || root.Height != 1 {
		t.Fatalf("unexpected root after cascade insert: %+v", root)
	}

	start, end, ok := fm.blockTree.HeightRange()
	if !ok || start != 1 || end != 7 {
		t.Fatalf("unexpected height range after cascade insert: ok=%v start=%v end=%v", ok, start, end)
	}

	branches := fm.blockTree.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header == nil || branches[0].Header.Key != "g" {
		t.Fatalf("expected first branch header g, got=%+v", branches[0].Header)
	}
	if branches[1].Header == nil || branches[1].Header.Key != "y" {
		t.Fatalf("expected second branch header y, got=%+v", branches[1].Header)
	}

	branch0 := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		branch0 = append(branch0, node.Key)
	}
	if got := strings.Join(branch0, ","); got != "g,f,e,d,c,b,a" {
		t.Fatalf("unexpected main branch path: %s", got)
	}

	branch1 := make([]string, 0, len(branches[1].Nodes))
	for _, node := range branches[1].Nodes {
		branch1 = append(branch1, node.Key)
	}
	if got := strings.Join(branch1, ","); got != "y,x,c,b,a" {
		t.Fatalf("unexpected fork branch path: %s", got)
	}

	storedStart, storedEnd, storedOK := fm.storedHeightRangeOnTree()
	if !storedOK || storedStart != 1 || storedEnd != 7 {
		t.Fatalf("unexpected stored height range: ok=%v start=%v end=%v", storedOK, storedStart, storedEnd)
	}
}

func TestPruneComplexForkRemovesPrunedBranchesAndStoredState(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil)
	fm.blockTree.Insert(4, "d", "c", 1, nil)
	fm.blockTree.Insert(5, "e", "d", 1, nil)
	fm.blockTree.Insert(3, "x", "b", 1, nil)
	fm.blockTree.Insert(4, "y", "x", 1, nil)

	for _, key := range []string{"a", "b", "c", "d", "e", "x", "y"} {
		fm.storedBlocks.MarkStored(key)
		fm.taskPool.AddTask(key)
	}

	fm.pruneStoredBlocks(context.Background())

	root := fm.blockTree.Root()
	if root == nil || root.Key != "c" || root.Height != 3 {
		t.Fatalf("unexpected root after prune: %+v", root)
	}

	for _, key := range []string{"a", "b", "x", "y"} {
		if fm.blockTree.Get(key) != nil {
			t.Fatalf("expected pruned node %s to be removed from tree", key)
		}
		if fm.taskPool.HasTask(key) {
			t.Fatalf("expected pruned node %s to be removed from taskManager", key)
		}
		stored := fm.storedBlocks.IsStored(key)
		if stored {
			t.Fatalf("expected pruned node %s to be removed from storagedBlock", key)
		}
	}

	for _, key := range []string{"c", "d", "e"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected kept node %s to remain in tree", key)
		}
		if !fm.taskPool.HasTask(key) {
			t.Fatalf("expected kept node %s task to remain", key)
		}
		stored := fm.storedBlocks.IsStored(key)
		if !stored {
			t.Fatalf("expected kept node %s to remain in storagedBlock", key)
		}
	}

	branches := fm.blockTree.Branches()
	if len(branches) != 1 {
		t.Fatalf("expected 1 branch after prune, got=%d", len(branches))
	}
	path := make([]string, 0, len(branches[0].Nodes))
	for _, node := range branches[0].Nodes {
		path = append(path, node.Key)
	}
	if got := strings.Join(path, ","); got != "e,d,c" {
		t.Fatalf("unexpected remaining branch path after prune: %s", got)
	}
}

func TestProcessBranchesDoesNotStoreWhenParentNotStored(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.blockTree.Insert(3, "c", "b", 1, nil)
	setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
	setTestNodeBlockHeader(t, fm, "c", makeHeader(3, "c", "b"))

	setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))
	setTestNodeBlockBody(t, fm, "c", makeEventBlockData(3, "c", "b"))

	writeOrder := make([]string, 0)
	setTestDbOperator(fm, &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			if blockData != nil && blockData.StorageFullBlock != nil {
				writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
			}
			return nil
		},
	})

	mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())

	if len(writeOrder) != 0 {
		t.Fatalf("expected no writes when parent is not stored, got=%v", writeOrder)
	}
	if fm.storedBlocks.IsStored("b") || fm.storedBlocks.IsStored("c") {
		t.Fatalf("child nodes should not be marked stored when parent is not stored")
	}
	if fm.taskPool.HasTask("b") || fm.taskPool.HasTask("c") {
		t.Fatalf("nodes with data but blocked parent should be skipped without auto-enqueueing tasks")
	}
}

func TestProcessBranchesMarksStoredOnlyOnSuccessfulWrite(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.storedBlocks.MarkStored("a")
	setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
	setTestNodeBlockBody(t, fm, "b", makeEventBlockData(2, "b", "a"))

	called := 0
	setTestDbOperator(fm, &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			called++
			return errors.New("write failed")
		},
	})

	mustTestScanFlow(t, fm).ProcessBranchesLowToHigh(context.Background())

	if called != 1 {
		t.Fatalf("expected one write attempt, got=%d", called)
	}
	if fm.storedBlocks.IsStored("b") {
		t.Fatalf("block should not be marked stored on write failure")
	}
}

func TestStoreNodeBodyDataDedupesConcurrentStoreRequests(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(1, "a", "", 1, nil)
	fm.blockTree.Insert(2, "b", "a", 1, nil)
	fm.storedBlocks.MarkStored("a")
	setTestNodeBlockHeader(t, fm, "b", makeHeader(2, "b", "a"))
	nodeData := makeEventBlockData(2, "b", "a")
	setTestNodeBlockBody(t, fm, "b", nodeData)

	node := fm.blockTree.Get("b")
	if node == nil {
		t.Fatal("expected blocktree node for b")
	}
	state, ok := mustTestScanFlow(t, fm).BuildBranchProcessState(node)
	if !ok {
		t.Fatal("expected branch state for b")
	}

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	var calls int
	var mu sync.Mutex
	setTestDbOperator(fm, &mockDbOperator{
		storeFn: func(_ context.Context, blockData *EventBlockData) error {
			mu.Lock()
			calls++
			mu.Unlock()
			started <- struct{}{}
			<-release
			return nil
		},
	})

	done1 := make(chan struct{})
	done2 := make(chan struct{})
	go func() {
		defer close(done1)
		mustTestScanFlow(t, fm).StoreNodeBodyData(context.Background(), node, nodeData, state)
	}()
	<-started
	go func() {
		defer close(done2)
		mustTestScanFlow(t, fm).StoreNodeBodyData(context.Background(), node, nodeData, state)
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
	if !fm.storedBlocks.IsStored("b") {
		t.Fatal("expected b to be marked stored after worker write succeeds")
	}
	if fm.storeWorker != nil && fm.storeWorker.IsInflight("b") {
		t.Fatal("expected b to be removed from in-flight store state after success")
	}
}

func TestPruneNoopWhenStoredSpanWithinKeepEvenWithStaleStoredHashes(t *testing.T) {
	fm := newTestFetchManager(t, 2)

	fm.blockTree.Insert(10, "a", "", 1, nil)
	fm.blockTree.Insert(11, "b", "a", 1, nil)
	fm.blockTree.Insert(12, "c", "b", 1, nil)

	fm.storedBlocks.MarkStored("a")
	fm.storedBlocks.MarkStored("b")
	fm.storedBlocks.MarkStored("c")
	fm.storedBlocks.MarkStored("ghost-1")
	fm.storedBlocks.MarkStored("ghost-2")
	fm.taskPool.AddTask("a")
	fm.taskPool.AddTask("b")
	fm.taskPool.AddTask("c")

	fm.pruneStoredBlocks(context.Background())

	for _, key := range []string{"a", "b", "c"} {
		if fm.blockTree.Get(key) == nil {
			t.Fatalf("expected node %s to remain when prune is noop", key)
		}
		if !fm.storedBlocks.IsStored(key) {
			t.Fatalf("expected stored node %s to remain stored when prune is noop", key)
		}
	}
	for _, key := range []string{"ghost-1", "ghost-2"} {
		ok := fm.storedBlocks.IsStored(key)
		if !ok {
			t.Fatalf("expected stale stored hash %s to be untouched by noop prune", key)
		}
	}
	if !fm.taskPool.HasTask("a") || !fm.taskPool.HasTask("b") || !fm.taskPool.HasTask("c") {
		t.Fatalf("expected tasks to remain untouched when prune is noop")
	}
}
