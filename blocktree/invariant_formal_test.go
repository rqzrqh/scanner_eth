package blocktree

import "testing"

func TestInvariantPruneReturnsRemovedOrphans(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(4, "D", "C", 1)

	// o2 should be removed (height <= new root), o3 should remain.
	bt.Insert(2, "o2", "missing-2", 1)
	bt.Insert(3, "o3", "missing-3", 1)

	pruned := bt.Prune(1)
	prunedKeys := nodeValueKeys(pruned)

	if _, ok := prunedKeys["o2"]; !ok {
		t.Fatal("expected removed orphan o2 to be included in prune result")
	}
	if _, ok := prunedKeys["o3"]; ok {
		t.Fatal("did not expect orphan o3 to be included in prune result")
	}
}

func TestInvariantOrphansAboveRootAfterPrune(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(4, "D", "C", 1)
	bt.Insert(5, "E", "D", 1)

	// Will be removed after prune because new root height is 3.
	bt.Insert(1, "o1", "missing-1", 1)
	bt.Insert(3, "o3", "missing-3", 1)
	// Should remain.
	bt.Insert(4, "o4", "missing-4", 1)

	_ = bt.Prune(1)

	root := bt.Root()
	if root == nil {
		t.Fatal("expected non-nil root after prune")
	}

	for _, siblings := range bt.orphanParentToChild {
		for _, orphanNode := range siblings {
			if orphanNode == nil {
				continue
			}
			if orphanNode.Height <= root.Height {
				t.Fatalf("found orphan height<=root after prune: orphan=%+v root=%+v", orphanNode, root)
			}
		}
	}
}
