package blocktree

import "testing"

func TestInvariantPruneReturnsRemovedOrphans(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)
	bt.Insert(4, "D", "C", 1, nil)

	// o2 should be removed (height <= new root), o3 should remain.
	bt.Insert(2, "o2", "missing-2", 1, nil)
	bt.Insert(3, "o3", "missing-3", 1, nil)

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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)
	bt.Insert(4, "D", "C", 1, nil)
	bt.Insert(5, "E", "D", 1, nil)

	// Will be removed after prune because new root height is 3.
	bt.Insert(1, "o1", "missing-1", 1, nil)
	bt.Insert(3, "o3", "missing-3", 1, nil)
	// Should remain.
	bt.Insert(4, "o4", "missing-4", 1, nil)

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
