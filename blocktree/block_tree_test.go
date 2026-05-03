package blocktree

import "testing"

type insertInput struct {
	height    uint64
	key       Key
	parentKey Key
	weight    uint64
}

func buildTreeNoPrune(t *testing.T, irreversibleCount int, inputs []insertInput) *BlockTree {
	t.Helper()

	bt := NewBlockTree(irreversibleCount)
	if len(inputs) == 0 {
		return bt
	}

	first := inputs[0]
	bt.root = &Node{Height: first.height, Key: first.key, ParentKey: first.parentKey, Weight: first.weight}
	inserted := bt.internalInsert(first.height, first.key, first.parentKey, first.weight)
	if len(inserted) == 0 {
		t.Fatalf("expected first insert to succeed")
	}

	for _, in := range inputs[1:] {
		if bt.Get(in.parentKey) == nil {
			t.Fatalf("parent %s not found for child %s", in.parentKey, in.key)
		}
		bt.internalInsert(in.height, in.key, in.parentKey, in.weight)
	}

	return bt
}

func applyInsertList(bt *BlockTree, inputs []insertInput) {
	for _, in := range inputs {
		bt.Insert(in.height, in.key, in.parentKey, in.weight)
	}
}

func mustGet(t *testing.T, bt *BlockTree, key Key) *LinkedNode {
	t.Helper()
	nv := bt.Get(key)
	if nv == nil {
		t.Fatalf("expected key %s to exist", key)
	}
	return nv
}

func assertMissing(t *testing.T, bt *BlockTree, key Key) {
	t.Helper()
	if bt.Get(key) != nil {
		t.Fatalf("expected key %s to be missing", key)
	}
}

func keySet(keys []Key) map[Key]struct{} {
	result := make(map[Key]struct{}, len(keys))
	for _, k := range keys {
		result[k] = struct{}{}
	}
	return result
}

func nodeValueKeys(values []*LinkedNode) map[Key]struct{} {
	result := make(map[Key]struct{}, len(values))
	for _, v := range values {
		if v != nil {
			result[v.Key] = struct{}{}
		}
	}
	return result
}

func branchKeyPath(branch Branch) []Key {
	keys := make([]Key, 0, len(branch.Nodes))
	for _, nv := range branch.Nodes {
		if nv != nil {
			keys = append(keys, nv.Key)
		}
	}
	return keys
}

func TestBlockTreeSnapshotReportsRuntimeShape(t *testing.T) {
	bt := NewBlockTree(2)
	applyInsertList(bt, []insertInput{
		{height: 1, key: "a", parentKey: "", weight: 1},
		{height: 2, key: "b", parentKey: "a", weight: 1},
		{height: 2, key: "c", parentKey: "a", weight: 2},
		{height: 4, key: "orphan", parentKey: "missing", weight: 1},
	})

	snapshot := bt.Snapshot()
	if !snapshot.HasRange || snapshot.StartHeight != 1 || snapshot.EndHeight != 2 {
		t.Fatalf("unexpected height range snapshot: %+v", snapshot)
	}
	if snapshot.LinkedCount != 3 || snapshot.LeafCount != 2 || snapshot.BranchCount != 2 {
		t.Fatalf("unexpected linked/leaf/branch counts: %+v", snapshot)
	}
	if snapshot.OrphanParentCount != 1 || snapshot.OrphanCount != 1 {
		t.Fatalf("unexpected orphan counts: %+v", snapshot)
	}
	if snapshot.Root == nil || snapshot.Root.Key != "a" {
		t.Fatalf("unexpected root snapshot: %+v", snapshot.Root)
	}
}

func assertKeyPathEqual(t *testing.T, got []Key, want []Key) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("path length mismatch: got=%v want=%v", got, want)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("path mismatch: got=%v want=%v", got, want)
		}
	}
}

func TestNewBlockTreeInit(t *testing.T) {
	bt := NewBlockTree(12)

	if bt == nil {
		t.Fatal("NewBlockTree returned nil")
	}
	if bt.irreversibleCount != 12 {
		t.Fatalf("irreversibleCount mismatch: got=%d", bt.irreversibleCount)
	}
	if bt.root != nil {
		t.Fatal("root should be nil on a new tree")
	}
	if bt.keyValue == nil || bt.parentToChild == nil || bt.headers == nil || bt.orphanParentToChild == nil || bt.orphanKeySet == nil {
		t.Fatal("internal maps should be initialized")
	}
}

func TestInsertRootAndDuplicate(t *testing.T) {
	bt := NewBlockTree(2)

	inserted := bt.Insert(10, "A", "P", 7)
	if len(inserted) != 1 {
		t.Fatalf("expected 1 inserted record, got=%d", len(inserted))
	}

	nv := inserted[0]
	if nv.Key != "A" || nv.ParentKey != "P" || nv.Height != 10 || nv.Weight != 7 {
		t.Fatalf("unexpected inserted node value: %+v", nv)
	}
	if nv.Irreversible.Key != "A" || nv.Irreversible.Height != 10 {
		t.Fatalf("unexpected irreversible field: %+v", nv.Irreversible)
	}

	if bt.root == nil || bt.root.Key != "A" {
		t.Fatalf("root mismatch: %+v", bt.root)
	}
	if _, ok := bt.headers["A"]; !ok {
		t.Fatal("A should be a leaf header")
	}
	if _, ok := bt.parentToChild["P"]["A"]; !ok {
		t.Fatal("A should be linked under parent P")
	}
	if bt.Len() != 1 {
		t.Fatalf("tree len mismatch: got=%d", bt.Len())
	}

	dup := bt.Insert(10, "A", "P", 7)
	if dup != nil {
		t.Fatal("duplicate insert should return nil")
	}
	if bt.Len() != 1 {
		t.Fatalf("tree len changed after duplicate insert: got=%d", bt.Len())
	}
}

func TestInsertRejectsHeightAtOrBelowRootWhenRootExists(t *testing.T) {
	bt := NewBlockTree(2)

	if got := bt.Insert(10, "A", "", 1); len(got) != 1 {
		t.Fatalf("expected root insert record, got=%d", len(got))
	}

	if got := bt.Insert(10, "B", "A", 1); got != nil {
		t.Fatal("insert at root height should be rejected")
	}
	if got := bt.Insert(9, "C", "A", 1); got != nil {
		t.Fatal("insert below root height should be rejected")
	}

	if bt.Get("B") != nil || bt.Get("C") != nil {
		t.Fatal("rejected nodes should not be linked")
	}
	if len(bt.orphanKeySet) != 0 {
		t.Fatal("rejected nodes should not be buffered as orphans")
	}
	if bt.Len() != 1 {
		t.Fatalf("tree len mismatch after rejected inserts: got=%d", bt.Len())
	}
}

func TestInsertOrphanResolves(t *testing.T) {
	bt := NewBlockTree(2)

	inserted := bt.Insert(1, "A", "", 1)
	if len(inserted) != 1 {
		t.Fatalf("expected 1 inserted record, got=%d", len(inserted))
	}

	if got := bt.Insert(3, "C", "B", 1); got != nil {
		t.Fatal("orphan insert should return nil")
	}

	resolved := bt.Insert(2, "B", "A", 1)
	if len(resolved) != 2 {
		t.Fatalf("expected B and C to be inserted, got=%d", len(resolved))
	}
	mustGet(t, bt, "C")
}

func TestInsertOrphanAndCascadeResolve(t *testing.T) {
	bt := NewBlockTree(2)

	if got := bt.Insert(1, "A", "", 1); len(got) != 1 {
		t.Fatalf("expected root insert record, got=%d", len(got))
	}

	if got := bt.Insert(3, "C", "B", 30); got != nil {
		t.Fatal("orphan insert should return nil")
	}
	if _, ok := bt.orphanParentToChild["B"]["C"]; !ok {
		t.Fatal("orphan C under parent B should exist")
	}
	if _, ok := bt.orphanKeySet["C"]; !ok {
		t.Fatal("orphanKeySet should record orphan C")
	}

	got := bt.Insert(2, "B", "A", 20)
	if len(got) != 2 {
		t.Fatalf("expected B and C to be inserted, got=%d", len(got))
	}
	if got[0].Key != "B" || got[1].Key != "C" {
		t.Fatalf("unexpected insertion order: %s, %s", got[0].Key, got[1].Key)
	}

	if _, ok := bt.orphanParentToChild["B"]; ok {
		t.Fatal("orphan list for B should be consumed")
	}
	if _, ok := bt.orphanKeySet["C"]; ok {
		t.Fatal("orphanKeySet should remove C after adoption")
	}
	if bt.Len() != 3 {
		t.Fatalf("tree len mismatch after cascade: got=%d", bt.Len())
	}

	b := mustGet(t, bt, "B")
	if b.Irreversible.Key != "A" || b.Irreversible.Height != 1 {
		t.Fatalf("B irreversible ancestor mismatch: %+v", b.Irreversible)
	}
	c := mustGet(t, bt, "C")
	if c.Irreversible.Key != "A" || c.Irreversible.Height != 1 {
		t.Fatalf("C irreversible ancestor mismatch: %+v", c.Irreversible)
	}
}

func TestInsertDuplicateOrphanKeyIgnored(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	if got := bt.Insert(3, "C", "B", 1); got != nil {
		t.Fatal("first orphan insert should return nil")
	}
	if got := bt.Insert(3, "C", "X", 2); got != nil {
		t.Fatal("duplicate orphan key insert should return nil")
	}

	if len(bt.orphanKeySet) != 1 {
		t.Fatalf("orphanKeySet size mismatch: got=%d", len(bt.orphanKeySet))
	}
	if _, ok := bt.orphanParentToChild["B"]["C"]; !ok {
		t.Fatal("original orphan C under parent B should remain")
	}
	if _, ok := bt.orphanParentToChild["X"]["C"]; ok {
		t.Fatal("duplicate orphan C should not be inserted under parent X")
	}
}

func TestHeightRange(t *testing.T) {
	bt := NewBlockTree(2)

	if start, end, ok := bt.HeightRange(); ok || start != 0 || end != 0 {
		t.Fatalf("empty tree height range mismatch: start=%d end=%d ok=%v", start, end, ok)
	}

	bt.Insert(10, "A", "", 1)
	bt.Insert(11, "B", "A", 1)
	bt.Insert(12, "C", "B", 1)

	start, end, ok := bt.HeightRange()
	if !ok {
		t.Fatal("height range should exist")
	}
	if start != 10 || end != 12 {
		t.Fatalf("height range mismatch: start=%d end=%d", start, end)
	}
}

func TestUnlinkedNodesReturnsOrphanParentsNotInOrphanKeySet(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(2, "B", "X", 1)
	bt.Insert(6, "F", "E", 1)
	bt.Insert(5, "E", "Y", 1)
	bt.Insert(4, "D", "A", 1)

	// orphanParentToChild: B->{C}, X->{B}, E->{F}, Y->{E}
	// B,E are in orphanKeySet; X,Y are missing parents not buffered as orphans
	got := keySet(bt.UnlinkedNodes())
	if len(got) != 2 {
		t.Fatalf("unlinked parent count mismatch: got=%d", len(got))
	}
	if _, ok := got["X"]; !ok {
		t.Fatal("X should be returned as orphan parent (B waits for X, X not in orphanKeySet)")
	}
	if _, ok := got["Y"]; !ok {
		t.Fatal("Y should be returned as orphan parent (E waits for Y, Y not in orphanKeySet)")
	}
	if _, ok := got["B"]; ok {
		t.Fatal("B should not be returned: B is in orphanKeySet")
	}
	if _, ok := got["E"]; ok {
		t.Fatal("E should not be returned: E is in orphanKeySet")
	}
}

func TestLinkedNodesReturnsKeyValueNodes(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(4, "D", "X", 1)

	got := nodeValueKeys(bt.LinkedNodes())
	if len(got) != 2 {
		t.Fatalf("linked node count mismatch: got=%d", len(got))
	}
	if _, ok := got["A"]; !ok {
		t.Fatal("A should be a linked node")
	}
	if _, ok := got["B"]; !ok {
		t.Fatal("B should be a linked node")
	}
	if _, ok := got["D"]; ok {
		t.Fatal("D should not be a linked node because it is orphaned")
	}
}

func TestBranchesEmptyTree(t *testing.T) {
	bt := NewBlockTree(2)

	branches := bt.Branches()
	if branches != nil {
		t.Fatalf("empty tree branches should be nil, got=%v", branches)
	}
}

func TestBranchesFromHeadersSortedByHeight(t *testing.T) {
	bt := NewBlockTree(10)

	// Build two linked branches with different header heights:
	// A-B-C-D (header height 4), and A-Y-Z (header height 3).
	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(4, "D", "C", 1)
	bt.Insert(2, "Y", "A", 1)
	bt.Insert(3, "Z", "Y", 1)

	branches := bt.Branches()
	if len(branches) != 2 {
		t.Fatalf("branch count mismatch: got=%d", len(branches))
	}

	if branches[0].Header == nil || branches[0].Header.Key != "D" {
		t.Fatalf("first branch header mismatch: %+v", branches[0].Header)
	}
	if branches[1].Header == nil || branches[1].Header.Key != "Z" {
		t.Fatalf("second branch header mismatch: %+v", branches[1].Header)
	}

	assertKeyPathEqual(t, branchKeyPath(branches[0]), []Key{"D", "C", "B", "A"})
	assertKeyPathEqual(t, branchKeyPath(branches[1]), []Key{"Z", "Y", "A"})
}

func TestPruneAdvancesRootOnMainChain(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(4, "D", "C", 1)
	bt.Prune(1)

	if bt.root == nil || bt.root.Key != "B" {
		t.Fatalf("root should advance to B after prune, got=%+v", bt.root)
	}
	assertMissing(t, bt, "A")
	mustGet(t, bt, "B")
	mustGet(t, bt, "C")
	mustGet(t, bt, "D")

	if bt.Len() != 3 {
		t.Fatalf("tree len mismatch after prune: got=%d", bt.Len())
	}
}

func TestPruneRemovesSideBranchesFromDeletedRange(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "Y", "A", 1)
	bt.Insert(3, "Z", "Y", 1)
	bt.Insert(2, "B", "A", 2)
	bt.Insert(3, "C", "B", 2)
	bt.Insert(4, "D", "C", 2)
	bt.Prune(1)

	if bt.root == nil || bt.root.Key != "B" {
		t.Fatalf("root should be B after prune, got=%+v", bt.root)
	}

	// Side branch rooted at Y should be removed together with pruned ancestors.
	assertMissing(t, bt, "A")
	assertMissing(t, bt, "Y")
	assertMissing(t, bt, "Z")

	mustGet(t, bt, "B")
	mustGet(t, bt, "C")
	mustGet(t, bt, "D")
}

func TestPruneTieBreakByWeightOnSameHeightLeaf(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.internalInsert(2, "B", "A", 1)
	bt.internalInsert(2, "C", "A", 1)
	bt.internalInsert(3, "D", "B", 1)
	bt.internalInsert(3, "E", "C", 1)

	// Two headers at same height 4, heavier one should win as best leaf.
	bt.internalInsert(4, "F", "D", 1)
	bt.internalInsert(4, "G", "E", 9)

	bt.Prune(1)

	if bt.root == nil || bt.root.Key != "C" {
		t.Fatalf("expected heavier branch to win and root to become C, got=%+v", bt.root)
	}

	assertMissing(t, bt, "A")
	assertMissing(t, bt, "B")
	assertMissing(t, bt, "D")
	assertMissing(t, bt, "F")

	mustGet(t, bt, "C")
	mustGet(t, bt, "E")
	mustGet(t, bt, "G")
}

func TestPruneRemovesOrphansAtOrBelowNewRootAndReturnsThem(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(4, "D", "C", 1)

	// Buffered orphans before prune: O2 should be removed, O3 should remain.
	if got := bt.Insert(2, "O2", "MISSING-2", 1); got != nil {
		t.Fatal("orphan insert should return nil")
	}
	if got := bt.Insert(3, "O3", "MISSING-3", 1); got != nil {
		t.Fatal("orphan insert should return nil")
	}

	pruned := bt.Prune(1)
	if bt.root == nil || bt.root.Key != "B" || bt.root.Height != 2 {
		t.Fatalf("root should advance to B(height=2), got=%+v", bt.root)
	}

	prunedKeys := nodeValueKeys(pruned)
	if _, ok := prunedKeys["A"]; !ok {
		t.Fatal("linked node A should be in prune result")
	}
	if _, ok := prunedKeys["O2"]; !ok {
		t.Fatal("orphan O2(height<=root) should be in prune result")
	}
	if _, ok := prunedKeys["O3"]; ok {
		t.Fatal("orphan O3(height>root) should not be pruned")
	}

	if _, ok := bt.orphanKeySet["O2"]; ok {
		t.Fatal("orphan O2 should be removed from orphanKeySet")
	}
	if _, ok := bt.orphanKeySet["O3"]; !ok {
		t.Fatal("orphan O3 should remain in orphanKeySet")
	}
	if _, ok := bt.orphanParentToChild["MISSING-2"]; ok {
		t.Fatal("empty orphan bucket MISSING-2 should be removed")
	}
	if _, ok := bt.orphanParentToChild["MISSING-3"]; !ok {
		t.Fatal("orphan bucket MISSING-3 should remain")
	}
}

func TestPruneKeepsOnlyUniqueRootSubtreeAndPrunesOlderForks(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "a", "", 1)
	bt.Insert(2, "b", "a", 1)
	bt.Insert(3, "c", "b", 2)
	bt.Insert(4, "d", "c", 2)
	bt.Insert(5, "e", "d", 2)

	// Fork below the future root: should be removed together with the old ancestry.
	bt.Insert(3, "x", "b", 1)
	bt.Insert(4, "y", "x", 1)

	// Fork at the future root: should remain because it still descends from the unique root.
	bt.Insert(4, "z", "c", 1)

	if got := bt.Insert(3, "o3", "missing-3", 1); got != nil {
		t.Fatal("orphan insert should return nil")
	}
	if got := bt.Insert(4, "o4", "missing-4", 1); got != nil {
		t.Fatal("orphan insert should return nil")
	}

	pruned := bt.Prune(1)
	if bt.root == nil || bt.root.Key != "c" || bt.root.Height != 3 {
		t.Fatalf("root should advance to c(height=3), got=%+v", bt.root)
	}

	for _, key := range []Key{"a", "b", "x", "y"} {
		assertMissing(t, bt, key)
	}
	for _, key := range []Key{"c", "d", "e", "z"} {
		mustGet(t, bt, key)
	}

	prunedKeys := nodeValueKeys(pruned)
	for _, key := range []Key{"a", "b", "x", "y", "o3"} {
		if _, ok := prunedKeys[key]; !ok {
			t.Fatalf("expected %s in prune result", key)
		}
	}
	if _, ok := prunedKeys["z"]; ok {
		t.Fatal("fork rooted at the new root should not be pruned")
	}
	if _, ok := prunedKeys["o4"]; ok {
		t.Fatal("orphan above new root should not be pruned")
	}

	if _, ok := bt.orphanKeySet["o3"]; ok {
		t.Fatal("orphan o3 should be removed after prune")
	}
	if _, ok := bt.orphanKeySet["o4"]; !ok {
		t.Fatal("orphan o4 should remain after prune")
	}
}

func TestRootReturnsCopy(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)

	r := bt.Root()
	if r == nil {
		t.Fatal("Root returned nil")
	}
	r.Key = "MUTATED"

	if bt.root.Key != "A" {
		t.Fatalf("internal root should not be mutated by Root copy, got=%s", bt.root.Key)
	}
}

func TestPruneNoopWhenNoIrreversibleNode(t *testing.T) {
	bt := NewBlockTree(5)

	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)

	bt.Prune(1)

	if bt.root == nil || bt.root.Key != "A" {
		t.Fatalf("root should stay A when irreversible key is not available, got=%+v", bt.root)
	}
	if bt.Len() != 3 {
		t.Fatalf("tree should remain unchanged, got len=%d", bt.Len())
	}
}

func TestComputeIrreversibleTableDriven(t *testing.T) {
	tests := []struct {
		name         string
		irreversible int
		inputs       []insertInput
		parentKey    Key
		wantHeight   uint64
		wantKey      Key
		wantNil      bool
	}{
		{
			name:         "irreversible disabled",
			irreversible: 0,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
			},
			parentKey:  "A",
			wantHeight: 1,
			wantKey:    "A",
			wantNil:    false,
		},
		{
			name:         "chain too short",
			irreversible: 3,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
			},
			parentKey:  "B",
			wantHeight: 1,
			wantKey:    "A",
			wantNil:    false,
		},
		{
			name:         "exact depth available",
			irreversible: 2,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
			},
			parentKey:  "B",
			wantHeight: 1,
			wantKey:    "A",
			wantNil:    false,
		},
		{
			name:         "deep chain returns proper ancestor",
			irreversible: 3,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
				{height: 3, key: "C", parentKey: "B", weight: 1},
				{height: 4, key: "D", parentKey: "C", weight: 1},
			},
			parentKey:  "D",
			wantHeight: 2,
			wantKey:    "B",
			wantNil:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := buildTreeNoPrune(t, tt.irreversible, tt.inputs)
			got := bt.computeIrreversible(tt.parentKey)
			if tt.wantNil {
				if got.Key != "" || got.Height != 0 {
					t.Fatalf("computeIrreversible should return nil, got: %+v", got)
				}
			} else {
				if got.Height != tt.wantHeight || got.Key != tt.wantKey {
					t.Fatalf("computeIrreversible mismatch: got=(%d,%s), want=(%d,%s)", got.Height, got.Key, tt.wantHeight, tt.wantKey)
				}
			}
		})
	}
}

func TestPruneMainChainRootProgressionTableDriven(t *testing.T) {
	tests := []struct {
		name         string
		irreversible int
		inputs       []insertInput
		wantRoot     Key
		wantMissing  []Key
	}{
		{
			name:         "irreversible 1 keeps latest parent as root",
			irreversible: 1,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
				{height: 3, key: "C", parentKey: "B", weight: 1},
			},
			wantRoot:    "B",
			wantMissing: []Key{"A"},
		},
		{
			name:         "irreversible 2 on length 4 chain",
			irreversible: 2,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
				{height: 3, key: "C", parentKey: "B", weight: 1},
				{height: 4, key: "D", parentKey: "C", weight: 1},
			},
			wantRoot:    "B",
			wantMissing: []Key{"A"},
		},
		{
			name:         "irreversible 3 on length 6 chain",
			irreversible: 3,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
				{height: 3, key: "C", parentKey: "B", weight: 1},
				{height: 4, key: "D", parentKey: "C", weight: 1},
				{height: 5, key: "E", parentKey: "D", weight: 1},
				{height: 6, key: "F", parentKey: "E", weight: 1},
			},
			wantRoot:    "C",
			wantMissing: []Key{"A", "B"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bt := NewBlockTree(tt.irreversible)
			applyInsertList(bt, tt.inputs)
			bt.Prune(1)

			if bt.root == nil {
				t.Fatal("root should not be nil")
			}
			if bt.root.Key != tt.wantRoot {
				t.Fatalf("root mismatch: got=%s want=%s", bt.root.Key, tt.wantRoot)
			}

			for _, k := range tt.wantMissing {
				assertMissing(t, bt, k)
			}
		})
	}
}

func TestRootNil(t *testing.T) {
	bt := NewBlockTree(2)
	if got := bt.Root(); got != nil {
		t.Fatalf("expected nil root on empty tree, got=%+v", got)
	}
}

func TestBlockTreeInsert(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)
	mustGet(t, bt, "A")
}

func TestWalkRemovesSubtree(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(3, "C", "B", 1)
	bt.Insert(2, "D", "A", 1)

	walk(bt, "B")

	mustGet(t, bt, "A")
	mustGet(t, bt, "D")
	assertMissing(t, bt, "B")
	assertMissing(t, bt, "C")
	if _, ok := bt.headers["D"]; !ok {
		t.Fatal("D should remain a header")
	}
}

func TestUnlinkedNodesSkipsEmptyBuckets(t *testing.T) {
	bt := NewBlockTree(2)
	bt.orphanParentToChild["P"] = map[Key]*Node{}
	bt.orphanParentToChild["Q"] = map[Key]*Node{"C": {Key: "C", ParentKey: "Q"}}

	got := keySet(bt.UnlinkedNodes())
	if len(got) != 1 {
		t.Fatalf("expected one unlinked parent, got=%d", len(got))
	}
	if _, ok := got["Q"]; !ok {
		t.Fatal("Q should be returned")
	}
}

func TestBranchesTieBreakAndMissingParentStop(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(2, "C", "A", 1)

	// Force both headers to same height/weight so key ordering branch is exercised.
	bt.keyValue["B"].Weight = 5
	bt.keyValue["C"].Weight = 5

	branches := bt.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header.Key != "B" || branches[1].Header.Key != "C" {
		t.Fatalf("expected key tie-break order B,C got %s,%s", branches[0].Header.Key, branches[1].Header.Key)
	}

	// Add a detached header whose parent is missing to hit the parent-not-found break.
	bt.keyValue["X"] = &LinkedNode{Node: Node{Height: 9, Key: "X", ParentKey: "MISSING", Weight: 9}}
	bt.headers["X"] = struct{}{}
	branches = bt.Branches()
	if len(branches) != 3 {
		t.Fatalf("expected 3 branches after adding X, got=%d", len(branches))
	}
	if branches[0].Header.Key != "X" {
		t.Fatalf("X should be first branch header by height, got=%s", branches[0].Header.Key)
	}
	if len(branches[0].Nodes) != 1 || branches[0].Nodes[0].Key != "X" {
		t.Fatalf("X branch should stop at missing parent, got=%v", branchKeyPath(branches[0]))
	}
}

func TestComputeIrreversibleFallbackBranches(t *testing.T) {
	t.Run("parent empty returns zero when nothing available", func(t *testing.T) {
		bt := NewBlockTree(3)
		got := bt.computeIrreversible("")
		if got.Key != "" || got.Height != 0 {
			t.Fatalf("expected zero irreversible, got %+v", got)
		}
	})

	t.Run("missing parent with no fallback returns zero", func(t *testing.T) {
		bt := NewBlockTree(3)
		got := bt.computeIrreversible("UNKNOWN")
		if got.Key != "" || got.Height != 0 {
			t.Fatalf("expected zero irreversible for missing parent, got %+v", got)
		}
	})

	t.Run("missing parent returns root fallback", func(t *testing.T) {
		bt := NewBlockTree(3)
		bt.Insert(1, "A", "", 1)
		got := bt.computeIrreversible("UNKNOWN")
		if got.Key != "A" || got.Height != 1 {
			t.Fatalf("expected root fallback, got %+v", got)
		}
	})

	t.Run("missing parent in chain walk returns earliest reached", func(t *testing.T) {
		bt := NewBlockTree(4)
		bt.keyValue["P"] = &LinkedNode{Node: Node{Height: 10, Key: "P", ParentKey: "MISSING", Weight: 1}}
		got := bt.computeIrreversible("P")
		if got.Key != "P" || got.Height != 10 {
			t.Fatalf("expected fallback to first reachable parent, got %+v", got)
		}
	})
}

func TestPruneGuardBranches(t *testing.T) {
	t.Run("count zero returns nil", func(t *testing.T) {
		bt := NewBlockTree(2)
		bt.Insert(1, "A", "", 1)
		if got := bt.Prune(0); got != nil {
			t.Fatalf("expected nil prune result, got=%v", got)
		}
	})

	t.Run("best header nil returns nil", func(t *testing.T) {
		bt := NewBlockTree(2)
		bt.root = &Node{Height: 1, Key: "A"}
		bt.headers["A"] = struct{}{}
		bt.keyValue["A"] = nil
		if got := bt.Prune(1); got != nil {
			t.Fatalf("expected nil prune when header node is nil, got=%v", got)
		}
	})

	t.Run("min new root greater than max returns nil", func(t *testing.T) {
		bt := NewBlockTree(2)
		bt.Insert(1, "A", "", 1)
		bt.Insert(2, "B", "A", 1)
		bt.Insert(3, "C", "B", 1)
		if got := bt.Prune(3); got != nil {
			t.Fatalf("expected nil prune when requested count too large, got=%v", got)
		}
	})

	t.Run("main chain walk stops on missing parent", func(t *testing.T) {
		bt := NewBlockTree(2)
		// Corrupt-but-valid test state: header H exists but its parent is missing.
		bt.root = &Node{Height: 1, Key: "R"}
		bt.keyValue["H"] = &LinkedNode{Node: Node{Height: 5, Key: "H", ParentKey: "MISSING", Weight: 10}}
		bt.headers["H"] = struct{}{}
		// Ensure prune is allowed and reaches the mainChainPath parent-missing branch.
		_ = bt.Prune(1)
		if bt.Root() == nil {
			t.Fatal("root should remain available after prune attempt")
		}
	})
}

func TestBranchesKeyTieBreakComparator(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "C", "A", 1)
	bt.Insert(2, "B", "A", 1)

	// Force identical height and weight so key comparison branch is required.
	bt.keyValue["B"].Weight = 9
	bt.keyValue["C"].Weight = 9

	branches := bt.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header.Key != "B" || branches[1].Header.Key != "C" {
		t.Fatalf("expected key-order tie break B,C got %s,%s", branches[0].Header.Key, branches[1].Header.Key)
	}
}

func TestBranchesWeightTieBreakComparator(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1)
	bt.Insert(2, "B", "A", 1)
	bt.Insert(2, "C", "A", 1)

	// Same height, different weight: heavier header should come first.
	bt.keyValue["B"].Weight = 3
	bt.keyValue["C"].Weight = 9

	branches := bt.Branches()
	if len(branches) != 2 {
		t.Fatalf("expected 2 branches, got=%d", len(branches))
	}
	if branches[0].Header.Key != "C" || branches[1].Header.Key != "B" {
		t.Fatalf("expected weight-order C,B got %s,%s", branches[0].Header.Key, branches[1].Header.Key)
	}
}
