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
	inserted := bt.internalInsert(first.height, first.key, first.parentKey, first.weight, nil)
	if len(inserted) == 0 {
		t.Fatalf("expected first insert to succeed")
	}

	for _, in := range inputs[1:] {
		if bt.Get(in.parentKey) == nil {
			t.Fatalf("parent %s not found for child %s", in.parentKey, in.key)
		}
		bt.internalInsert(in.height, in.key, in.parentKey, in.weight, nil)
	}

	return bt
}

func applyInsertList(bt *BlockTree, inputs []insertInput) {
	for _, in := range inputs {
		bt.Insert(in.height, in.key, in.parentKey, in.weight, nil)
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

	inserted := bt.Insert(10, "A", "P", 7, nil)
	if len(inserted) != 1 {
		t.Fatalf("expected 1 inserted record, got=%d", len(inserted))
	}

	nv := inserted[0]
	if nv.Key != "A" || nv.ParentKey != "P" || nv.Height != 10 || nv.Weight != 7 {
		t.Fatalf("unexpected inserted node value: %+v", nv)
	}
	if nv.Irreversible != nil {
		t.Fatalf("unexpected irreversible field should be nil: %+v", nv.Irreversible)
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

	dup := bt.Insert(10, "A", "P", 7, nil)
	if dup != nil {
		t.Fatal("duplicate insert should return nil")
	}
	if bt.Len() != 1 {
		t.Fatalf("tree len changed after duplicate insert: got=%d", bt.Len())
	}
}

func TestInsertStoresHeaderAndOrphanPreservesHeader(t *testing.T) {
	type mockHeader struct {
		id string
	}

	bt := NewBlockTree(2)

	hA := &mockHeader{id: "A"}
	inserted := bt.Insert(1, "A", "", 1, hA)
	if len(inserted) != 1 {
		t.Fatalf("expected 1 inserted record, got=%d", len(inserted))
	}
	if inserted[0].Header != hA {
		t.Fatalf("root linked node header mismatch: got=%v want=%v", inserted[0].Header, hA)
	}
	if bt.root == nil || bt.root.Header != hA {
		t.Fatalf("root header mismatch: got=%v want=%v", bt.root, hA)
	}

	hC := &mockHeader{id: "C"}
	if got := bt.Insert(3, "C", "B", 1, hC); got != nil {
		t.Fatal("orphan insert should return nil")
	}

	resolved := bt.Insert(2, "B", "A", 1, nil)
	if len(resolved) != 2 {
		t.Fatalf("expected B and C to be inserted, got=%d", len(resolved))
	}

	c := mustGet(t, bt, "C")
	if c.Header != hC {
		t.Fatalf("orphan-resolved node header mismatch: got=%v want=%v", c.Header, hC)
	}
}

func TestSetDataStoresPointerPayload(t *testing.T) {
	type payload struct {
		value string
	}

	bt := NewBlockTree(2)
	inserted := bt.Insert(1, "A", "", 1, nil)
	if len(inserted) != 1 {
		t.Fatalf("expected 1 inserted record, got=%d", len(inserted))
	}
	if inserted[0].Data != nil {
		t.Fatalf("expected Data to be nil before set, got=%v", inserted[0].Data)
	}

	p := &payload{value: "payload-A"}
	if !bt.SetData("A", p) {
		t.Fatal("expected SetData to succeed for linked node A")
	}

	node := mustGet(t, bt, "A")
	if node.Data != p {
		t.Fatalf("node Data mismatch: got=%v want=%v", node.Data, p)
	}
	if inserted[0].Data != p {
		t.Fatalf("insert return value should reflect updated Data: got=%v want=%v", inserted[0].Data, p)
	}
}

func TestSetDataMissingNodeReturnsFalse(t *testing.T) {
	bt := NewBlockTree(2)

	if bt.SetData("missing", &struct{}{}) {
		t.Fatal("SetData should return false for missing node")
	}
}

func TestInsertOrphanAndCascadeResolve(t *testing.T) {
	bt := NewBlockTree(2)

	if got := bt.Insert(1, "A", "", 1, nil); len(got) != 1 {
		t.Fatalf("expected root insert record, got=%d", len(got))
	}

	if got := bt.Insert(3, "C", "B", 30, nil); got != nil {
		t.Fatal("orphan insert should return nil")
	}
	if _, ok := bt.orphanParentToChild["B"]["C"]; !ok {
		t.Fatal("orphan C under parent B should exist")
	}
	if _, ok := bt.orphanKeySet["C"]; !ok {
		t.Fatal("orphanKeySet should record orphan C")
	}

	got := bt.Insert(2, "B", "A", 20, nil)
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
	if b.Irreversible != nil {
		t.Fatalf("B should not have irreversible ancestor yet: %+v", b.Irreversible)
	}
	c := mustGet(t, bt, "C")
	if c.Irreversible == nil || c.Irreversible.Key != "A" || c.Irreversible.Height != 1 {
		t.Fatalf("C irreversible ancestor mismatch: %+v", c.Irreversible)
	}
}

func TestInsertDuplicateOrphanKeyIgnored(t *testing.T) {
	bt := NewBlockTree(2)

	bt.Insert(1, "A", "", 1, nil)
	if got := bt.Insert(3, "C", "B", 1, nil); got != nil {
		t.Fatal("first orphan insert should return nil")
	}
	if got := bt.Insert(3, "C", "X", 2, nil); got != nil {
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

	bt.Insert(10, "A", "", 1, nil)
	bt.Insert(11, "B", "A", 1, nil)
	bt.Insert(12, "C", "B", 1, nil)

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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)
	bt.Insert(2, "B", "X", 1, nil)
	bt.Insert(6, "F", "E", 1, nil)
	bt.Insert(5, "E", "Y", 1, nil)
	bt.Insert(4, "D", "A", 1, nil)

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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(4, "D", "X", 1, nil)

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
	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)
	bt.Insert(4, "D", "C", 1, nil)
	bt.Insert(2, "Y", "A", 1, nil)
	bt.Insert(3, "Z", "Y", 1, nil)

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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)
	bt.Insert(4, "D", "C", 1, nil)
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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "Y", "A", 1, nil)
	bt.Insert(3, "Z", "Y", 1, nil)
	bt.Insert(2, "B", "A", 2, nil)
	bt.Insert(3, "C", "B", 2, nil)
	bt.Insert(4, "D", "C", 2, nil)
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

	bt.Insert(1, "A", "", 1, nil)
	bt.internalInsert(2, "B", "A", 1, nil)
	bt.internalInsert(2, "C", "A", 1, nil)
	bt.internalInsert(3, "D", "B", 1, nil)
	bt.internalInsert(3, "E", "C", 1, nil)

	// Two headers at same height 4, heavier one should win as best leaf.
	bt.internalInsert(4, "F", "D", 1, nil)
	bt.internalInsert(4, "G", "E", 9, nil)

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

func TestRootReturnsCopy(t *testing.T) {
	bt := NewBlockTree(2)
	bt.Insert(1, "A", "", 1, nil)

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

	bt.Insert(1, "A", "", 1, nil)
	bt.Insert(2, "B", "A", 1, nil)
	bt.Insert(3, "C", "B", 1, nil)

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
		name             string
		irreversible     int
		inputs           []insertInput
		parentKey        Key
		wantHeight       uint64
		wantKey          Key
		wantNil          bool
	}{
		{
			name:         "irreversible disabled",
			irreversible: 0,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
			},
			parentKey: "A",
			wantNil:   true,
		},
		{
			name:         "chain too short",
			irreversible: 3,
			inputs: []insertInput{
				{height: 1, key: "A", parentKey: "", weight: 1},
				{height: 2, key: "B", parentKey: "A", weight: 1},
			},
			parentKey: "B",
			wantNil:   true,
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
				if got != nil {
					t.Fatalf("computeIrreversible should return nil, got: %+v", got)
				}
			} else {
				if got == nil {
					t.Fatalf("computeIrreversible should not return nil")
				}
				if got.Height != tt.wantHeight || got.Key != tt.wantKey {
					t.Fatalf("computeIrreversible mismatch: got=(%d,%s), want=(%d,%s)", got.Height, got.Key, tt.wantHeight, tt.wantKey)
				}
			}
		})
	}
}

func TestPruneMainChainRootProgressionTableDriven(t *testing.T) {
	tests := []struct {
		name             string
		irreversible     int
		inputs           []insertInput
		wantRoot         Key
		wantMissing      []Key
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