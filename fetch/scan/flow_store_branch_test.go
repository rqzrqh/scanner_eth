package scan

import (
	"strings"
	"testing"
)

func TestCountActionableAndStoredLinkedNodes(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(10, "0xaa", "", 1)
	env.blockTree.Insert(11, "0xbb", "0xaa", 1)
	env.stagingStore.SetPendingHeader("0xaa", makeTestHeader(10, "0xaa", ""))
	env.stagingStore.SetPendingBody("0xaa", makeTestEventBlockData(10, "0xaa", ""))

	if got := env.flow.CountStoreBranchNodes(); got != 2 {
		t.Fatalf("expected 2 body branch nodes (stored root skipped, full suffix submitted), got=%d", got)
	}

	env.stored.MarkStored("0xaa")
	if got := env.flow.CountStoredLinkedTreeNodes(); got != 1 {
		t.Fatalf("expected 1 stored linked node, got=%d", got)
	}
}

func TestGetBodyBranchTargetsBuildsLowToHighBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.blockTree.Insert(2, "d", "a", 1)
	env.stored.MarkStored("a")
	env.stagingStore.SetPendingHeader("b", makeTestHeader(2, "b", "a"))
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingHeader("d", makeTestHeader(2, "d", "a"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))
	env.stagingStore.SetPendingBody("d", makeTestEventBlockData(2, "d", "a"))

	targets := env.flow.GetStoreBranchTargets()
	if got := strings.Join(targets, bodyTargetBranchSep); got != "b,c;d" {
		t.Fatalf("expected low-to-high full body branch targets, got=%q", got)
	}
}
