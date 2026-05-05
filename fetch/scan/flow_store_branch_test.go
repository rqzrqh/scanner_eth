package scan

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
	"strings"
	"testing"
)

func TestCountActionableAndStoredLinkedNodes(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	env.blockTree.Insert(10, "0xaa", "", 1)
	env.blockTree.Insert(11, "0xbb", "0xaa", 1)
	env.stagingStore.SetPendingHeader("0xaa", makeTestHeader(10, "0xaa", ""))
	env.stagingStore.SetPendingBody("0xaa", makeTestEventBlockData(10, "0xaa", ""))

	if got := env.flow.CountStoreBranchNodes(); got != 1 {
		t.Fatalf("expected only the contiguous storable prefix to be submitted, got=%d", got)
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
	if got := strings.Join(targets, bodyTargetBranchSep); got != "d" {
		t.Fatalf("expected only storable low-to-high branch targets, got=%q", got)
	}

	bodyTargets := env.flow.GetBodyBranchTargets()
	if got := strings.Join(bodyTargets, bodyTargetBranchSep); got != "b,c;d" {
		t.Fatalf("expected body sync targets to retain missing-body suffixes, got=%q", got)
	}
}

func TestSubmitStoreBranchesFiltersMissingBodiesBeforeSerialStore(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.blockTree.Insert(1, "a", "", 1)
	env.blockTree.Insert(2, "b", "a", 1)
	env.blockTree.Insert(3, "c", "b", 1)
	env.stored.MarkStored("a")
	env.stagingStore.SetPendingHeader("b", makeTestHeader(2, "b", "a"))
	env.stagingStore.SetPendingHeader("c", makeTestHeader(3, "c", "b"))
	env.stagingStore.SetPendingBody("c", makeTestEventBlockData(3, "c", "b"))

	writes := 0
	env.attachStoreWorker(func(_ context.Context, _ *fetchstore.EventBlockData) error {
		writes++
		return nil
	})

	if err := env.flow.SubmitStoreBranches(context.Background(), env.flow.collectStoreBranchesForBodySync()); err != nil {
		t.Fatalf("submit branches failed: %v", err)
	}
	if writes != 0 {
		t.Fatalf("missing parent body should prevent descendant store writes, got %d", writes)
	}
	metrics := env.store.MetricsPayload()
	reasons, ok := metrics["skip_reasons"].(map[string]uint64)
	if !ok {
		t.Fatalf("missing skip reasons payload: %+v", metrics)
	}
	if reasons["missing_body"] != 0 {
		t.Fatalf("scan should filter missing bodies before serial_store, got=%+v", reasons)
	}
}
