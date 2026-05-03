package scan

import (
	"context"
	"testing"
)

func TestStoreBranchEdgeBranches(t *testing.T) {
	env := newTestFlowEnv(t, 2)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if env.flow.RunScanStages(ctx) {
		t.Fatal("canceled context should stop scan stages")
	}
	env.flow.RunScanCycle(ctx)

	if ok, msg := env.flow.SyncStoreBranchTarget(context.Background(), "   "); ok || msg == "" {
		t.Fatal("expected invalid body branch target error")
	}

	env.flow.ProcessBranchNode(context.Background(), nil)
}
