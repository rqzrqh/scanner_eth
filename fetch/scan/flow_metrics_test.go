package scan

import (
	"context"
	"testing"
)

func TestFlowMetricsPayloadRecordsScanStages(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.insertLinearChain("a")
	env.markStoredHashes("a")
	env.flow.RunScanCycle(context.Background())

	payload := env.flow.MetricsPayload()
	stages, ok := payload["stages"].(map[string]map[string]any)
	if !ok {
		t.Fatalf("unexpected metrics payload: %+v", payload)
	}
	if _, ok := stages["expand_tree"]; !ok {
		t.Fatalf("expected expand_tree metrics, got %+v", stages)
	}
	if _, ok := stages["sync_body"]; !ok {
		t.Fatalf("expected sync_body metrics, got %+v", stages)
	}
	if _, ok := stages["prune"]; !ok {
		t.Fatalf("expected prune metrics, got %+v", stages)
	}
}
