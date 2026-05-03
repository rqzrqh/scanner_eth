package scan

import "testing"

func TestScanStageName(t *testing.T) {
	if got := scanStageName(scanStageExpandTree); got != "expand_tree" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageFillTree); got != "fill_tree" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageSyncBody); got != "sync_body" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageStoreBranches); got != "store_branches" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStagePrune); got != "prune" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStage(99)); got != "unknown" {
		t.Fatalf("unexpected stage name for unknown stage: %s", got)
	}
}
