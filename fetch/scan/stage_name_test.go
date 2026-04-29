package scan

import "testing"

func TestScanStageName(t *testing.T) {
	if got := scanStageName(scanStageHeaderByHeightDone); got != "header_by_height" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageHeaderByHashDone); got != "header_by_hash" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStageBodyDone); got != "body" {
		t.Fatalf("unexpected stage name: %s", got)
	}
	if got := scanStageName(scanStage(99)); got != "unknown" {
		t.Fatalf("unexpected stage name for unknown stage: %s", got)
	}
}
