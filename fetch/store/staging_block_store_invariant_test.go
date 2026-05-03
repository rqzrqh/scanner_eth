// Formal verification tests for StagingStore; see FormalVerification.md §4 I2 / §8 C2.
package store

import (
	fetcherpkg "scanner_eth/fetch/fetcher"
	"testing"
)

func TestInvariantSetBlockBodyNoImplicitCreate(t *testing.T) {
	store := NewStagingStore()
	body := &EventBlockData{}

	store.SetPendingBody("0x10", body)

	if got := store.GetPendingBody("0x10"); got != nil {
		t.Fatalf("expected nil body for missing pending block key, got=%+v", got)
	}
}

func TestStagingStoreSnapshotCountsPendingState(t *testing.T) {
	store := NewStagingStore()
	store.SetPendingHeader("0x1", &fetcherpkg.BlockHeaderJson{Hash: "0x1"})
	store.SetPendingHeader("0x2", &fetcherpkg.BlockHeaderJson{Hash: "0x2"})
	store.SetPendingBody("0x2", &EventBlockData{})

	snapshot := store.Snapshot()
	if snapshot.Blocks != 2 || snapshot.PendingHeaders != 2 || snapshot.PendingBodies != 1 || snapshot.CompleteBlocks != 1 {
		t.Fatalf("unexpected staging snapshot: %+v", snapshot)
	}
}
