// Formal verification tests for StagingStore; see FormalVerification.md §4 I2 / §8 C2.
package store

import "testing"

func TestInvariantSetBlockBodyNoImplicitCreate(t *testing.T) {
	store := NewStagingStore()
	body := &EventBlockData{}

	store.SetPendingBody("0x10", body)

	if got := store.GetPendingBody("0x10"); got != nil {
		t.Fatalf("expected nil body for missing pending block key, got=%+v", got)
	}
}
