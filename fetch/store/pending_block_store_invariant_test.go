// Formal verification tests for PendingBlockStore; see FormalVerification.md §4 I2 / §8 C2.
package store

import "testing"

func TestInvariantSetBlockBodyNoImplicitCreate(t *testing.T) {
	store := NewPendingBlockStore()
	body := &EventBlockData{}

	store.SetBody("0x10", body)

	if got := store.GetBody("0x10"); got != nil {
		t.Fatalf("expected nil body for missing pending block key, got=%+v", got)
	}
}
