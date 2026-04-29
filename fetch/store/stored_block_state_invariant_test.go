package store

import "testing"

// TestInvariantI5StoredBlockStateNormalizedClosure locks §4 I5: keys in D are normalized and non-empty.
func TestInvariantI5StoredBlockStateNormalizedClosure(t *testing.T) {
	s := NewStoredBlockState()
	s.MarkStored("")
	s.MarkStored("0xAaBb")

	snapshot := s.Snapshot()

	if len(snapshot) != 1 {
		t.Fatalf("I5: expected exactly one key in D, got %d", len(snapshot))
	}
	for k := range snapshot {
		if k == "" {
			t.Fatal("I5: empty key must not appear in D")
		}
		if k != normalizeHash(k) {
			t.Fatalf("I5: key %q must equal normalizeHash(key)", k)
		}
		if k != "0xaabb" {
			t.Fatalf("I5: expected lowercase canonical hex, got %q", k)
		}
	}
}
