// StoredBlockState invariant I5 (FormalVerification.md §4): normalized non-empty keys in D.
package store

import (
	"scanner_eth/util"
	"testing"
)

// TestInvariantI5StoredBlockStateNormalizedClosure locks §4 I5: keys in D are normalized and non-empty.
func TestInvariantI5StoredBlockStateNormalizedClosure(t *testing.T) {
	s := NewStoredBlockState()
	s.MarkStored("")
	s.MarkStored(" 0xAaBb ")

	snapshot := s.Snapshot()

	if len(snapshot) != 1 {
		t.Fatalf("I5: expected exactly one key in D, got %d", len(snapshot))
	}
	for k := range snapshot {
		if k == "" {
			t.Fatal("I5: empty key must not appear in D")
		}
		if k != util.NormalizeHash(k) {
			t.Fatalf("I5: key %q must equal util.NormalizeHash(key)", k)
		}
		if k != "0xaabb" {
			t.Fatalf("I5: expected trimmed lowercase canonical hex, got %q", k)
		}
	}
}
