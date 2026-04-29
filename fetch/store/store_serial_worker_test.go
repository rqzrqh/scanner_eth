package store

import "testing"

func TestStoreBlockInflightStateTryStartDedupes(t *testing.T) {
	s := NewInflightState()

	if !s.TryStart("0x01", nil) {
		t.Fatal("expected first in-flight mark to succeed")
	}
	if !s.Has("0x01") {
		t.Fatal("expected hash to be marked in-flight")
	}
	if s.TryStart("0x01", nil) {
		t.Fatal("expected duplicate in-flight mark to be rejected")
	}

	s.Finish("0x01")
	if s.Has("0x01") {
		t.Fatal("expected hash removed from in-flight state")
	}
	if !s.TryStart("0x01", nil) {
		t.Fatal("expected in-flight mark to succeed again after finish")
	}
}

func TestStoreBlockInflightStateRejectsStoredHash(t *testing.T) {
	s := NewInflightState()
	stored := NewStoredBlockState()
	stored.MarkStored("0x02")

	if s.TryStart("0x02", &stored) {
		t.Fatal("expected stored hash to reject in-flight mark")
	}
	if s.Has("0x02") {
		t.Fatal("stored hash should not enter in-flight state")
	}
	if s.Count() != 0 {
		t.Fatalf("unexpected in-flight count: got=%d want=0", s.Count())
	}
}
