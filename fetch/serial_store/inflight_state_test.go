package serialstore

import (
	fetchstore "scanner_eth/fetch/store"
	"testing"
)

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
	stored := fetchstore.NewStoredBlockState()
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

func TestStoreBlockInflightStateNormalizesWhitespaceAndCase(t *testing.T) {
	s := NewInflightState()

	if !s.TryStart(" 0xAb ", nil) {
		t.Fatal("expected normalized hash to start inflight")
	}
	if !s.Has("0XaB") {
		t.Fatal("expected Has to match normalized hash")
	}
	if s.TryStart("0xab", nil) {
		t.Fatal("expected duplicate normalized hash to be rejected")
	}

	s.Finish(" 0XAB ")
	if s.Has("0xab") {
		t.Fatal("expected normalized hash removed after finish")
	}
}
