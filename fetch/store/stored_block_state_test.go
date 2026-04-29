package store

import "testing"

func TestStoredBlockStateMarkAndIsStored(t *testing.T) {
	s := NewStoredBlockState()

	if s.IsStored("0xAa") {
		t.Fatal("expected hash not stored initially")
	}

	s.MarkStored("0xAa") // normalizeHash lower-cases and keeps canonical form.
	if !s.IsStored("0xaa") {
		t.Fatal("expected normalized hash to be stored")
	}
	if !s.IsStored("0xAa") {
		t.Fatal("expected lookup with mixed case to be normalized")
	}
}

func TestStoredBlockStateUnmarkStored(t *testing.T) {
	s := NewStoredBlockState()
	s.MarkStored("0x10")
	if !s.IsStored("0x10") {
		t.Fatal("expected hash stored before unmark")
	}

	s.UnmarkStored("0x10")
	if s.IsStored("0x10") {
		t.Fatal("expected hash removed after unmark")
	}
}

func TestStoredBlockStateReset(t *testing.T) {
	s := NewStoredBlockState()
	s.MarkStored("0x01")
	s.MarkStored("0x02")

	s.Reset()

	if s.IsStored("0x01") || s.IsStored("0x02") {
		t.Fatal("expected all hashes removed after reset")
	}
}

func TestStoredBlockStateEmptyHashNoop(t *testing.T) {
	s := NewStoredBlockState()

	s.MarkStored("")
	if s.IsStored("") {
		t.Fatal("empty hash should never be stored")
	}

	s.UnmarkStored("")
}

func TestStoredBlockStateWhitespaceHashIsStoredAsIs(t *testing.T) {
	s := NewStoredBlockState()

	s.MarkStored("   ")
	if !s.IsStored("   ") {
		t.Fatal("whitespace hash should be stored as-is under current normalizeHash behavior")
	}
}

func TestStoredBlockStateZeroValueUsable(t *testing.T) {
	var s StoredBlockState

	s.MarkStored("0x99")
	if !s.IsStored("0x99") {
		t.Fatal("expected zero-value state to be usable after mark")
	}
}
