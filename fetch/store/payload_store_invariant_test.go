package store

import "testing"

func TestInvariantSetBlockBodyNoImplicitCreate(t *testing.T) {
	store := NewPayloadStore[any, any]()
	body := struct{ Hash string }{Hash: "0x10"}

	store.SetBody("0x10", body)

	if got := store.GetBody("0x10"); got != nil {
		t.Fatalf("expected nil body for missing payload key, got=%+v", got)
	}
}
