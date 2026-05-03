package task

import "testing"

func TestDispatchSyncTaskRoutesKinds(t *testing.T) {
	bodyCalled := false
	hashCalled := false
	heightCalled := false

	if !DispatchSyncTask(
		&SyncTask{Kind: SyncTaskKindBody, Hash: "0xbody"},
		nil,
		func(hash string, stopCh <-chan struct{}) bool {
			bodyCalled = hash == "0xbody" && stopCh == nil
			return true
		},
		func(hash string) bool {
			hashCalled = true
			return false
		},
		func(height uint64) bool {
			heightCalled = true
			return false
		},
	) {
		t.Fatal("expected body task to dispatch successfully")
	}
	if !bodyCalled || hashCalled || heightCalled {
		t.Fatalf("unexpected dispatch result body=%v hash=%v height=%v", bodyCalled, hashCalled, heightCalled)
	}

	bodyCalled, hashCalled, heightCalled = false, false, false
	if !DispatchSyncTask(
		&SyncTask{Kind: SyncTaskKindHeaderHash, Hash: "0xhash"},
		nil,
		func(hash string, stopCh <-chan struct{}) bool {
			bodyCalled = true
			return false
		},
		func(hash string) bool {
			hashCalled = hash == "0xhash"
			return true
		},
		func(height uint64) bool {
			heightCalled = true
			return false
		},
	) {
		t.Fatal("expected header-hash task to dispatch successfully")
	}
	if bodyCalled || !hashCalled || heightCalled {
		t.Fatalf("unexpected dispatch result body=%v hash=%v height=%v", bodyCalled, hashCalled, heightCalled)
	}

	bodyCalled, hashCalled, heightCalled = false, false, false
	if !DispatchSyncTask(
		&SyncTask{Kind: SyncTaskKindHeaderHeight, Height: 12},
		nil,
		func(hash string, stopCh <-chan struct{}) bool {
			bodyCalled = true
			return false
		},
		func(hash string) bool {
			hashCalled = true
			return false
		},
		func(height uint64) bool {
			heightCalled = height == 12
			return true
		},
	) {
		t.Fatal("expected header-height task to dispatch successfully")
	}
	if bodyCalled || hashCalled || !heightCalled {
		t.Fatalf("unexpected dispatch result body=%v hash=%v height=%v", bodyCalled, hashCalled, heightCalled)
	}
}

func TestDispatchSyncTaskTreatsLegacyHashTaskAsBody(t *testing.T) {
	called := false
	if !DispatchSyncTask(
		&SyncTask{Hash: "0xlegacy"},
		nil,
		func(hash string, stopCh <-chan struct{}) bool {
			called = hash == "0xlegacy"
			return true
		},
		nil,
		nil,
	) {
		t.Fatal("expected legacy hash task to dispatch as body task")
	}
	if !called {
		t.Fatal("expected body handler to be called")
	}
}

func TestDispatchSyncTaskNilTaskIsSuccess(t *testing.T) {
	if !DispatchSyncTask(nil, nil, nil, nil, nil) {
		t.Fatal("nil task should be treated as success")
	}
}
