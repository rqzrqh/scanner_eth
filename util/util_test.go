package util

import (
	"errors"
	"math/big"
	"testing"
	"time"
)

func TestToBlockNumArg(t *testing.T) {
	if got := ToBlockNumArg(nil); got != "latest" {
		t.Fatalf("expected latest, got %q", got)
	}

	if got := ToBlockNumArg(big.NewInt(-1)); got != "pending" {
		t.Fatalf("expected pending, got %q", got)
	}

	if got := ToBlockNumArg(big.NewInt(16)); got != "0x10" {
		t.Fatalf("expected 0x10, got %q", got)
	}
}

func TestHitNoMoreRetryErrors(t *testing.T) {
	if HitNoMoreRetryErrors(nil) {
		t.Fatalf("nil error should not match no-more-retry list")
	}

	if !HitNoMoreRetryErrors(errors.New("rpc failed: execution reverted: custom")) {
		t.Fatalf("expected execution reverted to be matched")
	}

	if HitNoMoreRetryErrors(errors.New("temporary network timeout")) {
		t.Fatalf("unexpected match for non-terminal error")
	}
}

func TestHandleErrorWithRetry_SucceedsAfterRetries(t *testing.T) {
	calls := 0
	err := HandleErrorWithRetry(func() error {
		calls++
		if calls < 3 {
			return errors.New("temporary")
		}
		return nil
	}, 3, 0)
	if err != nil {
		t.Fatalf("expected nil, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("expected 3 calls, got %d", calls)
	}
}

func TestHandleErrorWithRetry_ReturnsAfterLimit(t *testing.T) {
	calls := 0
	err := HandleErrorWithRetry(func() error {
		calls++
		return errors.New("always fail")
	}, 2, 0)
	if err == nil {
		t.Fatalf("expected non-nil error")
	}
	if calls != 3 {
		t.Fatalf("expected initial call + 2 retries, got %d", calls)
	}
}

func TestHandleErrorWithRetry_NilHandler(t *testing.T) {
	start := time.Now()
	err := HandleErrorWithRetry(nil, 3, time.Second)
	if err != nil {
		t.Fatalf("nil handler should return nil error, got %v", err)
	}
	if time.Since(start) > 100*time.Millisecond {
		t.Fatalf("nil handler should return immediately")
	}
}
