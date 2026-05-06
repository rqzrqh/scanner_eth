package scan

import (
	"context"
	"errors"
	fetchstore "scanner_eth/fetch/store"
	"testing"
)

func TestPlanP2DBIntermittentFailureThenRecovery(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.seedStoredLinearBranch("a", "b")

	attempts := 0
	env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error {
		attempts++
		if attempts <= 2 {
			return errors.New("db temporary unavailable")
		}
		return nil
	})

	if err := env.flow.SubmitStoreBranches(context.Background(), env.flow.BuildStoreBranches()); err != nil {
		t.Fatalf("submit store branches failed: %v", err)
	}
	if env.stored.IsStored("b") {
		t.Fatal("b should not be marked stored after first failed write")
	}
	if env.stagingStore.GetPendingHeader("b") == nil || env.stagingStore.GetPendingBody("b") == nil {
		t.Fatal("pending header/body for b must remain after failed DB write for retry")
	}

	if err := env.flow.SubmitStoreBranches(context.Background(), env.flow.BuildStoreBranches()); err != nil {
		t.Fatalf("submit store branches failed: %v", err)
	}
	if env.stored.IsStored("b") {
		t.Fatal("b should not be marked stored after second failed write")
	}
	if env.stagingStore.GetPendingHeader("b") == nil || env.stagingStore.GetPendingBody("b") == nil {
		t.Fatal("pending header/body for b must remain after second failed DB write")
	}

	if err := env.flow.SubmitStoreBranches(context.Background(), env.flow.BuildStoreBranches()); err != nil {
		t.Fatalf("submit store branches failed: %v", err)
	}
	if !env.stored.IsStored("b") {
		t.Fatal("b should be marked stored after recovery write succeeds")
	}
	if attempts != 3 {
		t.Fatalf("unexpected store attempts: got=%d want=3", attempts)
	}
}
