package scan

import (
	"context"
	"errors"
	fetchstore "scanner_eth/fetch/store"
	"testing"
	"time"
)

func TestInvariantStoredOnlyAfterSuccessfulStore(t *testing.T) {
	t.Run("success path adds stored hash", func(t *testing.T) {
		env := newTestFlowEnv(t, 2)
		env.seedStoredLinearBranch("a", "b")
		env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error { return nil })

		env.flow.SubmitStoreBranchesLowToHigh(context.Background())
		if !env.stored.IsStored("b") {
			t.Fatal("expected b to be marked stored after successful DB write")
		}
	})

	t.Run("failure path does not add stored hash", func(t *testing.T) {
		env := newTestFlowEnv(t, 2)
		env.seedStoredLinearBranch("a", "b")
		env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error {
			return errors.New("db write failed")
		})

		env.flow.SubmitStoreBranchesLowToHigh(context.Background())
		if env.stored.IsStored("b") {
			t.Fatal("expected b to remain not stored when DB write fails")
		}
	})
}

func TestProcessBranchesDoesNotStoreWhenParentNotStored(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.insertLinearChain("a", "b", "c")
	env.stagePendingNodes("b", "c")

	writeOrder := make([]string, 0)
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		if blockData != nil && blockData.StorageFullBlock != nil {
			writeOrder = append(writeOrder, blockData.StorageFullBlock.Block.Hash)
		}
		return nil
	})

	env.flow.SubmitStoreBranchesLowToHigh(context.Background())
	if len(writeOrder) != 0 {
		t.Fatalf("expected no writes when parent is not stored, got=%v", writeOrder)
	}
	if env.stored.IsStored("b") || env.stored.IsStored("c") {
		t.Fatalf("child nodes should not be marked stored when parent is not stored")
	}
	if env.taskPool.HasTask("b") || env.taskPool.HasTask("c") {
		t.Fatalf("nodes with data but blocked parent should be skipped without auto-enqueueing tasks")
	}
}

func TestProcessBranchesMarksStoredOnlyOnSuccessfulWrite(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.seedStoredLinearBranch("a", "b")

	called := 0
	env.attachStoreWorker(func(context.Context, *fetchstore.EventBlockData) error {
		called++
		return errors.New("write failed")
	})

	env.flow.SubmitStoreBranchesLowToHigh(context.Background())
	if called != 1 {
		t.Fatalf("expected one write attempt, got=%d", called)
	}
	if env.stored.IsStored("b") {
		t.Fatalf("block should not be marked stored on write failure")
	}
}

func TestInvariantRunScanStagesWaitsForStoreCompletion(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	env.seedStoredLinearBranch("a", "b")

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	env.attachStoreWorker(func(_ context.Context, blockData *fetchstore.EventBlockData) error {
		if blockData != nil && blockData.StorageFullBlock != nil && blockData.StorageFullBlock.Block.Hash == "b" {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	})

	done := make(chan bool, 1)
	go func() {
		done <- env.flow.RunScanStages(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for synchronous store stage to start")
	}
	select {
	case <-done:
		t.Fatal("RunScanStages should not return before store stage completes")
	default:
	}

	close(release)

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("expected scan stages to succeed after store stage completes")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RunScanStages to finish")
	}
}
