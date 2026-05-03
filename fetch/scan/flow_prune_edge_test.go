package scan

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
	"testing"
	"time"
)

func TestRunScanCycleWaitsForStoreBeforePrune(t *testing.T) {
	env := newTestFlowEnv(t, 2)
	parent := ""
	for h := uint64(1); h <= 6; h++ {
		hash := "h" + string(rune('0'+h))
		env.blockTree.Insert(h, hash, parent, 1)
		if h > 1 {
			env.stagingStore.SetPendingHeader(hash, makeTestHeader(h, hash, parent))
			env.stagingStore.SetPendingBody(hash, makeTestEventBlockData(h, hash, parent))
		}
		parent = hash
	}
	env.stored.MarkStored("h1")

	started := make(chan struct{}, 1)
	release := make(chan struct{})
	env.attachStoreWorker(func(_ context.Context, data *fetchstore.EventBlockData) error {
		if data != nil && data.StorageFullBlock != nil && data.StorageFullBlock.Block.Hash == "h2" {
			select {
			case started <- struct{}{}:
			default:
			}
			<-release
		}
		return nil
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		env.flow.RunScanCycle(context.Background())
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for store stage to start")
	}

	root := env.blockTree.Root()
	if root == nil || root.Height != 1 || root.Key != "h1" {
		t.Fatalf("expected prune to wait while store is in progress, got root=%+v", root)
	}
	select {
	case <-done:
		t.Fatal("scan cycle should remain blocked until store completes")
	default:
	}

	close(release)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for scan cycle to finish")
	}

	root = env.blockTree.Root()
	if root == nil || root.Height != 4 || root.Key != "h4" {
		t.Fatalf("expected prune after store completion to advance root to h4, got=%+v", root)
	}
}
