package fetch

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

func TestNewDbOperatorAndStoreBlockDataGuards(t *testing.T) {
	if op := NewDbOperator(nil, 6); op == nil {
		t.Fatal("NewDbOperator should return non-nil operator")
	}

	op := newFetchManagerDbOperator(nil, 6)
	if _, err := op.LoadBlockWindowFromDB(); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}
	if err := op.StoreBlockData(nil); err == nil || !strings.Contains(err.Error(), "db is nil") {
		t.Fatalf("expected db is nil error, got %v", err)
	}

	db := newTestDB(t)
	op2 := newFetchManagerDbOperator(db, 6)
	if err := op2.StoreBlockData(nil); err == nil || !strings.Contains(err.Error(), "block data is nil") {
		t.Fatalf("expected block data is nil error, got %v", err)
	}
}

func TestNodeStateGetChainInfoAndNodeManagerMethods(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil})
	if got := nm.NodeCount(); got != 2 {
		t.Fatalf("unexpected node count: %d", got)
	}

	nm.UpdateNodeChainInfo(0, 12, "0x12")
	if got := nm.nodes[0].GetChainInfo(); got != 12 {
		t.Fatalf("unexpected node chain info height: %d", got)
	}

	// Make all nodes unavailable for target height.
	nm.SetNodeNotReady(0)
	nm.SetNodeNotReady(1)
	if _, _, err := nm.GetBestNode(1); err == nil {
		t.Fatal("expected no valid node error")
	}

	nm.SetNodeReady(1)
	nm.UpdateNodeState(1, 5, true)
	nm.UpdateNodeChainInfo(1, 20, "0x20")
	id, _, err := nm.GetBestNode(15)
	if err != nil {
		t.Fatalf("expected best node, got error: %v", err)
	}
	if id != 1 {
		t.Fatalf("unexpected best node id: %d", id)
	}

	// Invalid ids should be no-op and not panic.
	nm.UpdateNodeChainInfo(-1, 1, "0x1")
	nm.UpdateNodeChainInfo(100, 1, "0x1")
	nm.UpdateNodeState(-1, 1, true)
	nm.UpdateNodeState(100, 1, true)
	nm.SetNodeNotReady(-1)
	nm.SetNodeNotReady(100)
	nm.SetNodeReady(-1)
	nm.SetNodeReady(100)
}

func TestTaskPoolResetTracked(t *testing.T) {
	tp := taskPool{
		handleTaskFn: func(task *syncTask, stopCh <-chan struct{}) bool { return true },
		tracked:      make(map[string]struct{}),
		queueHigh:    make(chan *syncTask, 8),
		queueNormal:  make(chan *syncTask, 8),
		stopCh:       make(chan struct{}),
		workerCount:  1,
		maxRetry:     1,
	}
	tp.addTracked("0x1")
	tp.addTracked("0x2")
	if tp.trackedCount() != 2 {
		t.Fatalf("expected 2 tracked tasks before reset, got=%d", tp.trackedCount())
	}
	tp.resetTracked()
	if tp.trackedCount() != 0 {
		t.Fatalf("expected tracked tasks reset to zero, got=%d", tp.trackedCount())
	}
}

func TestTaskPoolStopWaitsTaskGoroutine(t *testing.T) {
	taskDone := make(chan struct{})
	allowExit := make(chan struct{})

	tp := taskPool{
		handleTaskFn: func(task *syncTask, stopCh <-chan struct{}) bool {
			close(taskDone)
			select {
			case <-allowExit:
				return true
			case <-stopCh:
				return false
			}
		},
		tracked:     make(map[string]struct{}),
		queueHigh:   make(chan *syncTask, 8),
		queueNormal: make(chan *syncTask, 8),
		stopCh:      make(chan struct{}),
		workerCount: 1,
		maxRetry:    1,
	}

	tp.start()
	tp.enqueueTask("0xabc")

	select {
	case <-taskDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task goroutine did not start in time")
	}

	done := make(chan struct{})
	go func() {
		tp.stop()
		close(done)
	}()

	select {
	case <-done:
		// stop is allowed to return after stop signal is observed by task goroutine
	case <-time.After(500 * time.Millisecond):
		t.Fatal("stop did not finish after sending stop signal")
	}

	close(allowExit)
}

func TestFetchManagerOnLostLeaderAndHelpers(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	fm.scanEnabled.Store(true)
	if !fm.taskPool.tryStartHeaderHeightSync(10) {
		t.Fatal("failed to seed height syncing state")
	}
	if !fm.taskPool.tryStartHeaderHashSync("0x10") {
		t.Fatal("failed to seed hash syncing state")
	}

	cancelCalled := false
	fm.scanLoopCancel = func() { cancelCalled = true }

	if err := fm.onLostLeader(context.Background()); err != nil {
		t.Fatalf("onLostLeader returned error: %v", err)
	}
	if fm.isScanEnabled() {
		t.Fatal("scan should be disabled after onLostLeader")
	}
	if fm.scanLoopCancel != nil {
		t.Fatal("scan loop cancel should be cleared")
	}
	if !cancelCalled {
		t.Fatal("scan loop cancel callback should be invoked")
	}

	heightCount, hashCount := fm.taskPool.headerSyncCounts()
	if heightCount != 0 || hashCount != 0 {
		t.Fatalf("header syncing state should be reset, got heights=%d hashes=%d", heightCount, hashCount)
	}
}

func TestFetchManagerTriggerScanAndStopLoop(t *testing.T) {
	var nilFM *FetchManager
	if nilFM.isScanEnabled() {
		t.Fatal("nil fetch manager should report scan disabled")
	}
	nilFM.triggerScan()

	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })
	fm.scanTriggerCh = make(chan struct{}, 1)

	fm.scanEnabled.Store(false)
	fm.triggerScan()
	if len(fm.scanTriggerCh) != 0 {
		t.Fatal("triggerScan should not enqueue when scan disabled")
	}

	fm.scanEnabled.Store(true)
	fm.triggerScan()
	fm.triggerScan()
	if len(fm.scanTriggerCh) != 1 {
		t.Fatalf("trigger channel should cap at 1, got=%d", len(fm.scanTriggerCh))
	}

	called := false
	fm.scanLoopCancel = func() { called = true }
	fm.stopScanLoop()
	if !called {
		t.Fatal("stopScanLoop should invoke cancel")
	}
	if fm.scanLoopCancel != nil {
		t.Fatal("stopScanLoop should clear cancel func")
	}

	// No panic on repeated stop.
	fm.stopScanLoop()
}