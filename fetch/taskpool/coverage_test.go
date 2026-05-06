package task

import (
	"context"
	"errors"
	"testing"
	"time"
)

func newCoveragePool(handle func(task *SyncTask, stopCh <-chan struct{}) bool) Pool {
	return Pool{
		HandleTaskFn:     handle,
		Tracked:          make(map[string]struct{}),
		trackedSince:     make(map[string]time.Time),
		QueueHigh:        make(chan *SyncTask, 4),
		QueueNormal:      make(chan *SyncTask, 4),
		StopCh:           make(chan struct{}),
		WorkerCount:      1,
		MaxRetry:         1,
		StatsLogInterval: 0,
	}
}

func TestCoverageDispatchFailures(t *testing.T) {
	if DispatchSyncTask(&SyncTask{Kind: SyncTaskKindBody, Hash: "b"}, nil, nil, nil, nil) {
		t.Fatal("body task without handler should fail")
	}
	if DispatchSyncTask(&SyncTask{Kind: SyncTaskKindHeaderHash, Hash: "h"}, nil, nil, nil, nil) {
		t.Fatal("header hash task without handler should fail")
	}
	if DispatchSyncTask(&SyncTask{Kind: 99, Height: 1}, nil, nil, nil, nil) {
		t.Fatal("header height fallback without handler should fail")
	}
	if DispatchSyncTask(&SyncTask{Hash: "legacy"}, nil, func(string, <-chan struct{}) bool { return false }, nil, nil) {
		t.Fatal("legacy body handler false should fail")
	}
}

func TestCoverageOptionsTrackedAndHeaderLifecycle(t *testing.T) {
	options := NormalizeTaskPoolOptions(TaskPoolOptions{
		WorkerCount:      -1,
		HighQueueSize:    -1,
		NormalQueueSize:  -1,
		MaxRetry:         -1,
		StatsLogInterval: -1,
	}, 0)
	if options.WorkerCount != 1 || options.HighQueueSize != 1024 || options.NormalQueueSize != 2048 || options.MaxRetry != 2 || options.StatsLogInterval != 0 {
		t.Fatalf("unexpected normalized options: %+v", options)
	}

	tp := Pool{}
	if tp.HasTracked("") || tp.HasTrackedKey("") {
		t.Fatal("empty tracked keys should be absent")
	}
	tp.AddTracked("")
	tp.AddTrackedKey("")
	tp.DelTracked("")
	tp.DelTrackedKey("")
	tp.AddTracked("body")
	tp.AddTrackedKey("key")
	if !tp.HasTask("body") || !tp.HasTaskKey("key") {
		t.Fatalf("expected tracked body/key, tracked=%+v", tp.Tracked)
	}
	tp.DelTask("body")
	tp.DelTaskKey("key")
	if tp.HasTask("body") || tp.HasTaskKey("key") {
		t.Fatalf("expected deleted tracked entries, tracked=%+v", tp.Tracked)
	}
	lazyKey := Pool{}
	lazyKey.AddTrackedKey("lazy")
	if !lazyKey.HasTrackedKey("lazy") {
		t.Fatal("AddTrackedKey should lazily initialize maps")
	}

	if headerHeightTaskKey(7) != "header_height:7" {
		t.Fatal("unexpected header height task key")
	}
	if headerHashTaskKey(" ") != "" {
		t.Fatal("blank header hash key should be empty")
	}
	if !tp.TryStartHeaderHeightSync(7) || tp.TryStartHeaderHeightSync(7) {
		t.Fatal("height sync reservation should be one-shot")
	}
	if !tp.IsHeaderHeightSyncing(7) {
		t.Fatal("height should be syncing")
	}
	tp.FinishHeaderHeightSync(7)
	if tp.IsHeaderHeightSyncing(7) {
		t.Fatal("height sync should finish")
	}
	if tp.TryStartHeaderHashSync(" ") {
		t.Fatal("blank hash sync reservation should fail")
	}
	if !tp.TryStartHeaderHashSync(" 0xAb ") || tp.TryStartHeaderHashSync("0xab") {
		t.Fatal("hash sync reservation should normalize and dedupe")
	}
	if !tp.IsHeaderHashSyncing("0xAB") {
		t.Fatal("hash should be syncing")
	}
	tp.FinishHeaderHashSync(" ")
	if !tp.IsHeaderHashSyncing("0xab") {
		t.Fatal("blank finish should not delete hash sync")
	}
	tp.FinishHeaderHashSync("0xab")
	if tp.IsHeaderHashSyncing("0xab") {
		t.Fatal("hash sync should finish")
	}
	if tp.IsHeaderHashSyncing(" ") {
		t.Fatal("blank hash should not be syncing")
	}
	lazyReserve := Pool{}
	if !lazyReserve.TryStartHeaderHeightSync(1) || !lazyReserve.TryStartHeaderHashSync("0x02") {
		t.Fatalf("reservations should lazily initialize maps, tracked=%+v", lazyReserve.Tracked)
	}
	tp.AddTask("body")
	tp.TryStartHeaderHeightSync(8)
	tp.TryStartHeaderHashSync("0x09")
	body, heights, hashes := tp.trackedCountsByKind()
	if body != 1 || heights != 1 || hashes != 1 {
		t.Fatalf("unexpected tracked counts: body=%d heights=%d hashes=%d", body, heights, hashes)
	}
	hh, hs := tp.HeaderSyncCounts()
	if hh != 1 || hs != 1 {
		t.Fatalf("unexpected header counts: heights=%d hashes=%d", hh, hs)
	}
}

func TestCoverageEnqueueAndPushTaskBoundaries(t *testing.T) {
	tp := newCoveragePool(func(task *SyncTask, stopCh <-chan struct{}) bool {
		<-stopCh
		return false
	})
	if tp.EnqueueSyncTask(nil) {
		t.Fatal("nil sync task should not enqueue")
	}
	if tp.EnqueueSyncTask(&SyncTask{}) {
		t.Fatal("empty sync task should not enqueue")
	}
	if tp.EnqueueHeaderHashTask(" ") {
		t.Fatal("blank header hash task should not enqueue")
	}
	if !tp.EnqueueHeaderHeightTask(3) {
		t.Fatal("header height task should enqueue")
	}
	if tp.EnqueueHeaderHeightTask(3) {
		t.Fatal("duplicate header height task should not enqueue")
	}
	if !tp.EnqueueSyncTask(&SyncTask{Kind: SyncTaskKindHeaderHash, Hash: "0xAb"}) {
		t.Fatal("header hash sync task should infer key")
	}
	if !tp.EnqueueSyncTask(&SyncTask{Kind: SyncTaskKindHeaderHeight, Height: 5}) {
		t.Fatal("header height sync task should infer key")
	}
	if !tp.EnqueueSyncTask(&SyncTask{Hash: "legacy-body"}) {
		t.Fatal("legacy body sync task should infer body kind and key")
	}
	if tp.EnqueueSyncTask(&SyncTask{Kind: SyncTaskKindBody}) {
		t.Fatal("body sync task without hash should fail")
	}
	tp.EnqueueTaskWithPriority(" 0xB ", 99)
	if !tp.HasTask("0xb") {
		t.Fatal("body task should normalize and enqueue")
	}
	tp.EnqueueTaskWithPriority(" ", TaskPriorityHigh)
	tp.Stop()

	drop := Pool{
		Tracked:     make(map[string]struct{}),
		QueueHigh:   make(chan *SyncTask, 1),
		QueueNormal: make(chan *SyncTask, 1),
		StopCh:      make(chan struct{}),
	}
	drop.QueueHigh <- &SyncTask{Key: "filled-high", Kind: SyncTaskKindHeaderHeight}
	if drop.PushTask(nil, true) {
		t.Fatal("nil push should fail")
	}
	if drop.PushTask(&SyncTask{Kind: SyncTaskKindBody}, true) {
		t.Fatal("body push without hash should fail")
	}
	if drop.PushTask(&SyncTask{Key: "body-no-hash", Kind: SyncTaskKindBody}, true) {
		t.Fatal("body push with key but without hash should fail")
	}
	if !drop.PushTask(&SyncTask{Kind: SyncTaskKindHeaderHeight, Height: 4}, true) {
		t.Fatal("header height should fall back to normal queue when high full")
	}
	<-drop.QueueNormal
	if !drop.PushTask(&SyncTask{Kind: SyncTaskKindHeaderHash, Hash: " 0xCD "}, true) {
		t.Fatal("header hash should infer normalized key")
	}
	<-drop.QueueNormal
	<-drop.QueueHigh
	if drop.PushTask(&SyncTask{Kind: SyncTaskKindBody}, true) {
		t.Fatal("body push without hash should fail")
	}
	drop.QueueHigh <- &SyncTask{Key: "filled-high-2", Kind: SyncTaskKindHeaderHeight}
	drop.QueueNormal <- &SyncTask{Key: "filled-normal", Kind: SyncTaskKindBody}
	if drop.PushTask(&SyncTask{Key: "drop", Kind: SyncTaskKindHeaderHash, Priority: TaskPriorityHigh, CreatedAt: time.Now()}, true) {
		t.Fatal("full queues should drop task")
	}
	if drop.HasTaskKey("drop") {
		t.Fatal("dropped task should be cleared when requested")
	}
	stats := drop.Stats()
	if stats.Dropped == 0 || stats.DroppedHeaderHash == 0 {
		t.Fatalf("expected dropped header hash stats, got=%+v", stats)
	}
}

func TestCoverageExecutionMetricsAndIdle(t *testing.T) {
	tp := newCoveragePool(func(task *SyncTask, stopCh <-chan struct{}) bool {
		return task.Hash == "ok"
	})
	tp.taskWG.Add(1)
	tp.executeTask(&SyncTask{Key: "ok", Hash: "ok", Kind: SyncTaskKindBody, CreatedAt: time.Now()})
	if tp.Stats().SucceededBody != 1 {
		t.Fatalf("expected body success, stats=%+v", tp.Stats())
	}
	tp.taskWG.Add(1)
	tp.executeTask(&SyncTask{Key: "retry", Hash: "retry", Kind: SyncTaskKindHeaderHash, CreatedAt: time.Now()})
	if tp.Stats().RetriedHeaderHash != 1 {
		t.Fatalf("expected header hash retry, stats=%+v", tp.Stats())
	}
	<-tp.QueueNormal
	tp.taskWG.Add(1)
	tp.executeTask(&SyncTask{Key: "fail", Hash: "fail", Kind: SyncTaskKindHeaderHeight, Retry: 1, CreatedAt: time.Now()})
	if tp.Stats().FailedHeaderH != 1 {
		t.Fatalf("expected header height failure, stats=%+v", tp.Stats())
	}
	if tp.handleTask(nil) != true || tp.handleTask(&SyncTask{}) != true {
		t.Fatal("nil/empty task should be treated as handled")
	}
	noHandler := newCoveragePool(nil)
	if noHandler.handleTask(&SyncTask{Key: "x"}) {
		t.Fatal("missing handler should fail")
	}
	stopping := newCoveragePool(nil)
	close(stopping.StopCh)
	stopping.taskWG.Add(1)
	stopping.executeTask(&SyncTask{Key: "stopping", Hash: "stopping", CreatedAt: time.Now()})
	if taskTotalMicros(nil) != 0 || taskTotalMicros(&SyncTask{}) != 0 {
		t.Fatal("empty task total time should be zero")
	}
	logTaskLifecycle("nil", nil, 0, 0)
	for kind, want := range map[int]string{
		SyncTaskKindBody:         "body",
		SyncTaskKindHeaderHeight: "header_height",
		SyncTaskKindHeaderHash:   "header_hash",
		42:                       "42",
	} {
		if got := syncTaskKindName(kind); got != want {
			t.Fatalf("unexpected kind name for %d: %s", kind, got)
		}
	}

	payload := tp.MetricsPayload()
	if payload["totals"] == nil || payload["by_kind"] == nil || payload["queues"] == nil || payload["tracked"] == nil || payload["config"] == nil {
		t.Fatalf("metrics payload missing sections: %+v", payload)
	}
	if (*Pool)(nil).IsIdle() != true {
		t.Fatal("nil pool should be idle")
	}
	if (*Pool)(nil).IsStopping() {
		t.Fatal("nil pool should not be stopping")
	}
	if err := (*Pool)(nil).WaitIdle(context.Background(), 0); err != nil {
		t.Fatalf("nil pool wait idle should succeed: %v", err)
	}
	if !tp.IsIdle() {
		t.Fatal("pool should be idle after direct executions")
	}
	if err := tp.WaitIdle(context.Background(), -1); err != nil {
		t.Fatalf("idle pool wait should succeed: %v", err)
	}
	tp.AddTask("busy")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()
	if err := tp.WaitIdle(ctx, 0); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("busy pool wait should time out, got=%v", err)
	}
	tp.DelTask("busy")
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := tp.WaitIdle(ctx, time.Millisecond); err != nil {
		t.Fatalf("quiet idle wait should succeed: %v", err)
	}
	if err := tp.WaitIdle(nil, 0); err != nil {
		t.Fatalf("nil context should default and succeed: %v", err)
	}
}

func TestCoverageStartStopReporterAndCancellation(t *testing.T) {
	defaults := Pool{HandleTaskFn: func(task *SyncTask, stopCh <-chan struct{}) bool { return true }}
	defaults.Start()
	defaults.Stop()

	tp := NewTaskPoolWithStop(TaskPoolOptions{WorkerCount: 1, HighQueueSize: 2, NormalQueueSize: 2, MaxRetry: 1, StatsLogInterval: time.Millisecond}, 0,
		func(task *SyncTask, stopCh <-chan struct{}) bool {
			<-stopCh
			return false
		})
	if tp.IsStopping() {
		t.Fatal("new pool should not be stopping")
	}
	tp.Start()
	tp.Start()
	if !tp.EnqueueSyncTask(&SyncTask{Key: "cancel", Hash: "cancel", Kind: SyncTaskKindBody}) {
		t.Fatal("expected cancel task to enqueue")
	}
	time.Sleep(2 * time.Millisecond)
	tp.Stop()
	if !tp.IsStopping() {
		t.Fatal("stopped pool should report stopping")
	}
	tp.Stop()
}

func TestCoverageWorkerNormalQueueAndTrackedAges(t *testing.T) {
	tp := newCoveragePool(func(task *SyncTask, stopCh <-chan struct{}) bool { return true })
	tp.QueueNormal <- &SyncTask{Key: "normal", Hash: "normal", Kind: SyncTaskKindBody, CreatedAt: time.Now(), EnqueuedAt: time.Now()}
	tp.workerWG.Add(1)
	done := make(chan struct{})
	go func() {
		tp.runTaskWorker()
		close(done)
	}()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if tp.Stats().SucceededBody == 1 {
			close(tp.StopCh)
			<-done
			goto ages
		}
		time.Sleep(time.Millisecond)
	}
	close(tp.StopCh)
	<-done
	t.Fatal("normal queue task was not processed")

ages:
	aged := Pool{
		Tracked: map[string]struct{}{
			"body":                 {},
			headerHeightTaskKey(1): {},
			headerHashTaskKey("h"): {},
		},
		trackedSince: map[string]time.Time{
			"body":                 time.Now().Add(-3 * time.Millisecond),
			headerHeightTaskKey(1): time.Now().Add(-2 * time.Millisecond),
			headerHashTaskKey("h"): time.Now().Add(-time.Millisecond),
		},
		QueueHigh:   make(chan *SyncTask, 1),
		QueueNormal: make(chan *SyncTask, 1),
	}
	stats := aged.Stats()
	if stats.TrackedBody != 1 || stats.TrackedHeaderH != 1 || stats.TrackedHeaderHash != 1 {
		t.Fatalf("unexpected tracked age stats: %+v", stats)
	}
	if stats.TrackedOldestAgeUS == 0 || stats.TrackedAvgAgeUS == 0 || stats.TrackedBodyOldestAgeUS == 0 || stats.TrackedHeaderHOldestAgeUS == 0 || stats.TrackedHeaderHashOldestAgeUS == 0 {
		t.Fatalf("expected non-zero age stats: %+v", stats)
	}

	highSecondSelect := newCoveragePool(func(task *SyncTask, stopCh <-chan struct{}) bool { return true })
	highSecondSelect.workerWG.Add(1)
	done = make(chan struct{})
	go func() {
		highSecondSelect.runTaskWorker()
		close(done)
	}()
	time.Sleep(time.Millisecond)
	highSecondSelect.QueueHigh <- &SyncTask{Key: "high", Hash: "high", Kind: SyncTaskKindBody, CreatedAt: time.Now(), EnqueuedAt: time.Now()}
	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if highSecondSelect.Stats().SucceededBody == 1 {
			close(highSecondSelect.StopCh)
			<-done
			return
		}
		time.Sleep(time.Millisecond)
	}
	close(highSecondSelect.StopCh)
	<-done
	t.Fatal("second-select high queue task was not processed")
}
