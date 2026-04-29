package task

import (
	"testing"
	"time"
)

func TestTaskPoolResetTracked(t *testing.T) {
	tp := Pool{
		HandleTaskFn: func(task *SyncTask, stopCh <-chan struct{}) bool { return true },
		Tracked:      make(map[string]struct{}),
		QueueHigh:    make(chan *SyncTask, 8),
		QueueNormal:  make(chan *SyncTask, 8),
		StopCh:       make(chan struct{}),
		WorkerCount:  1,
		MaxRetry:     1,
	}
	tp.AddTracked("0x1")
	tp.AddTracked("0x2")
	if tp.TrackedCount() != 2 {
		t.Fatalf("expected 2 tracked tasks before reset, got=%d", tp.TrackedCount())
	}
	tp.ResetTracked()
	if tp.TrackedCount() != 0 {
		t.Fatalf("expected tracked tasks reset to zero, got=%d", tp.TrackedCount())
	}
}

func TestTaskPoolStopWaitsTaskGoroutine(t *testing.T) {
	taskDone := make(chan struct{})
	allowExit := make(chan struct{})

	tp := Pool{
		HandleTaskFn: func(task *SyncTask, stopCh <-chan struct{}) bool {
			close(taskDone)
			select {
			case <-allowExit:
				return true
			case <-stopCh:
				return false
			}
		},
		Tracked:     make(map[string]struct{}),
		QueueHigh:   make(chan *SyncTask, 8),
		QueueNormal: make(chan *SyncTask, 8),
		StopCh:      make(chan struct{}),
		WorkerCount: 1,
		MaxRetry:    1,
	}

	tp.Start()
	tp.EnqueueTask("0xabc")

	select {
	case <-taskDone:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("task goroutine did not start in time")
	}

	done := make(chan struct{})
	go func() {
		tp.Stop()
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
