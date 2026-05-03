package task

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTaskPoolAsyncDeduplicated(t *testing.T) {
	release := make(chan struct{})
	var handled int32

	pool := NewTaskPoolWithStop(
		TaskPoolOptions{WorkerCount: 1, HighQueueSize: 16, NormalQueueSize: 16, MaxRetry: 2},
		1,
		func(task *SyncTask, stopCh <-chan struct{}) bool {
			if task == nil || task.Hash != "0xabc" {
				return true
			}
			atomic.AddInt32(&handled, 1)
			select {
			case <-release:
			case <-stopCh:
				return false
			}
			return true
		},
	)
	pool.Start()
	t.Cleanup(func() { pool.Stop() })

	pool.EnqueueTask("0xabc")
	pool.EnqueueTask("0xabc")

	waitHandleDeadline := time.Now().Add(300 * time.Millisecond)
	for atomic.LoadInt32(&handled) == 0 && time.Now().Before(waitHandleDeadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if !pool.HasTask("0xabc") {
		t.Fatalf("task should exist while worker is processing")
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed before release: got=%v want=1", got)
	}

	close(release)
	waitCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	if err := pool.WaitIdle(waitCtx, 0); err != nil {
		t.Fatalf("task pool did not become idle: %v", err)
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed: got=%v want=1", got)
	}
}

func TestTaskPoolPriorityHighFirst(t *testing.T) {
	order := make([]string, 0, 2)
	ready := make(chan struct{})

	pool := NewTaskPoolWithStop(
		TaskPoolOptions{WorkerCount: 1, HighQueueSize: 16, NormalQueueSize: 16, MaxRetry: 2},
		1,
		func(task *SyncTask, stopCh <-chan struct{}) bool {
			_ = stopCh
			order = append(order, task.Hash)
			if len(order) == 2 {
				close(ready)
			}
			return true
		},
	)

	pool.AddTask("normal")
	pool.PushTask(&SyncTask{Hash: "normal", Priority: TaskPriorityNormal, Retry: 0}, true)
	pool.AddTask("high")
	pool.PushTask(&SyncTask{Hash: "high", Priority: TaskPriorityHigh, Retry: 0}, true)
	pool.Start()
	t.Cleanup(func() { pool.Stop() })

	select {
	case <-ready:
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("timeout waiting for tasks")
	}

	if len(order) != 2 {
		t.Fatalf("order length mismatch: %v", order)
	}
	if order[0] != "high" {
		t.Fatalf("high priority should run first, got order=%v", order)
	}
}

func TestTaskPoolRetryAndStats(t *testing.T) {
	var attempts int32
	done := make(chan struct{})

	pool := NewTaskPoolWithStop(
		TaskPoolOptions{WorkerCount: 1, HighQueueSize: 16, NormalQueueSize: 16, MaxRetry: 2},
		1,
		func(task *SyncTask, stopCh <-chan struct{}) bool {
			_ = stopCh
			if task == nil || task.Hash != "retry-me" {
				return false
			}
			current := atomic.AddInt32(&attempts, 1)
			if current >= 3 {
				select {
				case <-done:
				default:
					close(done)
				}
				return true
			}
			return false
		},
	)
	pool.Start()
	t.Cleanup(func() { pool.Stop() })

	pool.EnqueueTask("retry-me")

	select {
	case <-done:
	case <-time.After(800 * time.Millisecond):
		t.Fatalf("timeout waiting retry task completion")
	}

	waitCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := pool.WaitIdle(waitCtx, 0); err != nil {
		t.Fatalf("task pool did not become idle after retry: %v", err)
	}

	stats := pool.Stats()
	if stats.Retried < 2 {
		t.Fatalf("expected retried >= 2, got=%v", stats.Retried)
	}
	if stats.Succeeded < 1 {
		t.Fatalf("expected succeeded >= 1, got=%v", stats.Succeeded)
	}
	if stats.WorkerCount != 1 {
		t.Fatalf("expected worker count 1, got=%v", stats.WorkerCount)
	}
	if stats.MaxRetry != 2 {
		t.Fatalf("expected max retry 2, got=%v", stats.MaxRetry)
	}
}

func TestNormalizeTaskPoolOptions(t *testing.T) {
	options := NormalizeTaskPoolOptions(TaskPoolOptions{}, 3)
	if options.WorkerCount != 3 {
		t.Fatalf("worker count mismatch: got=%v want=3", options.WorkerCount)
	}
	if options.HighQueueSize != 1024 {
		t.Fatalf("high queue size mismatch: got=%v want=1024", options.HighQueueSize)
	}
	if options.NormalQueueSize != 2048 {
		t.Fatalf("normal queue size mismatch: got=%v want=2048", options.NormalQueueSize)
	}
	if options.MaxRetry != 2 {
		t.Fatalf("max retry mismatch: got=%v want=2", options.MaxRetry)
	}
}
