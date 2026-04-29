package fetch

import (
	"context"
	"expvar"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"scanner_eth/data"
	fetcherpkg "scanner_eth/fetch/fetcher"
	fetchtask "scanner_eth/fetch/task"
)

func TestTaskPoolAsyncDeduplicated(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "0xabc", "", 1, nil)
	setTestNodeBlockHeader(t, fm, "0xabc", makeHeader(1, "0xabc", ""))
	setNodeLatestHeight(fm, 1)

	release := make(chan struct{})
	var handled int32
	setTestBlockFetcher(fm, &mockBlockFetcher{fetchFullFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		if normalizeHash(header.Hash) == "0xabc" {
			atomic.AddInt32(&handled, 1)
			<-release
		}
		return makeMinimalFullBlock(1, header.Hash, header.ParentHash)
	}})
	fm.taskPool.Start()
	t.Cleanup(func() { fm.taskPool.Stop() })

	fm.taskPool.EnqueueTask("0xabc")
	fm.taskPool.EnqueueTask("0xabc")

	waitHandleDeadline := time.Now().Add(300 * time.Millisecond)
	for atomic.LoadInt32(&handled) == 0 && time.Now().Before(waitHandleDeadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if !fm.taskPool.HasTask("0xabc") {
		t.Fatalf("task should exist while worker is processing")
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed before release: got=%v want=1", got)
	}

	close(release)
	deadline := time.Now().Add(500 * time.Millisecond)
	for fm.taskPool.HasTask("0xabc") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if fm.taskPool.HasTask("0xabc") {
		t.Fatalf("task should be removed after async worker completes")
	}
	if got := atomic.LoadInt32(&handled); got != 1 {
		t.Fatalf("dedupe failed: got=%v want=1", got)
	}
}

func TestTaskPoolPriorityHighFirst(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "normal", "", 1, nil)
	fm.blockTree.Insert(2, "high", "normal", 1, nil)
	setTestNodeBlockHeader(t, fm, "normal", makeHeader(1, "normal", ""))
	setTestNodeBlockHeader(t, fm, "high", makeHeader(2, "high", "normal"))
	setNodeLatestHeight(fm, 2)
	order := make([]string, 0, 2)
	ready := make(chan struct{})

	setTestBlockFetcher(fm, &mockBlockFetcher{fetchFullFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
		order = append(order, header.Hash)
		if len(order) == 2 {
			close(ready)
		}
		height := uint64(1)
		if header.Hash == "high" {
			height = 2
		}
		return makeMinimalFullBlock(height, header.Hash, header.ParentHash)
	}})
	fm.taskPool.AddTask("normal")
	fm.taskPool.PushTask(&fetchtask.SyncTask{Hash: "normal", Priority: fetchtask.TaskPriorityNormal, Retry: 0}, true)
	fm.taskPool.AddTask("high")
	fm.taskPool.PushTask(&fetchtask.SyncTask{Hash: "high", Priority: fetchtask.TaskPriorityHigh, Retry: 0}, true)
	fm.taskPool.Start()
	t.Cleanup(func() { fm.taskPool.Stop() })

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
	fm := newTestFetchManager(t, 2)
	fm.blockTree.Insert(1, "retry-me", "", 1, nil)
	setNodeLatestHeight(fm, 1)
	fm.taskPool.MaxRetry = 2
	var attempts int32
	done := make(chan struct{})

	setTestBlockFetcher(fm, &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, hash string) *BlockHeaderJson {
			if normalizeHash(hash) != "retry-me" {
				return nil
			}
			current := atomic.AddInt32(&attempts, 1)
			if current >= 3 {
				return makeHeader(1, "retry-me", "")
			}
			return nil
		},
		fetchFullFn: func(_ context.Context, nodeOp fetcherpkg.NodeOperator, taskID int, header *BlockHeaderJson) *data.FullBlock {
			if normalizeHash(header.Hash) == "retry-me" {
				select {
				case <-done:
				default:
					close(done)
				}
			}
			return makeMinimalFullBlock(1, header.Hash, header.ParentHash)
		},
	})
	fm.taskPool.Start()
	t.Cleanup(func() { fm.taskPool.Stop() })

	fm.taskPool.EnqueueTask("retry-me")

	select {
	case <-done:
	case <-time.After(800 * time.Millisecond):
		t.Fatalf("timeout waiting retry task completion")
	}

	deadline := time.Now().Add(300 * time.Millisecond)
	for fm.taskPool.HasTask("retry-me") && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	if fm.taskPool.HasTask("retry-me") {
		t.Fatalf("retry task should be removed after success")
	}

	stats := fm.taskPool.Stats()
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
	options := fetchtask.NormalizeTaskPoolOptions(fetchtask.TaskPoolOptions{}, 3)
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

func TestEnableTaskPoolMetricsPublishesStats(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	name := fmt.Sprintf("fetch_task_pool_test_%d", time.Now().UnixNano())

	fm.EnableTaskPoolMetrics(name)
	v := expvar.Get(name)
	if v == nil {
		t.Fatalf("expected expvar metric to be published")
	}
	if !strings.Contains(v.String(), "\"config\"") || !strings.Contains(v.String(), "\"worker_count\":1") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}
	if !strings.Contains(v.String(), "\"by_kind\"") || !strings.Contains(v.String(), "\"header_hash_sync\"") {
		t.Fatalf("unexpected metric payload: %v", v.String())
	}

	storeV := expvar.Get(name + "_store_block")
	if storeV == nil {
		t.Fatalf("expected store block expvar metric to be published")
	}
	if !strings.Contains(storeV.String(), "\"submitted\"") || !strings.Contains(storeV.String(), "\"storing\"") {
		t.Fatalf("unexpected store block metric payload: %v", storeV.String())
	}
}
