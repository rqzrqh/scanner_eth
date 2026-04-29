package scan

import (
	"context"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/task"
)

type TaskPoolAdapter struct {
	taskPool *fetchtask.Pool
}

func NewTaskPoolAdapter(taskPool *fetchtask.Pool) *TaskPoolAdapter {
	return &TaskPoolAdapter{taskPool: taskPool}
}

func (a *TaskPoolAdapter) EnqueueHeaderHeightTask(height uint64) bool {
	return a != nil && a.taskPool != nil && a.taskPool.EnqueueHeaderHeightTask(height)
}

func (a *TaskPoolAdapter) EnqueueHeaderHashTask(hash string) bool {
	return a != nil && a.taskPool != nil && a.taskPool.EnqueueHeaderHashTask(hash)
}

func (a *TaskPoolAdapter) EnqueueBodyTask(hash string, priority int) {
	if a == nil || a.taskPool == nil {
		return
	}
	a.taskPool.EnqueueTaskWithPriority(hash, priority)
}

func (a *TaskPoolAdapter) IsHeaderHeightSyncing(height uint64) bool {
	return a != nil && a.taskPool != nil && a.taskPool.IsHeaderHeightSyncing(height)
}

func (a *TaskPoolAdapter) TryStartHeaderHeightSync(height uint64) bool {
	return a != nil && a.taskPool != nil && a.taskPool.TryStartHeaderHeightSync(height)
}

func (a *TaskPoolAdapter) FinishHeaderHeightSync(height uint64) {
	if a != nil && a.taskPool != nil {
		a.taskPool.FinishHeaderHeightSync(height)
	}
}

func (a *TaskPoolAdapter) IsHeaderHashSyncing(hash string) bool {
	return a != nil && a.taskPool != nil && a.taskPool.IsHeaderHashSyncing(hash)
}

func (a *TaskPoolAdapter) TryStartHeaderHashSync(hash string) bool {
	return a != nil && a.taskPool != nil && a.taskPool.TryStartHeaderHashSync(hash)
}

func (a *TaskPoolAdapter) FinishHeaderHashSync(hash string) {
	if a != nil && a.taskPool != nil {
		a.taskPool.FinishHeaderHashSync(hash)
	}
}

type PayloadStoreAccessor[H any, B any] struct {
	nodeExists func(string) bool
	store      *fetchstore.PayloadStore[H, B]
}

func NewPayloadStoreAccessor[H any, B any](nodeExists func(string) bool, store *fetchstore.PayloadStore[H, B]) *PayloadStoreAccessor[H, B] {
	return &PayloadStoreAccessor[H, B]{nodeExists: nodeExists, store: store}
}

func (a *PayloadStoreAccessor[H, B]) SetNodeBlockHeader(hash string, header any) bool {
	if a == nil || a.store == nil || a.nodeExists == nil {
		return false
	}
	if !a.nodeExists(hash) {
		return false
	}
	typed, _ := header.(H)
	a.store.SetHeader(hash, typed)
	return true
}

func (a *PayloadStoreAccessor[H, B]) SetNodeBlockBody(hash string, data any) bool {
	if a == nil || a.store == nil || a.nodeExists == nil {
		return false
	}
	if !a.nodeExists(hash) {
		return false
	}
	typed, _ := data.(B)
	a.store.SetBody(hash, typed)
	return true
}

func (a *PayloadStoreAccessor[H, B]) GetNodeBlockHeader(hash string) any {
	if a == nil || a.store == nil {
		return nil
	}
	return a.store.GetHeader(hash)
}

func (a *PayloadStoreAccessor[H, B]) GetNodeBlockBody(hash string) any {
	if a == nil || a.store == nil {
		return nil
	}
	return a.store.GetBody(hash)
}

type StoreWorkerAdapter[T any] struct {
	worker *fetchstore.SerialWorker[T]
}

func NewStoreWorkerAdapter[T any](worker *fetchstore.SerialWorker[T]) *StoreWorkerAdapter[T] {
	return &StoreWorkerAdapter[T]{worker: worker}
}

func (a *StoreWorkerAdapter[T]) IsInflight(hash string) bool {
	return a != nil && a.worker != nil && a.worker.IsInflight(hash)
}

func (a *StoreWorkerAdapter[T]) Submit(ctx context.Context, hash string, height uint64, data any) error {
	if a == nil || a.worker == nil {
		return fetchstore.ErrWorkerNotInitialized
	}
	typed, _ := data.(T)
	return a.worker.Submit(ctx, hash, height, typed)
}
