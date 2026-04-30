package store

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"

	"gorm.io/gorm"
)

const (
	DefaultBatchSize   = 128
	DefaultWorkerCount = 8
)

var (
	ErrWorkerNotInitialized = errors.New("store worker is not initialized")
	ErrStoreFullBlockFailed = errors.New("store fullblock failed")
)

type Runtime struct {
	mu                   sync.Mutex
	taskCounter          uint64
	batchSize            int
	storeTaskChannel     chan *Task
	storeCompleteChannel chan *Complete
	afterFirstTaskSend   func()
}

var defaultRuntime = &Runtime{
	batchSize: DefaultBatchSize,
}

func DefaultRuntime() *Runtime {
	return defaultRuntime
}

func (rt *Runtime) Init(db *gorm.DB, batchSize int, workerCount int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.batchSize = normalizeBatchSize(batchSize)
	workerCount = normalizeWorkerCount(workerCount)
	rt.storeTaskChannel = make(chan *Task, workerCount*2)
	rt.storeCompleteChannel = make(chan *Complete, workerCount*2)
	for i := 0; i < workerCount; i++ {
		NewWorker(i, db, rt.storeTaskChannel, rt.storeCompleteChannel).Run()
	}
}

func (rt *Runtime) ResetForTest(batchSize int, taskCh chan *Task, completeCh chan *Complete) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	rt.taskCounter = 0
	rt.batchSize = normalizeBatchSize(batchSize)
	rt.storeTaskChannel = taskCh
	rt.storeCompleteChannel = completeCh
}

func (rt *Runtime) StartWorkers(db *gorm.DB, workerCount int) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	workerCount = normalizeWorkerCount(workerCount)
	for i := 0; i < workerCount; i++ {
		NewWorker(i, db, rt.storeTaskChannel, rt.storeCompleteChannel).Run()
	}
}

func (rt *Runtime) BatchSize() int {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	return rt.batchSize
}

func (rt *Runtime) NextTaskID() uint64 {
	return atomic.AddUint64(&rt.taskCounter, 1)
}

func (rt *Runtime) SetAfterFirstTaskSendHook(fn func()) {
	rt.mu.Lock()
	defer rt.mu.Unlock()
	rt.afterFirstTaskSend = fn
}

func (rt *Runtime) ClearAfterFirstTaskSendHook() {
	rt.SetAfterFirstTaskSendHook(nil)
}

func (rt *Runtime) RunTasks(ctx context.Context, allTasks []*Task) error {
	if ctx == nil {
		ctx = context.Background()
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()

	if rt.storeTaskChannel == nil || rt.storeCompleteChannel == nil {
		return ErrWorkerNotInitialized
	}

	for _, t := range allTasks {
		t.Ctx = ctx
	}

	taskSet := make(map[uint64]struct{}, len(allTasks))
	for _, t := range allTasks {
		taskSet[t.TaskID] = struct{}{}
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	nextTask := 0
	storeFailed := false
	cancelled := false
	for len(taskSet) > 0 {
		if nextTask < len(allTasks) {
			select {
			case rt.storeTaskChannel <- allTasks[nextTask]:
				nextTask++
				if fn := rt.afterFirstTaskSend; fn != nil {
					rt.afterFirstTaskSend = nil
					fn()
				}
			case c := <-rt.storeCompleteChannel:
				if c == nil {
					continue
				}
				if _, ok := taskSet[c.TaskID]; !ok {
					continue
				}
				if c.Err != nil {
					storeFailed = true
				}
				delete(taskSet, c.TaskID)
			case <-ctx.Done():
				cancelled = true
			}
		} else {
			select {
			case c := <-rt.storeCompleteChannel:
				if c == nil {
					continue
				}
				if _, ok := taskSet[c.TaskID]; !ok {
					continue
				}
				if c.Err != nil {
					storeFailed = true
				}
				delete(taskSet, c.TaskID)
			case <-ctx.Done():
				cancelled = true
			}
		}
		if cancelled {
			break
		}
	}

	if cancelled {
		for i := nextTask; i < len(allTasks); i++ {
			delete(taskSet, allTasks[i].TaskID)
		}
		for len(taskSet) > 0 {
			c := <-rt.storeCompleteChannel
			if c == nil {
				continue
			}
			if _, ok := taskSet[c.TaskID]; !ok {
				continue
			}
			delete(taskSet, c.TaskID)
		}
		return ctx.Err()
	}

	if storeFailed {
		return ErrStoreFullBlockFailed
	}

	return nil
}

func normalizeBatchSize(batchSize int) int {
	if batchSize <= 0 {
		return DefaultBatchSize
	}
	return batchSize
}

func normalizeWorkerCount(workerCount int) int {
	if workerCount <= 0 {
		return DefaultWorkerCount
	}
	return workerCount
}
