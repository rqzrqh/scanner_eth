package task

import (
	"fmt"
	"scanner_eth/util"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	TaskPriorityHigh = iota + 1
	TaskPriorityNormal
)

const (
	SyncTaskKindBody = iota + 1
	SyncTaskKindHeaderHeight
	SyncTaskKindHeaderHash
)

const (
	headerHeightTaskPrefix = "header_height:"
	headerHashTaskPrefix   = "header_hash:"
)

type SyncTask struct {
	Key      string
	Hash     string
	Height   uint64
	Kind     int
	Priority int
	Retry    int
}

type TaskPoolStats struct {
	Enqueued            uint64
	EnqueuedBody        uint64
	EnqueuedHeaderH     uint64
	EnqueuedHeaderHash  uint64
	Dequeued            uint64
	DequeuedBody        uint64
	DequeuedHeaderH     uint64
	DequeuedHeaderHash  uint64
	Succeeded           uint64
	SucceededBody       uint64
	SucceededHeaderH    uint64
	SucceededHeaderHash uint64
	Failed              uint64
	FailedBody          uint64
	FailedHeaderH       uint64
	FailedHeaderHash    uint64
	Retried             uint64
	RetriedBody         uint64
	RetriedHeaderH      uint64
	RetriedHeaderHash   uint64
	Dropped             uint64
	DroppedBody         uint64
	DroppedHeaderH      uint64
	DroppedHeaderHash   uint64
	PendingHigh         uint64
	PendingNormal       uint64
	Tracked             uint64
	TrackedBody         uint64
	TrackedHeaderH      uint64
	TrackedHeaderHash   uint64
	WorkerCount         uint64
	HighQueueCapacity   uint64
	NormalQueueCapacity uint64
	MaxRetry            uint64
}

type TaskPoolOptions struct {
	WorkerCount      int
	HighQueueSize    int
	NormalQueueSize  int
	MaxRetry         int
	StatsLogInterval time.Duration
}

func NormalizeTaskPoolOptions(options TaskPoolOptions, clientCount int) TaskPoolOptions {
	if options.WorkerCount <= 0 {
		options.WorkerCount = clientCount
	}
	if options.WorkerCount <= 0 {
		options.WorkerCount = 1
	}
	if options.HighQueueSize <= 0 {
		options.HighQueueSize = 1024
	}
	if options.NormalQueueSize <= 0 {
		options.NormalQueueSize = 2048
	}
	if options.MaxRetry < 0 {
		options.MaxRetry = 0
	}
	if options.MaxRetry == 0 {
		options.MaxRetry = 2
	}
	if options.StatsLogInterval < 0 {
		options.StatsLogInterval = 0
	}
	return options
}

type Pool struct {
	mu       sync.Mutex
	once     sync.Once
	stopOnce sync.Once
	workerWG sync.WaitGroup
	taskWG   sync.WaitGroup
	started  bool

	HandleTaskFn     func(task *SyncTask, stopCh <-chan struct{}) bool
	Tracked          map[string]struct{}
	QueueHigh        chan *SyncTask
	QueueNormal      chan *SyncTask
	StopCh           chan struct{}
	WorkerCount      int
	MaxRetry         int
	StatsLogInterval time.Duration

	enqueued          uint64
	enqueuedBody      uint64
	enqueuedHeaderH   uint64
	enqueuedHeaderHs  uint64
	dequeued          uint64
	dequeuedBody      uint64
	dequeuedHeaderH   uint64
	dequeuedHeaderHs  uint64
	succeeded         uint64
	succeededBody     uint64
	succeededHeaderH  uint64
	succeededHeaderHs uint64
	failed            uint64
	failedBody        uint64
	failedHeaderH     uint64
	failedHeaderHs    uint64
	retried           uint64
	retriedBody       uint64
	retriedHeaderH    uint64
	retriedHeaderHs   uint64
	dropped           uint64
	droppedBody       uint64
	droppedHeaderH    uint64
	droppedHeaderHs   uint64
	MetricsOnce       sync.Once
}

func NewTaskPoolWithStop(options TaskPoolOptions, clientCount int, handleTaskFn func(task *SyncTask, stopCh <-chan struct{}) bool) Pool {
	options = NormalizeTaskPoolOptions(options, clientCount)
	return Pool{
		HandleTaskFn:     handleTaskFn,
		Tracked:          make(map[string]struct{}),
		QueueHigh:        make(chan *SyncTask, options.HighQueueSize),
		QueueNormal:      make(chan *SyncTask, options.NormalQueueSize),
		StopCh:           make(chan struct{}),
		WorkerCount:      options.WorkerCount,
		MaxRetry:         options.MaxRetry,
		StatsLogInterval: options.StatsLogInterval,
	}
}

func (tp *Pool) HasTracked(hash string) bool {
	if hash == "" {
		return false
	}
	tp.mu.Lock()
	_, ok := tp.Tracked[hash]
	tp.mu.Unlock()
	return ok
}

func (tp *Pool) HasTrackedKey(key string) bool {
	if key == "" {
		return false
	}
	tp.mu.Lock()
	_, ok := tp.Tracked[key]
	tp.mu.Unlock()
	return ok
}

func (tp *Pool) AddTracked(hash string) {
	if hash == "" {
		return
	}
	tp.mu.Lock()
	if tp.Tracked == nil {
		tp.Tracked = make(map[string]struct{})
	}
	tp.Tracked[hash] = struct{}{}
	tp.mu.Unlock()
}

func (tp *Pool) AddTrackedKey(key string) {
	if key == "" {
		return
	}
	tp.mu.Lock()
	if tp.Tracked == nil {
		tp.Tracked = make(map[string]struct{})
	}
	tp.Tracked[key] = struct{}{}
	tp.mu.Unlock()
}

func (tp *Pool) DelTracked(hash string) {
	if hash == "" {
		return
	}
	tp.mu.Lock()
	delete(tp.Tracked, hash)
	tp.mu.Unlock()
}

func (tp *Pool) DelTrackedKey(key string) {
	if key == "" {
		return
	}
	tp.mu.Lock()
	delete(tp.Tracked, key)
	tp.mu.Unlock()
}

func (tp *Pool) ResetTracked() {
	tp.mu.Lock()
	tp.Tracked = make(map[string]struct{})
	tp.mu.Unlock()
}

func (tp *Pool) TrackedCount() int {
	tp.mu.Lock()
	n := len(tp.Tracked)
	tp.mu.Unlock()
	return n
}

func (tp *Pool) HasTask(hash string) bool   { return tp.HasTracked(hash) }
func (tp *Pool) HasTaskKey(key string) bool { return tp.HasTrackedKey(key) }
func (tp *Pool) AddTask(hash string)        { tp.AddTracked(hash) }
func (tp *Pool) AddTaskKey(key string)      { tp.AddTrackedKey(key) }
func (tp *Pool) DelTask(hash string)        { tp.DelTracked(hash) }
func (tp *Pool) DelTaskKey(key string)      { tp.DelTrackedKey(key) }

func (tp *Pool) EnqueueTask(hash string) {
	tp.EnqueueTaskWithPriority(hash, TaskPriorityNormal)
}

func (tp *Pool) EnqueueTaskWithPriority(hash string, priority int) {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return
	}
	task := &SyncTask{Key: hash, Hash: hash, Kind: SyncTaskKindBody, Priority: priority, Retry: 0}
	_ = tp.EnqueueSyncTask(task)
}

func (tp *Pool) EnqueueHeaderHeightTask(height uint64) bool {
	task := &SyncTask{Key: headerHeightTaskKey(height), Height: height, Kind: SyncTaskKindHeaderHeight, Priority: TaskPriorityHigh, Retry: 0}
	return tp.EnqueueSyncTask(task)
}

func (tp *Pool) EnqueueHeaderHashTask(hash string) bool {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return false
	}
	task := &SyncTask{Key: headerHashTaskKey(hash), Hash: hash, Kind: SyncTaskKindHeaderHash, Priority: TaskPriorityHigh, Retry: 0}
	return tp.EnqueueSyncTask(task)
}

func (tp *Pool) EnqueueSyncTask(task *SyncTask) bool {
	tp.Start()
	if task == nil {
		return false
	}
	if task.Hash != "" {
		task.Hash = util.NormalizeHash(task.Hash)
	}
	if task.Key == "" {
		switch task.Kind {
		case SyncTaskKindHeaderHeight:
			task.Key = headerHeightTaskKey(task.Height)
		case SyncTaskKindHeaderHash:
			task.Key = headerHashTaskKey(task.Hash)
		default:
			if task.Hash == "" {
				return false
			}
			task.Kind = SyncTaskKindBody
			task.Key = task.Hash
		}
	}
	if task.Key == "" || tp.HasTaskKey(task.Key) {
		return false
	}
	tp.AddTaskKey(task.Key)
	return tp.PushTask(task, true)
}

func (tp *Pool) PushTask(task *SyncTask, clearOnDrop bool) bool {
	if task == nil {
		return false
	}
	if task.Key == "" {
		if task.Kind == SyncTaskKindHeaderHeight {
			task.Key = headerHeightTaskKey(task.Height)
		} else if task.Kind == SyncTaskKindHeaderHash {
			task.Hash = util.NormalizeHash(task.Hash)
			task.Key = headerHashTaskKey(task.Hash)
		} else {
			task.Hash = util.NormalizeHash(task.Hash)
			if task.Hash == "" {
				return false
			}
			task.Kind = SyncTaskKindBody
			task.Key = task.Hash
		}
	}
	if task.Kind == SyncTaskKindBody && task.Hash == "" {
		return false
	}
	if task.Priority != TaskPriorityHigh {
		task.Priority = TaskPriorityNormal
	}

	var sent bool
	if task.Priority == TaskPriorityHigh {
		select {
		case tp.QueueHigh <- task:
			sent = true
		default:
		}
	}
	if !sent {
		select {
		case tp.QueueNormal <- task:
			sent = true
		default:
		}
	}

	if sent {
		atomic.AddUint64(&tp.enqueued, 1)
		tp.addKindCounter(&tp.enqueuedBody, &tp.enqueuedHeaderH, &tp.enqueuedHeaderHs, task.Kind)
		return true
	}

	atomic.AddUint64(&tp.dropped, 1)
	tp.addKindCounter(&tp.droppedBody, &tp.droppedHeaderH, &tp.droppedHeaderHs, task.Kind)
	if clearOnDrop {
		tp.DelTaskKey(task.Key)
	}
	logrus.Warnf("task queue full, drop task key:%v kind:%v priority:%v retry:%v", task.Key, task.Kind, task.Priority, task.Retry)
	return false
}

func (tp *Pool) Start() {
	tp.once.Do(func() {
		tp.started = true
		if tp.WorkerCount <= 0 {
			tp.WorkerCount = 1
		}
		if tp.QueueHigh == nil {
			tp.QueueHigh = make(chan *SyncTask, 1024)
		}
		if tp.QueueNormal == nil {
			tp.QueueNormal = make(chan *SyncTask, 2048)
		}
		if tp.StopCh == nil {
			tp.StopCh = make(chan struct{})
		}
		if tp.StatsLogInterval > 0 {
			go tp.runTaskStatsReporter()
		}
		for i := 0; i < tp.WorkerCount; i++ {
			tp.workerWG.Add(1)
			go tp.runTaskWorker()
		}
	})
}

func (tp *Pool) runTaskStatsReporter() {
	ticker := time.NewTicker(tp.StatsLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tp.StopCh:
			return
		case <-ticker.C:
			stats := tp.Stats()
			logrus.Infof("task pool stats enqueued:%v(body:%v hh:%v hs:%v) dequeued:%v(body:%v hh:%v hs:%v) succeeded:%v(body:%v hh:%v hs:%v) failed:%v(body:%v hh:%v hs:%v) retried:%v(body:%v hh:%v hs:%v) dropped:%v(body:%v hh:%v hs:%v) pending_high:%v pending_normal:%v tracked:%v(body:%v hh:%v hs:%v) workers:%v max_retry:%v",
				stats.Enqueued, stats.EnqueuedBody, stats.EnqueuedHeaderH, stats.EnqueuedHeaderHash,
				stats.Dequeued, stats.DequeuedBody, stats.DequeuedHeaderH, stats.DequeuedHeaderHash,
				stats.Succeeded, stats.SucceededBody, stats.SucceededHeaderH, stats.SucceededHeaderHash,
				stats.Failed, stats.FailedBody, stats.FailedHeaderH, stats.FailedHeaderHash,
				stats.Retried, stats.RetriedBody, stats.RetriedHeaderH, stats.RetriedHeaderHash,
				stats.Dropped, stats.DroppedBody, stats.DroppedHeaderH, stats.DroppedHeaderHash,
				stats.PendingHigh, stats.PendingNormal,
				stats.Tracked, stats.TrackedBody, stats.TrackedHeaderH, stats.TrackedHeaderHash,
				stats.WorkerCount, stats.MaxRetry,
			)
		}
	}
}

func (tp *Pool) runTaskWorker() {
	defer tp.workerWG.Done()
	for {
		var task *SyncTask

		select {
		case <-tp.StopCh:
			return
		case task = <-tp.QueueHigh:
		default:
			select {
			case <-tp.StopCh:
				return
			case task = <-tp.QueueHigh:
			case task = <-tp.QueueNormal:
			}
		}

		atomic.AddUint64(&tp.dequeued, 1)
		tp.addKindCounter(&tp.dequeuedBody, &tp.dequeuedHeaderH, &tp.dequeuedHeaderHs, task.Kind)

		done := make(chan struct{})
		tp.taskWG.Add(1)
		go func(t *SyncTask) {
			defer close(done)
			tp.executeTask(t)
		}(task)
		<-done
	}
}

func (tp *Pool) executeTask(task *SyncTask) {
	defer tp.taskWG.Done()
	if tp.IsStopping() {
		tp.DelTaskKey(task.Key)
		return
	}

	success := tp.handleTask(task)
	if success {
		atomic.AddUint64(&tp.succeeded, 1)
		tp.addKindCounter(&tp.succeededBody, &tp.succeededHeaderH, &tp.succeededHeaderHs, task.Kind)
		tp.DelTaskKey(task.Key)
		return
	}

	if task.Retry < tp.MaxRetry {
		if tp.IsStopping() {
			tp.DelTaskKey(task.Key)
			return
		}
		task.Retry++
		atomic.AddUint64(&tp.retried, 1)
		tp.addKindCounter(&tp.retriedBody, &tp.retriedHeaderH, &tp.retriedHeaderHs, task.Kind)
		if tp.PushTask(task, false) {
			return
		}
	}

	atomic.AddUint64(&tp.failed, 1)
	tp.addKindCounter(&tp.failedBody, &tp.failedHeaderH, &tp.failedHeaderHs, task.Kind)
	tp.DelTaskKey(task.Key)
}

func (tp *Pool) handleTask(task *SyncTask) bool {
	if task == nil || task.Key == "" {
		return true
	}
	tp.mu.Lock()
	handler := tp.HandleTaskFn
	tp.mu.Unlock()
	if handler == nil {
		return false
	}
	return handler(task, tp.StopCh)
}

func (tp *Pool) IsStopping() bool {
	if tp == nil {
		return false
	}
	select {
	case <-tp.StopCh:
		return true
	default:
		return false
	}
}

func headerHeightTaskKey(height uint64) string {
	return fmt.Sprintf("%s%d", headerHeightTaskPrefix, height)
}

func headerHashTaskKey(hash string) string {
	hash = util.NormalizeHash(hash)
	if hash == "" {
		return ""
	}
	return headerHashTaskPrefix + hash
}

func (tp *Pool) IsHeaderHeightSyncing(height uint64) bool {
	return tp.HasTaskKey(headerHeightTaskKey(height))
}

func (tp *Pool) IsHeaderHashSyncing(hash string) bool {
	key := headerHashTaskKey(hash)
	if key == "" {
		return false
	}
	return tp.HasTaskKey(key)
}

func (tp *Pool) tryReserveTrackedKey(key string) bool {
	if key == "" {
		return false
	}
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.Tracked == nil {
		tp.Tracked = make(map[string]struct{})
	}
	if _, exists := tp.Tracked[key]; exists {
		return false
	}
	tp.Tracked[key] = struct{}{}
	return true
}

func (tp *Pool) TryStartHeaderHeightSync(height uint64) bool {
	return tp.tryReserveTrackedKey(headerHeightTaskKey(height))
}

func (tp *Pool) FinishHeaderHeightSync(height uint64) {
	tp.DelTaskKey(headerHeightTaskKey(height))
}

func (tp *Pool) TryStartHeaderHashSync(hash string) bool {
	return tp.tryReserveTrackedKey(headerHashTaskKey(hash))
}

func (tp *Pool) FinishHeaderHashSync(hash string) {
	key := headerHashTaskKey(hash)
	if key == "" {
		return
	}
	tp.DelTaskKey(key)
}

func (tp *Pool) HeaderSyncCounts() (heightCount int, hashCount int) {
	_, hh, hs := tp.trackedCountsByKind()
	return hh, hs
}

func (tp *Pool) trackedCountsByKind() (bodyCount int, headerHeightCount int, headerHashCount int) {
	tp.mu.Lock()
	for k := range tp.Tracked {
		if strings.HasPrefix(k, headerHeightTaskPrefix) {
			headerHeightCount++
			continue
		}
		if strings.HasPrefix(k, headerHashTaskPrefix) {
			headerHashCount++
			continue
		}
		bodyCount++
	}
	tp.mu.Unlock()
	return bodyCount, headerHeightCount, headerHashCount
}

func (tp *Pool) addKindCounter(bodyCounter *uint64, headerHeightCounter *uint64, headerHashCounter *uint64, kind int) {
	switch kind {
	case SyncTaskKindHeaderHeight:
		atomic.AddUint64(headerHeightCounter, 1)
	case SyncTaskKindHeaderHash:
		atomic.AddUint64(headerHashCounter, 1)
	default:
		atomic.AddUint64(bodyCounter, 1)
	}
}

func (tp *Pool) Stop() {
	if !tp.started {
		return
	}
	tp.stopOnce.Do(func() {
		if tp.StopCh != nil {
			close(tp.StopCh)
		}
		tp.workerWG.Wait()
		tp.taskWG.Wait()
		tp.started = false
	})
}

func (tp *Pool) Stats() TaskPoolStats {
	trackedBody, trackedHeaderH, trackedHeaderHash := tp.trackedCountsByKind()
	tracked := trackedBody + trackedHeaderH + trackedHeaderHash

	return TaskPoolStats{
		Enqueued:            atomic.LoadUint64(&tp.enqueued),
		EnqueuedBody:        atomic.LoadUint64(&tp.enqueuedBody),
		EnqueuedHeaderH:     atomic.LoadUint64(&tp.enqueuedHeaderH),
		EnqueuedHeaderHash:  atomic.LoadUint64(&tp.enqueuedHeaderHs),
		Dequeued:            atomic.LoadUint64(&tp.dequeued),
		DequeuedBody:        atomic.LoadUint64(&tp.dequeuedBody),
		DequeuedHeaderH:     atomic.LoadUint64(&tp.dequeuedHeaderH),
		DequeuedHeaderHash:  atomic.LoadUint64(&tp.dequeuedHeaderHs),
		Succeeded:           atomic.LoadUint64(&tp.succeeded),
		SucceededBody:       atomic.LoadUint64(&tp.succeededBody),
		SucceededHeaderH:    atomic.LoadUint64(&tp.succeededHeaderH),
		SucceededHeaderHash: atomic.LoadUint64(&tp.succeededHeaderHs),
		Failed:              atomic.LoadUint64(&tp.failed),
		FailedBody:          atomic.LoadUint64(&tp.failedBody),
		FailedHeaderH:       atomic.LoadUint64(&tp.failedHeaderH),
		FailedHeaderHash:    atomic.LoadUint64(&tp.failedHeaderHs),
		Retried:             atomic.LoadUint64(&tp.retried),
		RetriedBody:         atomic.LoadUint64(&tp.retriedBody),
		RetriedHeaderH:      atomic.LoadUint64(&tp.retriedHeaderH),
		RetriedHeaderHash:   atomic.LoadUint64(&tp.retriedHeaderHs),
		Dropped:             atomic.LoadUint64(&tp.dropped),
		DroppedBody:         atomic.LoadUint64(&tp.droppedBody),
		DroppedHeaderH:      atomic.LoadUint64(&tp.droppedHeaderH),
		DroppedHeaderHash:   atomic.LoadUint64(&tp.droppedHeaderHs),
		PendingHigh:         uint64(len(tp.QueueHigh)),
		PendingNormal:       uint64(len(tp.QueueNormal)),
		Tracked:             uint64(tracked),
		TrackedBody:         uint64(trackedBody),
		TrackedHeaderH:      uint64(trackedHeaderH),
		TrackedHeaderHash:   uint64(trackedHeaderHash),
		WorkerCount:         uint64(tp.WorkerCount),
		HighQueueCapacity:   uint64(cap(tp.QueueHigh)),
		NormalQueueCapacity: uint64(cap(tp.QueueNormal)),
		MaxRetry:            uint64(tp.MaxRetry),
	}
}

func (tp *Pool) MetricsPayload() map[string]any {
	stats := tp.Stats()
	return map[string]any{
		"totals": map[string]uint64{
			"enqueued":  stats.Enqueued,
			"dequeued":  stats.Dequeued,
			"succeeded": stats.Succeeded,
			"failed":    stats.Failed,
			"retried":   stats.Retried,
			"dropped":   stats.Dropped,
		},
		"by_kind": map[string]map[string]uint64{
			"body": {
				"enqueued":  stats.EnqueuedBody,
				"dequeued":  stats.DequeuedBody,
				"succeeded": stats.SucceededBody,
				"failed":    stats.FailedBody,
				"retried":   stats.RetriedBody,
				"dropped":   stats.DroppedBody,
				"tracked":   stats.TrackedBody,
			},
			"header_height_sync": {
				"enqueued":  stats.EnqueuedHeaderH,
				"dequeued":  stats.DequeuedHeaderH,
				"succeeded": stats.SucceededHeaderH,
				"failed":    stats.FailedHeaderH,
				"retried":   stats.RetriedHeaderH,
				"dropped":   stats.DroppedHeaderH,
				"tracked":   stats.TrackedHeaderH,
			},
			"header_hash_sync": {
				"enqueued":  stats.EnqueuedHeaderHash,
				"dequeued":  stats.DequeuedHeaderHash,
				"succeeded": stats.SucceededHeaderHash,
				"failed":    stats.FailedHeaderHash,
				"retried":   stats.RetriedHeaderHash,
				"dropped":   stats.DroppedHeaderHash,
				"tracked":   stats.TrackedHeaderHash,
			},
		},
		"queues": map[string]uint64{
			"pending_high":   stats.PendingHigh,
			"pending_normal": stats.PendingNormal,
		},
		"tracked": map[string]uint64{
			"total": stats.Tracked,
		},
		"config": map[string]uint64{
			"worker_count":          stats.WorkerCount,
			"high_queue_capacity":   stats.HighQueueCapacity,
			"normal_queue_capacity": stats.NormalQueueCapacity,
			"max_retry":             stats.MaxRetry,
		},
	}
}
