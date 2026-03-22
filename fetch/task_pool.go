package fetch

import (
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
)

const (
	taskPriorityHigh = iota + 1
	taskPriorityNormal
)

const (
	syncTaskKindBody = iota + 1
	syncTaskKindHeaderHeight
	syncTaskKindHeaderHash
)

const (
	headerHeightTaskPrefix = "header_height:"
	headerHashTaskPrefix   = "header_hash:"
)

type syncTask struct {
	key      string
	hash     string
	height   uint64
	kind     int
	priority int
	retry    int
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

func normalizeTaskPoolOptions(options TaskPoolOptions, clientCount int) TaskPoolOptions {
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

type taskPool struct {
	mu               sync.Mutex
	once             sync.Once
	stopOnce         sync.Once
	workerWG         sync.WaitGroup
	taskWG           sync.WaitGroup
	started          bool
	handleTaskFn     func(task *syncTask, stopCh <-chan struct{}) bool
	tracked          map[string]struct{}
	queueHigh        chan *syncTask
	queueNormal      chan *syncTask
	stopCh           chan struct{}
	workerCount      int
	maxRetry         int
	statsLogInterval time.Duration
	enqueued         uint64
	enqueuedBody     uint64
	enqueuedHeaderH  uint64
	enqueuedHeaderHs uint64
	dequeued         uint64
	dequeuedBody     uint64
	dequeuedHeaderH  uint64
	dequeuedHeaderHs uint64
	succeeded        uint64
	succeededBody    uint64
	succeededHeaderH uint64
	succeededHeaderHs uint64
	failed           uint64
	failedBody       uint64
	failedHeaderH    uint64
	failedHeaderHs   uint64
	retried          uint64
	retriedBody      uint64
	retriedHeaderH   uint64
	retriedHeaderHs  uint64
	dropped          uint64
	droppedBody      uint64
	droppedHeaderH   uint64
	droppedHeaderHs  uint64
	metricsOnce      sync.Once
}

func newTaskPool(options TaskPoolOptions, clientCount int, handleTaskFn func(task *syncTask) bool) taskPool {
	wrappedHandler := func(task *syncTask, _ <-chan struct{}) bool {
		if handleTaskFn == nil {
			return false
		}
		return handleTaskFn(task)
	}
	return newTaskPoolWithStop(options, clientCount, wrappedHandler)
}

func newTaskPoolWithStop(options TaskPoolOptions, clientCount int, handleTaskFn func(task *syncTask, stopCh <-chan struct{}) bool) taskPool {
	options = normalizeTaskPoolOptions(options, clientCount)
	return taskPool{
		handleTaskFn:     handleTaskFn,
		tracked:          make(map[string]struct{}),
		queueHigh:        make(chan *syncTask, options.HighQueueSize),
		queueNormal:      make(chan *syncTask, options.NormalQueueSize),
		stopCh:           make(chan struct{}),
		workerCount:      options.WorkerCount,
		maxRetry:         options.MaxRetry,
		statsLogInterval: options.StatsLogInterval,
	}
}

func (tp *taskPool) hasTracked(hash string) bool {
	if hash == "" {
		return false
	}
	tp.mu.Lock()
	_, ok := tp.tracked[hash]
	tp.mu.Unlock()
	return ok
}

func (tp *taskPool) hasTrackedKey(key string) bool {
	if key == "" {
		return false
	}
	tp.mu.Lock()
	_, ok := tp.tracked[key]
	tp.mu.Unlock()
	return ok
}

func (tp *taskPool) addTracked(hash string) {
	if hash == "" {
		return
	}
	tp.mu.Lock()
	if tp.tracked == nil {
		tp.tracked = make(map[string]struct{})
	}
	tp.tracked[hash] = struct{}{}
	tp.mu.Unlock()
}

func (tp *taskPool) addTrackedKey(key string) {
	if key == "" {
		return
	}
	tp.mu.Lock()
	if tp.tracked == nil {
		tp.tracked = make(map[string]struct{})
	}
	tp.tracked[key] = struct{}{}
	tp.mu.Unlock()
}

func (tp *taskPool) delTracked(hash string) {
	if hash == "" {
		return
	}
	tp.mu.Lock()
	delete(tp.tracked, hash)
	tp.mu.Unlock()
}

func (tp *taskPool) delTrackedKey(key string) {
	if key == "" {
		return
	}
	tp.mu.Lock()
	delete(tp.tracked, key)
	tp.mu.Unlock()
}

func (tp *taskPool) resetTracked() {
	tp.mu.Lock()
	tp.tracked = make(map[string]struct{})
	tp.mu.Unlock()
}

func (tp *taskPool) trackedCount() int {
	tp.mu.Lock()
	n := len(tp.tracked)
	tp.mu.Unlock()
	return n
}

func (tp *taskPool) hasTask(hash string) bool { return tp.hasTracked(hash) }
func (tp *taskPool) hasTaskKey(key string) bool { return tp.hasTrackedKey(key) }
func (tp *taskPool) addTask(hash string) { tp.addTracked(hash) }
func (tp *taskPool) addTaskKey(key string) { tp.addTrackedKey(key) }
func (tp *taskPool) delTask(hash string) { tp.delTracked(hash) }
func (tp *taskPool) delTaskKey(key string) { tp.delTrackedKey(key) }

func (tp *taskPool) enqueueTask(hash string) {
	tp.enqueueTaskWithPriority(hash, taskPriorityNormal)
}

func (tp *taskPool) enqueueTaskWithPriority(hash string, priority int) {
	hash = normalizeHash(hash)
	if hash == "" {
		return
	}
	task := &syncTask{key: hash, hash: hash, kind: syncTaskKindBody, priority: priority, retry: 0}
	_ = tp.enqueueSyncTask(task)
}

func (tp *taskPool) enqueueHeaderHeightTask(height uint64) bool {
	task := &syncTask{key: headerHeightTaskKey(height), height: height, kind: syncTaskKindHeaderHeight, priority: taskPriorityHigh, retry: 0}
	return tp.enqueueSyncTask(task)
}

func (tp *taskPool) enqueueHeaderHashTask(hash string) bool {
	hash = normalizeHash(hash)
	if hash == "" {
		return false
	}
	task := &syncTask{key: headerHashTaskKey(hash), hash: hash, kind: syncTaskKindHeaderHash, priority: taskPriorityHigh, retry: 0}
	return tp.enqueueSyncTask(task)
}

func (tp *taskPool) enqueueSyncTask(task *syncTask) bool {
	tp.start()
	if task == nil {
		return false
	}
	if task.hash != "" {
		task.hash = normalizeHash(task.hash)
	}
	if task.key == "" {
		switch task.kind {
		case syncTaskKindHeaderHeight:
			task.key = headerHeightTaskKey(task.height)
		case syncTaskKindHeaderHash:
			task.key = headerHashTaskKey(task.hash)
		default:
			if task.hash == "" {
				return false
			}
			task.kind = syncTaskKindBody
			task.key = task.hash
		}
	}
	if task.key == "" || tp.hasTaskKey(task.key) {
		return false
	}
	tp.addTaskKey(task.key)
	return tp.pushTask(task, true)
}

func (tp *taskPool) pushTask(task *syncTask, clearOnDrop bool) bool {
	if task == nil {
		return false
	}
	if task.key == "" {
		if task.kind == syncTaskKindHeaderHeight {
			task.key = headerHeightTaskKey(task.height)
		} else if task.kind == syncTaskKindHeaderHash {
			task.hash = normalizeHash(task.hash)
			task.key = headerHashTaskKey(task.hash)
		} else {
			task.hash = normalizeHash(task.hash)
			if task.hash == "" {
				return false
			}
			task.kind = syncTaskKindBody
			task.key = task.hash
		}
	}
	if task.kind == syncTaskKindBody && task.hash == "" {
		return false
	}
	if task.priority != taskPriorityHigh {
		task.priority = taskPriorityNormal
	}

	var sent bool
	if task.priority == taskPriorityHigh {
		select {
		case tp.queueHigh <- task:
			sent = true
		default:
		}
	}
	if !sent {
		select {
		case tp.queueNormal <- task:
			sent = true
		default:
		}
	}

	if sent {
		atomic.AddUint64(&tp.enqueued, 1)
		tp.addKindCounter(&tp.enqueuedBody, &tp.enqueuedHeaderH, &tp.enqueuedHeaderHs, task.kind)
		return true
	}

	atomic.AddUint64(&tp.dropped, 1)
	tp.addKindCounter(&tp.droppedBody, &tp.droppedHeaderH, &tp.droppedHeaderHs, task.kind)
	if clearOnDrop {
		tp.delTaskKey(task.key)
	}
	logrus.Warnf("task queue full, drop task key:%v kind:%v priority:%v retry:%v", task.key, task.kind, task.priority, task.retry)
	return false
}

func (tp *taskPool) start() {
	tp.once.Do(func() {
		tp.started = true
		if tp.workerCount <= 0 {
			tp.workerCount = 1
		}
		if tp.queueHigh == nil {
			tp.queueHigh = make(chan *syncTask, 1024)
		}
		if tp.queueNormal == nil {
			tp.queueNormal = make(chan *syncTask, 2048)
		}
		if tp.stopCh == nil {
			tp.stopCh = make(chan struct{})
		}
		if tp.statsLogInterval > 0 {
			go tp.runTaskStatsReporter()
		}
		for i := 0; i < tp.workerCount; i++ {
			tp.workerWG.Add(1)
			go tp.runTaskWorker()
		}
	})
}

func (tp *taskPool) runTaskStatsReporter() {
	ticker := time.NewTicker(tp.statsLogInterval)
	defer ticker.Stop()

	for {
		select {
		case <-tp.stopCh:
			return
		case <-ticker.C:
			stats := tp.stats()
			logrus.Infof("task pool stats enqueued:%v(body:%v hh:%v hs:%v) dequeued:%v(body:%v hh:%v hs:%v) succeeded:%v(body:%v hh:%v hs:%v) failed:%v(body:%v hh:%v hs:%v) retried:%v(body:%v hh:%v hs:%v) dropped:%v(body:%v hh:%v hs:%v) pending_high:%v pending_normal:%v tracked:%v(body:%v hh:%v hs:%v) workers:%v max_retry:%v",
				stats.Enqueued,
				stats.EnqueuedBody,
				stats.EnqueuedHeaderH,
				stats.EnqueuedHeaderHash,
				stats.Dequeued,
				stats.DequeuedBody,
				stats.DequeuedHeaderH,
				stats.DequeuedHeaderHash,
				stats.Succeeded,
				stats.SucceededBody,
				stats.SucceededHeaderH,
				stats.SucceededHeaderHash,
				stats.Failed,
				stats.FailedBody,
				stats.FailedHeaderH,
				stats.FailedHeaderHash,
				stats.Retried,
				stats.RetriedBody,
				stats.RetriedHeaderH,
				stats.RetriedHeaderHash,
				stats.Dropped,
				stats.DroppedBody,
				stats.DroppedHeaderH,
				stats.DroppedHeaderHash,
				stats.PendingHigh,
				stats.PendingNormal,
				stats.Tracked,
				stats.TrackedBody,
				stats.TrackedHeaderH,
				stats.TrackedHeaderHash,
				stats.WorkerCount,
				stats.MaxRetry,
			)
		}
	}
}

func (tp *taskPool) runTaskWorker() {
	defer tp.workerWG.Done()
	for {
		var task *syncTask

		select {
		case <-tp.stopCh:
			return
		case task = <-tp.queueHigh:
		default:
			select {
			case <-tp.stopCh:
				return
			case task = <-tp.queueHigh:
			case task = <-tp.queueNormal:
			}
		}

		if task == nil {
			continue
		}
		atomic.AddUint64(&tp.dequeued, 1)
		tp.addKindCounter(&tp.dequeuedBody, &tp.dequeuedHeaderH, &tp.dequeuedHeaderHs, task.kind)

		done := make(chan struct{})
		tp.taskWG.Add(1)
		go func(t *syncTask) {
			defer close(done)
			tp.executeTask(t)
		}(task)
		<-done
	}
}

func (tp *taskPool) executeTask(task *syncTask) {
	defer tp.taskWG.Done()
	if task == nil {
		return
	}
	if tp.isStopping() {
		tp.delTaskKey(task.key)
		return
	}

	success := tp.handleTask(task)
	if success {
		atomic.AddUint64(&tp.succeeded, 1)
		tp.addKindCounter(&tp.succeededBody, &tp.succeededHeaderH, &tp.succeededHeaderHs, task.kind)
		tp.delTaskKey(task.key)
		return
	}

	if task.retry < tp.maxRetry {
		if tp.isStopping() {
			tp.delTaskKey(task.key)
			return
		}
		task.retry++
		atomic.AddUint64(&tp.retried, 1)
		tp.addKindCounter(&tp.retriedBody, &tp.retriedHeaderH, &tp.retriedHeaderHs, task.kind)
		if tp.pushTask(task, false) {
			return
		}
	}

	atomic.AddUint64(&tp.failed, 1)
	tp.addKindCounter(&tp.failedBody, &tp.failedHeaderH, &tp.failedHeaderHs, task.kind)
	tp.delTaskKey(task.key)
}

func (tp *taskPool) handleTask(task *syncTask) bool {
	if task == nil || task.key == "" {
		return true
	}
	tp.mu.Lock()
	handler := tp.handleTaskFn
	tp.mu.Unlock()
	if handler == nil {
		return false
	}
	return handler(task, tp.stopCh)
}

func (tp *taskPool) isStopping() bool {
	if tp == nil || tp.stopCh == nil {
		return false
	}
	select {
	case <-tp.stopCh:
		return true
	default:
		return false
	}
}

func headerHeightTaskKey(height uint64) string {
	return fmt.Sprintf("%s%d", headerHeightTaskPrefix, height)
}

func headerHashTaskKey(hash string) string {
	hash = normalizeHash(hash)
	if hash == "" {
		return ""
	}
	return headerHashTaskPrefix + hash
}

func (tp *taskPool) isHeaderHeightSyncing(height uint64) bool {
	return tp.hasTaskKey(headerHeightTaskKey(height))
}

func (tp *taskPool) isHeaderHashSyncing(hash string) bool {
	key := headerHashTaskKey(hash)
	if key == "" {
		return false
	}
	return tp.hasTaskKey(key)
}

func (tp *taskPool) tryStartHeaderHeightSync(height uint64) bool {
	key := headerHeightTaskKey(height)
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.tracked == nil {
		tp.tracked = make(map[string]struct{})
	}
	if _, exists := tp.tracked[key]; exists {
		return false
	}
	tp.tracked[key] = struct{}{}
	return true
}

func (tp *taskPool) finishHeaderHeightSync(height uint64) {
	tp.delTaskKey(headerHeightTaskKey(height))
}

func (tp *taskPool) tryStartHeaderHashSync(hash string) bool {
	key := headerHashTaskKey(hash)
	if key == "" {
		return false
	}
	tp.mu.Lock()
	defer tp.mu.Unlock()
	if tp.tracked == nil {
		tp.tracked = make(map[string]struct{})
	}
	if _, exists := tp.tracked[key]; exists {
		return false
	}
	tp.tracked[key] = struct{}{}
	return true
}

func (tp *taskPool) finishHeaderHashSync(hash string) {
	key := headerHashTaskKey(hash)
	if key == "" {
		return
	}
	tp.delTaskKey(key)
}

func (tp *taskPool) headerSyncCounts() (heightCount int, hashCount int) {
	tp.mu.Lock()
	for k := range tp.tracked {
		if strings.HasPrefix(k, headerHeightTaskPrefix) {
			heightCount++
			continue
		}
		if strings.HasPrefix(k, headerHashTaskPrefix) {
			hashCount++
		}
	}
	tp.mu.Unlock()
	return heightCount, hashCount
}

func (tp *taskPool) trackedCountsByKind() (bodyCount int, headerHeightCount int, headerHashCount int) {
	tp.mu.Lock()
	for k := range tp.tracked {
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

func (tp *taskPool) addKindCounter(bodyCounter *uint64, headerHeightCounter *uint64, headerHashCounter *uint64, kind int) {
	switch kind {
	case syncTaskKindHeaderHeight:
		atomic.AddUint64(headerHeightCounter, 1)
	case syncTaskKindHeaderHash:
		atomic.AddUint64(headerHashCounter, 1)
	default:
		atomic.AddUint64(bodyCounter, 1)
	}
}

func (tp *taskPool) stop() {
	if !tp.started {
		return
	}
	tp.stopOnce.Do(func() {
		if tp.stopCh != nil {
			close(tp.stopCh)
		}
		tp.workerWG.Wait()
		tp.taskWG.Wait()
		tp.started = false
	})
}

func (tp *taskPool) resetRuntime() {
	highCap := 1024
	normalCap := 2048
	if tp.queueHigh != nil && cap(tp.queueHigh) > 0 {
		highCap = cap(tp.queueHigh)
	}
	if tp.queueNormal != nil && cap(tp.queueNormal) > 0 {
		normalCap = cap(tp.queueNormal)
	}

	tp.mu.Lock()
	tp.once = sync.Once{}
	tp.stopOnce = sync.Once{}
	tp.workerWG = sync.WaitGroup{}
	tp.taskWG = sync.WaitGroup{}
	tp.started = false
	tp.tracked = make(map[string]struct{})
	tp.queueHigh = make(chan *syncTask, highCap)
	tp.queueNormal = make(chan *syncTask, normalCap)
	tp.stopCh = make(chan struct{})
	tp.enqueued = 0
	tp.dequeued = 0
	tp.succeeded = 0
	tp.failed = 0
	tp.retried = 0
	tp.dropped = 0
	tp.enqueuedBody = 0
	tp.enqueuedHeaderH = 0
	tp.enqueuedHeaderHs = 0
	tp.dequeuedBody = 0
	tp.dequeuedHeaderH = 0
	tp.dequeuedHeaderHs = 0
	tp.succeededBody = 0
	tp.succeededHeaderH = 0
	tp.succeededHeaderHs = 0
	tp.failedBody = 0
	tp.failedHeaderH = 0
	tp.failedHeaderHs = 0
	tp.retriedBody = 0
	tp.retriedHeaderH = 0
	tp.retriedHeaderHs = 0
	tp.droppedBody = 0
	tp.droppedHeaderH = 0
	tp.droppedHeaderHs = 0
	tp.mu.Unlock()
}

func (tp *taskPool) stats() TaskPoolStats {
	tracked := tp.trackedCount()
	trackedBody, trackedHeaderH, trackedHeaderHash := tp.trackedCountsByKind()

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
		PendingHigh:         uint64(len(tp.queueHigh)),
		PendingNormal:       uint64(len(tp.queueNormal)),
		Tracked:             uint64(tracked),
		TrackedBody:         uint64(trackedBody),
		TrackedHeaderH:      uint64(trackedHeaderH),
		TrackedHeaderHash:   uint64(trackedHeaderHash),
		WorkerCount:         uint64(tp.workerCount),
		HighQueueCapacity:   uint64(cap(tp.queueHigh)),
		NormalQueueCapacity: uint64(cap(tp.queueNormal)),
		MaxRetry:            uint64(tp.maxRetry),
	}
}

func (tp *taskPool) metricsPayload() map[string]any {
	stats := tp.stats()
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
