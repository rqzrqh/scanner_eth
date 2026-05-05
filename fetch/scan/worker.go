package scan

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

const scanWorkerPollInterval = 5 * time.Second

type Worker struct {
	flow *Flow

	enabled   atomic.Bool
	triggerCh chan int64

	lastScanStartedAtMicro int64
	ignoredTriggerEvents   uint64

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorker(flow *Flow) *Worker {
	return &Worker{
		flow:      flow,
		triggerCh: make(chan int64, 1),
	}
}

func (w *Worker) Start() {
	if w == nil {
		return
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if w.cancel != nil {
		w.cancel()
		w.wg.Wait()
	}

	loopCtx, cancel := context.WithCancel(context.Background())
	w.cancel = cancel
	w.wg.Add(1)
	go w.run(loopCtx)
}

func (w *Worker) run(loopCtx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(scanWorkerPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			w.runScanCycle(loopCtx)
		case triggerAtMicro := <-w.triggerCh:
			if w.shouldIgnoreTrigger(triggerAtMicro) {
				continue
			}
			w.runScanCycle(loopCtx)
		}
	}
}

func (w *Worker) runScanCycle(ctx context.Context) {
	if w == nil || w.flow == nil || !w.IsEnabled() {
		return
	}
	w.recordScanStartedAtMicro(time.Now().UnixMicro())
	w.flow.RunScanCycle(ctx)
}

func (w *Worker) Trigger() {
	if w == nil {
		return
	}
	w.TriggerAtMicro(time.Now().UnixMicro())
}

func (w *Worker) TriggerAtMicro(triggerAtMicro int64) {
	if w == nil || w.triggerCh == nil || !w.IsEnabled() {
		return
	}
	if triggerAtMicro <= 0 {
		triggerAtMicro = time.Now().UnixMicro()
	}
	if w.shouldIgnoreTrigger(triggerAtMicro) {
		return
	}

	select {
	case w.triggerCh <- triggerAtMicro:
	default:
	}
}

func (w *Worker) TriggerChan() <-chan int64 {
	if w == nil {
		return nil
	}
	return w.triggerCh
}

func (w *Worker) LastScanStartedAtMicro() int64 {
	if w == nil {
		return 0
	}
	return atomic.LoadInt64(&w.lastScanStartedAtMicro)
}

func (w *Worker) IgnoredTriggerEvents() uint64 {
	if w == nil {
		return 0
	}
	return atomic.LoadUint64(&w.ignoredTriggerEvents)
}

func (w *Worker) recordScanStartedAtMicro(startedAtMicro int64) {
	if w == nil {
		return
	}
	atomic.StoreInt64(&w.lastScanStartedAtMicro, startedAtMicro)
}

func (w *Worker) shouldIgnoreTrigger(triggerAtMicro int64) bool {
	if w == nil {
		return true
	}
	lastScanStartedAtMicro := atomic.LoadInt64(&w.lastScanStartedAtMicro)
	if triggerAtMicro > 0 && lastScanStartedAtMicro > 0 && triggerAtMicro < lastScanStartedAtMicro {
		atomic.AddUint64(&w.ignoredTriggerEvents, 1)
		return true
	}
	return false
}

func (w *Worker) IsEnabled() bool {
	if w == nil {
		return false
	}
	return w.enabled.Load()
}

func (w *Worker) SetEnabled(enabled bool) {
	if w == nil {
		return
	}
	w.enabled.Store(enabled)
}

func (w *Worker) Stop() {
	if w == nil {
		return
	}

	w.mu.Lock()
	cancel := w.cancel
	w.cancel = nil
	w.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	w.wg.Wait()
}
