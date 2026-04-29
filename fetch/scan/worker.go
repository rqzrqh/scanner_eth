package scan

import (
	"context"
	"sync"
	"sync/atomic"
	"time"
)

type Worker struct {
	flow *Flow

	enabled   atomic.Bool
	triggerCh chan struct{}

	mu     sync.Mutex
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewWorker(flow *Flow) *Worker {
	return &Worker{
		flow:      flow,
		triggerCh: make(chan struct{}, 1),
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

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-loopCtx.Done():
			return
		case <-ticker.C:
			if w.flow != nil {
				w.flow.RunScanCycle(loopCtx)
			}
		case <-w.triggerCh:
			if w.flow != nil {
				w.flow.RunScanCycle(loopCtx)
			}
		}
	}
}

func (w *Worker) Trigger() {
	if w == nil || w.triggerCh == nil || !w.IsEnabled() {
		return
	}

	select {
	case w.triggerCh <- struct{}{}:
	default:
	}
}

func (w *Worker) TriggerChan() <-chan struct{} {
	if w == nil {
		return nil
	}
	return w.triggerCh
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
