package scan

import "context"

type FlowWorker struct {
	core *Worker
	flow *Flow
}

func NewFlowWorker(flow *Flow) *FlowWorker {
	var runner Runner
	if flow != nil {
		runner = RunnerFunc(func(ctx context.Context) {
			flow.ScanEvents(ctx)
		})
	}
	return &FlowWorker{
		core: NewWorker(runner),
		flow: flow,
	}
}

func (w *FlowWorker) Start() {
	if w == nil || w.core == nil {
		return
	}
	w.core.Start()
}

func (w *FlowWorker) Flow() *Flow {
	if w == nil {
		return nil
	}
	return w.flow
}

func (w *FlowWorker) Trigger() {
	if w == nil || w.core == nil {
		return
	}
	w.core.Trigger()
}

func (w *FlowWorker) TriggerChan() <-chan struct{} {
	if w == nil || w.core == nil {
		return nil
	}
	return w.core.TriggerChan()
}

func (w *FlowWorker) IsEnabled() bool {
	if w == nil || w.core == nil {
		return false
	}
	return w.core.IsEnabled()
}

func (w *FlowWorker) SetEnabled(enabled bool) {
	if w == nil || w.core == nil {
		return
	}
	w.core.SetEnabled(enabled)
}

func (w *FlowWorker) Stop() {
	if w == nil || w.core == nil {
		return
	}
	w.core.Stop()
}
