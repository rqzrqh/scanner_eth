package scan

import "testing"

func TestScanWorkerTriggerAndStop(t *testing.T) {
	var nilWorker *Worker
	if nilWorker.IsEnabled() {
		t.Fatal("nil scan worker should report disabled")
	}
	nilWorker.Trigger()
	nilWorker.Stop()

	worker := NewWorker(nil)
	worker.SetEnabled(false)
	worker.Trigger()
	if len(worker.TriggerChan()) != 0 {
		t.Fatal("trigger should not enqueue when scan disabled")
	}

	worker.SetEnabled(true)
	worker.Trigger()
	worker.Trigger()
	if len(worker.TriggerChan()) != 1 {
		t.Fatalf("trigger channel should cap at 1, got=%d", len(worker.TriggerChan()))
	}

	worker.Start()
	worker.Stop()
	worker.Stop()
}

func TestScanWorkerIgnoresTriggersOlderThanLastScanStart(t *testing.T) {
	worker := NewWorker(nil)
	worker.SetEnabled(true)
	worker.recordScanStartedAtMicro(100)

	worker.TriggerAtMicro(99)
	if len(worker.TriggerChan()) != 0 {
		t.Fatalf("old trigger should be ignored, got queue length %d", len(worker.TriggerChan()))
	}
	if worker.IgnoredTriggerEvents() != 1 {
		t.Fatalf("expected one ignored trigger, got=%d", worker.IgnoredTriggerEvents())
	}

	worker.TriggerAtMicro(100)
	if len(worker.TriggerChan()) != 1 {
		t.Fatalf("trigger at scan start should be accepted, got queue length %d", len(worker.TriggerChan()))
	}
	if worker.LastScanStartedAtMicro() != 100 {
		t.Fatalf("last scan started time should be recorded in microseconds, got=%d", worker.LastScanStartedAtMicro())
	}
}

func TestScanWorkerIgnoresBufferedOldTriggerOnConsume(t *testing.T) {
	worker := NewWorker(nil)
	worker.SetEnabled(true)
	worker.TriggerAtMicro(100)

	worker.recordScanStartedAtMicro(101)
	triggerAtMicro := <-worker.TriggerChan()
	if !worker.shouldIgnoreTrigger(triggerAtMicro) {
		t.Fatal("buffered trigger older than latest scan start should be ignored")
	}
}
