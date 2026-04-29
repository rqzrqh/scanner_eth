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
