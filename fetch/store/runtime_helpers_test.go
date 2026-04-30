package store

import (
	"testing"

	"scanner_eth/model"
)

func TestStoreWorkerHelpers(t *testing.T) {
	full := &StorageFullBlock{
		TxList:                   []model.Tx{{}, {}},
		TxInternalList:           []model.TxInternal{{}},
		EventLogList:             []model.EventLog{{}},
		EventErc20TransferList:   []model.EventErc20Transfer{{}},
		EventErc721TransferList:  []model.EventErc721Transfer{{}},
		EventErc1155TransferList: []model.EventErc1155Transfer{{}},
		ContractList:             []model.Contract{{}},
	}
	full.assignBlockID(42)
	if full.TxList[0].BlockId != 42 || full.TxList[1].BlockId != 42 {
		t.Fatal("assignStorageBlockID should set tx block ids")
	}
	if full.TxInternalList[0].BlockId != 42 || full.EventLogList[0].BlockId != 42 || full.ContractList[0].BlockId != 42 {
		t.Fatal("assignStorageBlockID should set all nested block ids")
	}

	vals := []int{1, 2, 3, 4, 5}
	iface := ToInterfaceSlice(vals)
	if len(iface) != 5 {
		t.Fatalf("unexpected interface slice length: %d", len(iface))
	}
	if iface[0].(int) != 1 || iface[4].(int) != 5 {
		t.Fatal("toInterfaceSlice values mismatch")
	}

	DefaultRuntime().ResetForTest(128, make(chan *Task, 1), make(chan *Complete, 1))
	tasks := SplitTasks(Tx, iface, 2, DefaultRuntime().NextTaskID, 100)
	if len(tasks) != 3 {
		t.Fatalf("expected 3 split tasks, got=%d", len(tasks))
	}
	if tasks[0].TaskID != 1 || tasks[1].TaskID != 2 || tasks[2].TaskID != 3 {
		t.Fatalf("unexpected task ids: %d,%d,%d", tasks[0].TaskID, tasks[1].TaskID, tasks[2].TaskID)
	}
	if len(tasks[0].Data) != 2 || len(tasks[1].Data) != 2 || len(tasks[2].Data) != 1 {
		t.Fatal("split task batch sizes mismatch")
	}
}

func TestInitStoreAndNewStoreWorker(t *testing.T) {
	// Ensure InitStore handles non-positive inputs and does not panic.
	DefaultRuntime().Init(nil, 0, 0)
	if DefaultRuntime().BatchSize() != 128 {
		t.Fatalf("expected default batchSize=128, got=%d", DefaultRuntime().BatchSize())
	}

	taskCh := make(chan *Task, 1)
	completeCh := make(chan *Complete, 1)
	sw := NewWorker(7, nil, taskCh, completeCh)
	if sw == nil {
		t.Fatal("NewStoreWorker should return non-nil")
	}
	if sw.ID != 7 || sw.StoreTaskChannel != taskCh || sw.StoreCompleteChannel != completeCh {
		t.Fatal("NewStoreWorker fields not initialized as expected")
	}
}
