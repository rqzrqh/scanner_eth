package fetch

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"scanner_eth/model"
)

func TestFetchManagerRunStopNilSafe(t *testing.T) {
	var nilFM *FetchManager
	nilFM.Run()

	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	// No election set: Run should still be safe and return.
	fm.Run()
	fm.Stop()
}

func TestStartHeaderNotifiersConsumerIgnoresRegressiveRemoteHeight(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { fm.stopHeaderNotifiersAndConsumer() })

	updates := make(chan *RemoteChainUpdate, 8)
	fm.hns = []*HeaderNotifier{NewHeaderNotifier(0, nil)}
	// Disable scan so this test isolates remote tip tracking from insertHeader side effects.
	fm.scanEnabled.Store(false)

	fm.startHeaderNotifiersWithChannel(ctx, updates)

	send := func(height uint64, hash string) {
		t.Helper()
		updates <- &RemoteChainUpdate{
			NodeId:    0,
			Height:    height,
			BlockHash: hash,
			Header: &RemoteHeader{
				Hash:       hash,
				ParentHash: "",
				Number:     hexutil.EncodeUint64(height),
				Difficulty: "0x1",
			},
		}
	}

	send(30, "0x1e")
	send(20, "0x14")
	send(31, "0x1f")

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if fm.nodeManager.GetLatestHeight() == 31 && fm.nodeManager.nodes[0].GetChainInfo() == 31 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected regressive height notification to be ignored and tip to reach 31")
}

func TestStartHeaderNotifiersAndConsumerAppliesRemoteUpdate(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { fm.stopHeaderNotifiersAndConsumer() })

	updates := make(chan *RemoteChainUpdate, 2)
	fm.hns = []*HeaderNotifier{NewHeaderNotifier(0, nil)}
	fm.scanEnabled.Store(true)

	// New-head path now calls eth_getBlockByHash; mock the full header for 0x0f.
	fm.blockFetcher = &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, _ *NodeOperator, _ int, hash string) *BlockHeaderJson {
			if normalizeHash(hash) == "0x0f" {
				return &BlockHeaderJson{
					Number:       "0xf",
					Hash:         "0x0f",
					ParentHash:   "0x0e",
					Difficulty:   "0x2",
					Transactions: []string{},
				}
			}
			return nil
		},
	}

	fm.startHeaderNotifiersWithChannel(ctx, updates)

	updates <- &RemoteChainUpdate{
		NodeId:    0,
		Height:    15,
		BlockHash: "0x0f",
		Header: &RemoteHeader{
			Hash:       "0x0f",
			ParentHash: "0x0e",
			Number:     "0xf",
			Difficulty: "0x2",
		},
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n := fm.blockTree.Get("0x0f")
		if n != nil {
			if h := fm.nodeManager.nodes[0].GetChainInfo(); h != 15 {
				t.Fatalf("node height not updated, got=%d", h)
			}
			if len(fm.scanTriggerCh) == 0 {
				t.Fatal("expected scan trigger after remote update")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("header from remote update was not inserted into blocktree")
}

// FormalVerification.md §3.7.1: newHeads only carries a slim RemoteHeader; the consumer must
// use fetchAndInsertHeaderByHashImmediate. If the tree matched RemoteHeader’s parent, that
// would indicate the slim path was used for insert; we assert the parent comes from the RPC response.
func TestStartHeaderNotifiersRemoteUpdateUsesBlockFetcherNotSlimHeader(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() { fm.stopHeaderNotifiersAndConsumer() })

	updates := make(chan *RemoteChainUpdate, 1)
	fm.hns = []*HeaderNotifier{NewHeaderNotifier(0, nil)}
	fm.scanEnabled.Store(true)

	var fetchByHashCalls int
	fm.blockFetcher = &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, _ *NodeOperator, _ int, hash string) *BlockHeaderJson {
			if normalizeHash(hash) != "0xabc1" {
				return nil
			}
			fetchByHashCalls++
			return &BlockHeaderJson{
				Number:       "0x64",
				Hash:         "0xabc1",
				ParentHash:   "0xrpc_parent",
				Difficulty:   "0x3",
				Transactions: []string{"0xtx1"},
			}
		},
	}

	fm.startHeaderNotifiersWithChannel(ctx, updates)

	updates <- &RemoteChainUpdate{
		NodeId:    0,
		Height:    100,
		BlockHash: "0xabc1",
		Header: &RemoteHeader{
			Hash:       "0xabc1",
			ParentHash: "0xwrong_parent",
			Number:     "0x64",
			Difficulty: "0x9999",
		},
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		n := fm.blockTree.Get("0xabc1")
		if n != nil {
			if fetchByHashCalls == 0 {
				t.Fatal("expected at least one FetchBlockHeaderByHash from newHead consumer")
			}
			if n.ParentKey != "0xrpc_parent" {
				t.Fatalf("expected parent from BlockFetcher (0xrpc_parent), got %q (slim had 0xwrong_parent)", n.ParentKey)
			}
			if n.Weight == 0 {
				t.Fatal("expected non-zero weight from rpc difficulty 0x3")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("block never appeared in tree; BlockFetcher may not be used on new head path")
}

func TestHeaderNotifierRunNilSafe(t *testing.T) {
	ctx := context.Background()
	var nilNotifier *HeaderNotifier
	nilNotifier.Run(ctx, nil, nil)

	ch := make(chan *RemoteChainUpdate, 1)
	(&HeaderNotifier{}).Run(ctx, ch, nil)
	(&HeaderNotifier{client: nil}).Run(ctx, ch, nil)
	close(ch)
}

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
	assignStorageBlockID(full, 42)
	if full.TxList[0].BlockId != 42 || full.TxList[1].BlockId != 42 {
		t.Fatal("assignStorageBlockID should set tx block ids")
	}
	if full.TxInternalList[0].BlockId != 42 || full.EventLogList[0].BlockId != 42 || full.ContractList[0].BlockId != 42 {
		t.Fatal("assignStorageBlockID should set all nested block ids")
	}

	vals := []int{1, 2, 3, 4, 5}
	iface := toInterfaceSlice(vals)
	if len(iface) != 5 {
		t.Fatalf("unexpected interface slice length: %d", len(iface))
	}
	if iface[0].(int) != 1 || iface[4].(int) != 5 {
		t.Fatal("toInterfaceSlice values mismatch")
	}

	taskCounter = 0
	tasks := splitTask(Tx, iface, 2, 100)
	if len(tasks) != 3 {
		t.Fatalf("expected 3 split tasks, got=%d", len(tasks))
	}
	if tasks[0].taskID != 1 || tasks[1].taskID != 2 || tasks[2].taskID != 3 {
		t.Fatalf("unexpected task ids: %d,%d,%d", tasks[0].taskID, tasks[1].taskID, tasks[2].taskID)
	}
	if len(tasks[0].data) != 2 || len(tasks[1].data) != 2 || len(tasks[2].data) != 1 {
		t.Fatal("split task batch sizes mismatch")
	}
}

func TestInitStoreAndNewStoreWorker(t *testing.T) {
	// Ensure InitStore handles non-positive inputs and does not panic.
	InitStore(nil, 0, 0)
	if batchSize != 128 {
		t.Fatalf("expected default batchSize=128, got=%d", batchSize)
	}

	taskCh := make(chan *StoreTask, 1)
	completeCh := make(chan *StoreComplete, 1)
	sw := NewStoreWorker(7, nil, taskCh, completeCh)
	if sw == nil {
		t.Fatal("NewStoreWorker should return non-nil")
	}
	if sw.id != 7 || sw.storeTaskChannel != taskCh || sw.storeCompleteChannel != completeCh {
		t.Fatal("NewStoreWorker fields not initialized as expected")
	}
}
