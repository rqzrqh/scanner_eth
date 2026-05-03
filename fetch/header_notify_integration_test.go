package fetch

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"

	headernotify "scanner_eth/fetch/header_notify"
)

func TestStartHeaderNotifiersConsumerIgnoresRegressiveRemoteHeight(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 8)
	attachTestHeaderManager(fm)
	// Disable scan so this test isolates remote tip tracking from insertHeader side effects.
	mustTestScanWorker(t, fm).SetEnabled(false)

	mustTestHeaderManager(t, fm).StartWithChannel(ctx, updates)

	send := func(height uint64, hash string) {
		t.Helper()
		updates <- &headernotify.RemoteChainUpdate{
			NodeId:    0,
			Height:    height,
			BlockHash: hash,
			Header: &headernotify.RemoteHeader{
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
		if node := fm.nodeManager.Node(0); fm.nodeManager.GetLatestHeight() == 31 && node != nil && node.GetChainInfo() == 31 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected regressive height notification to be ignored and tip to reach 31")
}

func TestStartHeaderNotifiersEnqueuesConnectedHeaderHashCandidate(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 2)
	attachTestHeaderManager(fm)
	mustTestScanWorker(t, fm).SetEnabled(true)
	mustTestBlockTree(t, fm).Insert(14, "0x0e", "", 1)
	beforeHeaderHash := taskPool.Stats().EnqueuedHeaderHash

	mustTestHeaderManager(t, fm).StartWithChannel(ctx, updates)

	updates <- &headernotify.RemoteChainUpdate{
		NodeId:    0,
		Height:    15,
		BlockHash: "0x0f",
		Header: &headernotify.RemoteHeader{
			Hash:       "0x0f",
			ParentHash: "0x0e",
			Number:     "0xf",
			Difficulty: "0x2",
		},
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if taskPool.Stats().EnqueuedHeaderHash > beforeHeaderHash {
			node := fm.nodeManager.Node(0)
			if node == nil {
				t.Fatal("node 0 is nil")
			}
			if h := node.GetChainInfo(); h != 15 {
				t.Fatalf("node height not updated, got=%d", h)
			}
			if len(mustTestScanWorker(t, fm).TriggerChan()) == 0 {
				t.Fatal("expected scan trigger after remote update")
			}
			if n := mustTestBlockTree(t, fm).Get("0x0f"); n != nil {
				t.Fatal("remote update must enqueue header-by-hash instead of inserting blocktree directly")
			}
			if header := getTestNodeBlockHeader(t, fm, "0x0f"); header != nil {
				t.Fatalf("expected remote update header not to be cached in pending store, got %+v", header)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("header from remote update did not enqueue header-by-hash")
}

func TestStartHeaderNotifiersRemoteUpdateRejectsUnlinkedHeaderCandidate(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	taskPool := mustTestTaskPool(t, fm)
	t.Cleanup(func() { taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 1)
	attachTestHeaderManager(fm)
	mustTestScanWorker(t, fm).SetEnabled(true)
	beforeHeaderHash := taskPool.Stats().EnqueuedHeaderHash

	mustTestHeaderManager(t, fm).StartWithChannel(ctx, updates)

	updates <- &headernotify.RemoteChainUpdate{
		NodeId:    0,
		Height:    100,
		BlockHash: "0xabc1",
		Header: &headernotify.RemoteHeader{
			Hash:       "0xabc1",
			ParentHash: "0xwrong_parent",
			Number:     "0x64",
			Difficulty: "0x9999",
		},
	}

	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		if len(mustTestScanWorker(t, fm).TriggerChan()) > 0 {
			if n := mustTestBlockTree(t, fm).Get("0xabc1"); n != nil {
				t.Fatal("unlinked remote header candidate must not be inserted into blocktree")
			}
			if after := taskPool.Stats().EnqueuedHeaderHash; after != beforeHeaderHash {
				t.Fatalf("unlinked remote header candidate must not enqueue header-by-hash, before=%d after=%d", beforeHeaderHash, after)
			}
			if header := getTestNodeBlockHeader(t, fm, "0xabc1"); header != nil {
				t.Fatalf("expected remote update header not to be cached in pending store, got %+v", header)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("expected scan trigger after remote update")
}

func TestHeaderNotifierRunNilSafe(t *testing.T) {
	ctx := context.Background()
	var nilNotifier *headernotify.HeaderNotifier
	nilNotifier.Run(ctx, nil, nil)

	ch := make(chan *headernotify.RemoteChainUpdate, 1)
	(&headernotify.HeaderNotifier{}).Run(ctx, ch, nil)
	headernotify.NewHeaderNotifier(0, nil).Run(ctx, ch, nil)
	close(ch)
}
