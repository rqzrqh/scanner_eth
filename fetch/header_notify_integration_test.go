package fetch

import (
	"context"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"

	fetcherpkg "scanner_eth/fetch/fetcher"
	headernotify "scanner_eth/fetch/header_notify"
)

func TestStartHeaderNotifiersConsumerIgnoresRegressiveRemoteHeight(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 8)
	fm.hns = []*headernotify.HeaderNotifier{headernotify.NewHeaderNotifier(0, nil)}
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

func TestStartHeaderNotifiersAndConsumerAppliesRemoteUpdate(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 2)
	fm.hns = []*headernotify.HeaderNotifier{headernotify.NewHeaderNotifier(0, nil)}
	attachTestHeaderManager(fm)
	mustTestScanWorker(t, fm).SetEnabled(true)

	// New-head path now calls eth_getBlockByHash; mock the full header for 0x0f.
	setTestBlockFetcher(fm, &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, _ fetcherpkg.NodeOperator, _ int, hash string) *BlockHeaderJson {
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
	})

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
		n := fm.blockTree.Get("0x0f")
		if n != nil {
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
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("header from remote update was not inserted into blocktree")
}

// FormalVerification.md §3.7.1: newHeads only carries a slim RemoteHeader; the consumer must
// use fetchAndInsertHeaderByHashImmediate. If the tree matched RemoteHeader's parent, that
// would indicate the slim path was used for insert; we assert the parent comes from the RPC response.
func TestStartHeaderNotifiersRemoteUpdateUsesBlockFetcherNotSlimHeader(t *testing.T) {
	fm := newTestFetchManager(t, 2)
	t.Cleanup(func() { fm.taskPool.Stop() })

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	t.Cleanup(func() {
		if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
			headerManager.Stop()
		}
	})

	updates := make(chan *headernotify.RemoteChainUpdate, 1)
	fm.hns = []*headernotify.HeaderNotifier{headernotify.NewHeaderNotifier(0, nil)}
	attachTestHeaderManager(fm)
	mustTestScanWorker(t, fm).SetEnabled(true)

	var fetchByHashCalls int
	setTestBlockFetcher(fm, &mockBlockFetcher{
		fetchByHashFn: func(_ context.Context, _ fetcherpkg.NodeOperator, _ int, hash string) *BlockHeaderJson {
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
	})

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
	var nilNotifier *headernotify.HeaderNotifier
	nilNotifier.Run(ctx, nil, nil)

	ch := make(chan *headernotify.RemoteChainUpdate, 1)
	(&headernotify.HeaderNotifier{}).Run(ctx, ch, nil)
	headernotify.NewHeaderNotifier(0, nil).Run(ctx, ch, nil)
	close(ch)
}
