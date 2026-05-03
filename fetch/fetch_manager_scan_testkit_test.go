package fetch

import (
	"context"
	"scanner_eth/data"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/ethclient"
	fetcherpkg "scanner_eth/fetch/fetcher"

	nodepkg "scanner_eth/fetch/node"
)

func attachTestHeaderManager(fm *FetchManager) {
	if fm == nil {
		return
	}
	if headerManager := fm.runtimeHeaderManager(); headerManager != nil {
		headerManager.Stop()
	}
	if rt := fm.currentRuntime(); rt != nil {
		rt.headerManager = fm.newHeaderManager()
	}
}

func runScanAndWait(t *testing.T, fm *FetchManager) {
	t.Helper()
	scanWorker := fm.runtimeScanWorker()
	scanFlow := fm.runtimeScanFlow()
	taskPool := fm.runtimeTaskPool()
	if scanWorker == nil {
		t.Fatal("scanWorker is nil")
	}
	scanWorker.SetEnabled(true)
	if scanFlow == nil {
		t.Fatal("scanFlow is nil")
	}
	if taskPool == nil {
		t.Fatal("taskPool is nil")
	}
	scanFlow.RunScanCycle(context.Background())

	deadline := time.Now().Add(2 * time.Second)
	quietSince := time.Time{}
	for time.Now().Before(deadline) {
		drainedTrigger := false
		for {
			select {
			case <-scanWorker.TriggerChan():
				drainedTrigger = true
				scanFlow.RunScanCycle(context.Background())
			default:
				goto checkIdle
			}
		}

	checkIdle:
		taskStats := taskPool.Stats()
		taskSyncing := taskStats.PendingHigh > 0 || taskStats.PendingNormal > 0 || taskPool.TrackedCount() > 0
		storeWorker := fm.runtimeStoreWorker()
		storeBusy := storeWorker != nil && !storeWorker.IsIdle()
		if !taskSyncing && !storeBusy && !drainedTrigger {
			hasMoreWork := len(scanFlow.GetExpandTreeTargets()) > 0 ||
				len(scanFlow.GetFillTreeTargets()) > 0
			if hasMoreWork {
				scanFlow.RunScanCycle(context.Background())
				quietSince = time.Time{}
				waitFetchManagerRuntimeQuiet(fm, 20*time.Millisecond)
				continue
			}
			if quietSince.IsZero() {
				quietSince = time.Now()
			} else if time.Since(quietSince) >= 100*time.Millisecond {
				return
			}
		} else {
			quietSince = time.Time{}
		}
		waitFetchManagerRuntimeQuiet(fm, 20*time.Millisecond)
	}

	t.Fatalf("scan stages did not finish before timeout")
}

func waitFetchManagerRuntimeQuiet(fm *FetchManager, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	if taskPool := fm.runtimeTaskPool(); taskPool != nil {
		_ = taskPool.WaitIdle(ctx, 0)
	}
	if storeWorker := fm.runtimeStoreWorker(); storeWorker != nil {
		_ = storeWorker.WaitIdle(ctx, 0)
	}
}

func makeHeader(height uint64, hash string, parent string) *fetcherpkg.BlockHeaderJson {
	return &fetcherpkg.BlockHeaderJson{
		Number:       hexutil.EncodeUint64(height),
		Hash:         hash,
		ParentHash:   parent,
		Difficulty:   "0x1",
		Transactions: []string{},
	}
}

func makeMinimalFullBlock(height uint64, hash string, parent string) *data.FullBlock {
	return &data.FullBlock{
		Block: &data.Block{Height: height, Hash: hash, ParentHash: parent},
		StateSet: &data.StateSet{
			ContractList:       []*data.Contract{},
			ContractErc20List:  []*data.ContractErc20{},
			ContractErc721List: []*data.ContractErc721{},
			BalanceNativeList:  []*data.BalanceNative{},
			BalanceErc20List:   []*data.BalanceErc20{},
			BalanceErc1155List: []*data.BalanceErc1155{},
			TokenErc721List:    []*data.TokenErc721{},
		},
	}
}

func setNodeLatestHeight(fm *FetchManager, height uint64) {
	if fm == nil {
		return
	}
	if fm.nodeManager == nil {
		fm.nodeManager = nodepkg.NewNodeManager([]*ethclient.Client{nil}, 0)
	}
	fm.nodeManager.ResetNodeRemoteTip(0)
	fm.nodeManager.UpdateNodeChainInfo(0, height, "")
}

func setTestScanStartHeight(fm *FetchManager, height uint64) {
	if fm == nil {
		return
	}
	fm.scanConfig.StartHeight = height
	if scanFlow := fm.runtimeScanFlow(); scanFlow != nil {
		scanFlow.BindRuntimeDeps()
	}
}

func setTestFetcher(fm *FetchManager, fetcher fetcherpkg.Fetcher) {
	if fm == nil {
		return
	}
	if fetcher == nil {
		fm.fetcher = nil
		return
	}
	fm.fetcher = fetcher
	if scanFlow := fm.runtimeScanFlow(); scanFlow != nil {
		scanFlow.BindRuntimeDeps()
	}
}
