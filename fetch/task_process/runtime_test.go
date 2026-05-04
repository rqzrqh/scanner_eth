package taskprocess

import (
	"context"
	"math/big"
	"testing"

	"scanner_eth/blocktree"
	"scanner_eth/data"
	fetcherpkg "scanner_eth/fetch/fetcher"
	nodepkg "scanner_eth/fetch/node"
	fetchstore "scanner_eth/fetch/store"

	"github.com/ethereum/go-ethereum/ethclient"
)

type attemptReportingFetcher struct {
	result *fetcherpkg.FullBlockFetchResult
}

func (f attemptReportingFetcher) FetchBlockHeaderByHeight(context.Context, nodepkg.NodeOperator, int, uint64) *fetcherpkg.BlockHeaderJson {
	return nil
}

func (f attemptReportingFetcher) FetchBlockHeaderByHash(context.Context, nodepkg.NodeOperator, int, string) *fetcherpkg.BlockHeaderJson {
	return nil
}

func (f attemptReportingFetcher) FetchFullBlockWithAttempts(context.Context, []nodepkg.NodeOperator, int, *fetcherpkg.BlockHeaderJson) *fetcherpkg.FullBlockFetchResult {
	return f.result
}

func TestSyncNodeDataByHashRecordsActualAttemptNodes(t *testing.T) {
	const (
		hash       = "0x0000000000000000000000000000000000000000000000000000000000000064"
		parentHash = "0x0000000000000000000000000000000000000000000000000000000000000063"
	)
	header := &fetcherpkg.BlockHeaderJson{
		Hash:         hash,
		ParentHash:   parentHash,
		Number:       "0x64",
		GasUsed:      "0x0",
		GasLimit:     "0x0",
		Size:         "0x0",
		Difficulty:   "0x1",
		TimeStamp:    "0x0",
		Transactions: []string{},
	}

	tree := blocktree.NewBlockTree(0)
	tree.Insert(100, hash, parentHash, fetcherpkg.HeaderWeight(header))
	staging := fetchstore.NewStagingStore()
	staging.SetPendingHeader(hash, header)

	nodeManager := nodepkg.NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nodeManager.UpdateNodeChainInfo(0, 100, hash)
	nodeManager.UpdateNodeChainInfo(1, 100, hash)

	deps := RuntimeDeps{
		BlockTree:    tree,
		StagingStore: staging,
		NodeManager:  nodeManager,
		Fetcher: attemptReportingFetcher{result: &fetcherpkg.FullBlockFetchResult{
			FullBlock: &data.FullBlock{
				Block: &data.Block{
					Height:     100,
					Hash:       hash,
					ParentHash: parentHash,
					Difficulty: big.NewInt(1).String(),
				},
				FullTxList: []*data.FullTx{},
				StateSet:   &data.StateSet{},
			},
			Attempts: []fetcherpkg.FullBlockRPCAttempt{
				{OpName: "FetchTransactionsByHashBatch", NodeID: 0, CostMicros: 200, Success: false, Error: "temporary"},
				{OpName: "FetchTransactionsByHashBatch", NodeID: 1, CostMicros: 50, Success: true},
			},
		}},
	}

	if !deps.SyncNodeDataByHash(context.Background(), hash) {
		t.Fatal("expected body sync to succeed")
	}
	if body := staging.GetPendingBody(hash); body == nil || body.StorageFullBlock == nil {
		t.Fatal("expected pending body to be staged")
	}

	snapshot := nodeManager.Snapshot()
	if snapshot.Nodes[0].FailureCount != 1 || snapshot.Nodes[0].SuccessCount != 0 {
		t.Fatalf("expected node 0 to record only failure, got %+v", snapshot.Nodes[0])
	}
	if snapshot.Nodes[1].SuccessCount != 1 || snapshot.Nodes[1].FailureCount != 0 {
		t.Fatalf("expected node 1 to record success, got %+v", snapshot.Nodes[1])
	}
}
