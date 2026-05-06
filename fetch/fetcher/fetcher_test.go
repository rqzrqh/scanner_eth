package fetcher

import (
	"context"
	"errors"
	"math/big"
	"sync/atomic"
	"testing"
	"time"

	"scanner_eth/data"
	nodepkg "scanner_eth/fetch/node"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"gorm.io/gorm"
)

func TestSetEnableInternalTx(t *testing.T) {
	SetEnableInternalTx(true)
	if !enableInternalTx {
		t.Fatal("expected enableInternalTx=true")
	}
	SetEnableInternalTx(false)
	if enableInternalTx {
		t.Fatal("expected enableInternalTx=false")
	}
}

func TestSetFullBlockBodyConcurrency(t *testing.T) {
	oldConcurrency := fullBlockBodyConcurrency
	defer SetFullBlockBodyConcurrency(oldConcurrency)

	SetFullBlockBodyConcurrency(4)
	if fullBlockBodyConcurrency != 4 {
		t.Fatalf("expected concurrency 4, got %d", fullBlockBodyConcurrency)
	}
	SetFullBlockBodyConcurrency(0)
	if fullBlockBodyConcurrency != 1 {
		t.Fatalf("expected non-positive concurrency to normalize to 1, got %d", fullBlockBodyConcurrency)
	}
}

func TestNewFetcherImpl(t *testing.T) {
	bf := NewFetcherImpl(nil)
	if bf == nil {
		t.Fatal("NewFetcherImpl should return non-nil")
	}
	if bf.db != (*gorm.DB)(nil) {
		t.Fatal("expected nil db in fetcher impl")
	}
}

func TestNewMockFetcher(t *testing.T) {
	mockFetcher := NewMockFetcher(nil, nil, nil)
	if mockFetcher == nil {
		t.Fatal("NewMockFetcher should return non-nil")
	}
}

func TestWithFullBlockRPCRetryReselectsNodePerAttempt(t *testing.T) {
	oldInterval := fullBlockRPCRetryInterval
	fullBlockRPCRetryInterval = time.Nanosecond
	defer func() { fullBlockRPCRetryInterval = oldInterval }()

	nm := nodepkg.NewNodeManager([]*ethclient.Client{nil, nil, nil}, 0)
	for id := 0; id < nm.NodeCount(); id++ {
		nm.UpdateNodeChainInfo(id, 10, "0x10")
	}

	var nodeIDs []int
	result := withFullBlockRPCRetry(context.Background(), nm.GetAllValidNodeOperators(10, "0x10"), 10, 7, "test_rpc", func(nodeOp nodepkg.NodeOperator) error {
		nodeIDs = append(nodeIDs, nodeOp.ID())
		if len(nodeIDs) < 3 {
			return errors.New("temporary rpc failure")
		}
		return nil
	})
	if !result.OK {
		t.Fatal("expected retry to eventually succeed")
	}
	if len(nodeIDs) != 3 {
		t.Fatalf("unexpected attempt count: got=%d want=3 ids=%v", len(nodeIDs), nodeIDs)
	}
	if nodeIDs[0] == nodeIDs[1] || nodeIDs[1] == nodeIDs[2] || nodeIDs[0] == nodeIDs[2] {
		t.Fatalf("expected each retry to choose a different node while available, got %v", nodeIDs)
	}
	if len(result.Attempts) != 3 {
		t.Fatalf("unexpected attempt result count: got=%d want=3", len(result.Attempts))
	}
	if result.Attempts[0].Success || result.Attempts[1].Success || !result.Attempts[2].Success {
		t.Fatalf("unexpected attempt success flags: %+v", result.Attempts)
	}
	if result.NodeID != nodeIDs[2] {
		t.Fatalf("unexpected successful node id: got=%d want=%d", result.NodeID, nodeIDs[2])
	}
}

func TestTransTraceAddressToString(t *testing.T) {
	if got := transTraceAddressToString("CALL", nil); got != "call" {
		t.Fatalf("unexpected trace address: %s", got)
	}
	if got := transTraceAddressToString("CREATE2", []uint64{0, 2, 9}); got != "create2_0_2_9" {
		t.Fatalf("unexpected trace address path: %s", got)
	}
}

func TestNormalizeTraceAddress(t *testing.T) {
	if got := normalizeTraceAddress(""); got != "" {
		t.Fatalf("expected empty normalize result, got %q", got)
	}
	if got := normalizeTraceAddress("0x"); got != "" {
		t.Fatalf("expected empty normalize result for 0x, got %q", got)
	}
	if got := normalizeTraceAddress("0x00000000000000000000000000000000000000AA"); got != "0x00000000000000000000000000000000000000aa" {
		t.Fatalf("unexpected normalized address: %q", got)
	}
}

func TestParseTraceBigInt(t *testing.T) {
	if got := parseTraceBigInt(nil); got.Cmp(big.NewInt(0)) != 0 {
		t.Fatalf("expected zero big int for nil input, got %v", got)
	}
	v := hexutil.Big(*big.NewInt(12345))
	if got := parseTraceBigInt(&v); got.Int64() != 12345 {
		t.Fatalf("unexpected parsed big int: %v", got)
	}
}

func TestWalkTxInternalTraceAndParseTxInternal(t *testing.T) {
	rootValue := hexutil.Big(*big.NewInt(7))
	childValue := hexutil.Big(*big.NewInt(0))

	root := &TxInternalJson{
		Type:    "create",
		From:    "0x00000000000000000000000000000000000000aa",
		To:      "0x00000000000000000000000000000000000000bb",
		Value:   &rootValue,
		Gas:     hexutil.Uint64(100),
		GasUsed: hexutil.Uint64(80),
		Input:   "0x11",
		Output:  "0x22",
		Calls: []*TxInternalJson{{
			Type:    "call",
			From:    "0x00000000000000000000000000000000000000bb",
			To:      "0x00000000000000000000000000000000000000cc",
			Value:   &childValue,
			Gas:     hexutil.Uint64(50),
			GasUsed: hexutil.Uint64(40),
			Error:   "revert",
		}},
	}

	list := make([]*data.TxInternal, 0)
	contracts := make([]*data.Contract, 0)
	balanceNative := make(map[string]struct{})
	idx := 0

	walkTxInternalTrace("0xtx", root, nil, 0, &idx, &list, &contracts, balanceNative)
	if len(list) != 2 {
		t.Fatalf("expected 2 internal tx entries, got=%d", len(list))
	}
	if list[0].TraceAddress != "create" || list[1].TraceAddress != "call_0" {
		t.Fatalf("unexpected trace addresses: %s, %s", list[0].TraceAddress, list[1].TraceAddress)
	}
	if len(contracts) != 1 {
		t.Fatalf("expected one created contract, got=%d", len(contracts))
	}
	if _, ok := balanceNative[normalizeTraceAddress(root.From)]; !ok {
		t.Fatal("expected root from address in native balance set")
	}
	if _, ok := balanceNative[normalizeTraceAddress(root.To)]; !ok {
		t.Fatal("expected root to address in native balance set")
	}

	result := parseTxInternal([]*TxInternalTraceResultJson{
		nil,
		{TxHash: "0xskip", Error: "rpc error"},
		{TxHash: "0xempty", Result: nil},
		{TxHash: "0xtx", Result: root},
	}, 100)
	if result == nil {
		t.Fatal("parseTxInternal should not return nil")
	}
	if len(result.InternalTxList) != 2 {
		t.Fatalf("unexpected parsed internal tx size: %d", len(result.InternalTxList))
	}
	if len(result.InternalContractList) != 1 {
		t.Fatalf("unexpected parsed contract size: %d", len(result.InternalContractList))
	}
}

func TestFetchFullBlockWithAttemptsRunsIndependentRPCsConcurrently(t *testing.T) {
	oldEnableInternalTx := enableInternalTx
	oldConcurrency := fullBlockBodyConcurrency
	SetEnableInternalTx(false)
	SetFullBlockBodyConcurrency(3)
	defer func() {
		SetEnableInternalTx(oldEnableInternalTx)
		SetFullBlockBodyConcurrency(oldConcurrency)
	}()

	node := &testFullBlockNode{txDelay: 80 * time.Millisecond, receiptDelay: 80 * time.Millisecond}
	result := FetchFullBlockWithAttempts(context.Background(), []nodepkg.NodeOperator{node}, 1, nil, testFullBlockHeader())

	if result == nil || result.FullBlock == nil {
		t.Fatalf("expected full block result, got %+v", result)
	}
	if node.maxActiveRPC.Load() < 2 {
		t.Fatalf("expected tx and receipt RPCs to overlap, max_active=%d", node.maxActiveRPC.Load())
	}
	if node.internalCalls.Load() != 0 {
		t.Fatalf("internal traces should stay disabled, got calls=%d", node.internalCalls.Load())
	}
}

func TestFetchFullBlockWithAttemptsRespectsConfiguredConcurrency(t *testing.T) {
	oldEnableInternalTx := enableInternalTx
	oldConcurrency := fullBlockBodyConcurrency
	SetEnableInternalTx(false)
	SetFullBlockBodyConcurrency(1)
	defer func() {
		SetEnableInternalTx(oldEnableInternalTx)
		SetFullBlockBodyConcurrency(oldConcurrency)
	}()

	node := &testFullBlockNode{txDelay: 10 * time.Millisecond, receiptDelay: 10 * time.Millisecond}
	result := FetchFullBlockWithAttempts(context.Background(), []nodepkg.NodeOperator{node}, 1, nil, testFullBlockHeader())
	if result == nil || result.FullBlock == nil {
		t.Fatalf("expected full block result, got %+v", result)
	}
	if node.maxActiveRPC.Load() != 1 {
		t.Fatalf("expected configured concurrency to serialize RPCs, max_active=%d", node.maxActiveRPC.Load())
	}
}

func TestFetchFullBlockWithAttemptsRecordsFailedConcurrentAttempt(t *testing.T) {
	oldEnableInternalTx := enableInternalTx
	oldConcurrency := fullBlockBodyConcurrency
	SetEnableInternalTx(false)
	SetFullBlockBodyConcurrency(3)
	defer func() {
		SetEnableInternalTx(oldEnableInternalTx)
		SetFullBlockBodyConcurrency(oldConcurrency)
	}()

	node := &testFullBlockNode{failReceipts: true}
	result := FetchFullBlockWithAttempts(context.Background(), []nodepkg.NodeOperator{node}, 1, nil, testFullBlockHeader())
	if result == nil {
		t.Fatal("expected result wrapper")
	}
	if result.FullBlock != nil {
		t.Fatalf("expected full block fetch to fail, got %+v", result.FullBlock)
	}
	var foundFailedReceipt bool
	for _, attempt := range result.Attempts {
		if attempt.OpName == "FetchReceiptsBatch" && !attempt.Success {
			foundFailedReceipt = true
			break
		}
	}
	if !foundFailedReceipt {
		t.Fatalf("expected failed receipt attempt, got %+v", result.Attempts)
	}
}

type testFullBlockNode struct {
	txDelay       time.Duration
	receiptDelay  time.Duration
	failReceipts  bool
	internalCalls atomic.Int32
	activeRPC     atomic.Int32
	maxActiveRPC  atomic.Int32
}

func (n *testFullBlockNode) ID() int { return 1 }

func (n *testFullBlockNode) FetchBlockHeaderByHeight(context.Context, int, uint64) *BlockHeaderJson {
	return nil
}

func (n *testFullBlockNode) FetchBlockHeaderByHash(context.Context, int, string) *BlockHeaderJson {
	return nil
}

func (n *testFullBlockNode) FetchTransactionsByHashBatch(ctx context.Context, txHashes []string, txs []*TxJson) error {
	done := n.trackRPC()
	defer done()
	if err := waitForTestDelay(ctx, n.txDelay); err != nil {
		return err
	}
	for i, hash := range txHashes {
		txs[i] = testFullBlockTx(hash)
	}
	return nil
}

func (n *testFullBlockNode) FetchReceiptsBatch(ctx context.Context, txHashes []string, receipts []*ethTypes.Receipt) error {
	done := n.trackRPC()
	defer done()
	if err := waitForTestDelay(ctx, n.receiptDelay); err != nil {
		return err
	}
	if n.failReceipts {
		return errors.New("receipt rpc failed")
	}
	for i := range txHashes {
		receipts[i] = &ethTypes.Receipt{Status: 1, GasUsed: 21000}
	}
	return nil
}

func (n *testFullBlockNode) FetchInternalTxTracesByBlockHash(context.Context, int, string, uint64) ([]*TxInternalTraceResultJson, error) {
	n.internalCalls.Add(1)
	return nil, nil
}

func (n *testFullBlockNode) FetchBalanceNative(context.Context, []*data.BalanceNative, uint64) error {
	return nil
}

func (n *testFullBlockNode) FetchErc20BalancesBatch(context.Context, []*data.BalanceErc20, uint64) error {
	return nil
}

func (n *testFullBlockNode) FetchErc1155BalancesBatch(context.Context, []*data.BalanceErc1155, uint64) error {
	return nil
}

func (n *testFullBlockNode) FetchContractErc20(context.Context, *common.Address, uint64) (*data.ContractErc20, error) {
	return &data.ContractErc20{}, nil
}

func (n *testFullBlockNode) FetchContractErc721(context.Context, *common.Address) (*data.ContractErc721, error) {
	return &data.ContractErc721{}, nil
}

func (n *testFullBlockNode) FetchTokenErc721(context.Context, *common.Address, *big.Int) (*data.TokenErc721, error) {
	return &data.TokenErc721{}, nil
}

func (n *testFullBlockNode) trackRPC() func() {
	active := n.activeRPC.Add(1)
	for {
		maxActive := n.maxActiveRPC.Load()
		if active <= maxActive || n.maxActiveRPC.CompareAndSwap(maxActive, active) {
			break
		}
	}
	return func() {
		n.activeRPC.Add(-1)
	}
}

func waitForTestDelay(ctx context.Context, delay time.Duration) error {
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func testFullBlockHeader() *BlockHeaderJson {
	return &BlockHeaderJson{
		BaseFeePerGas:   "0x0",
		Difficulty:      "0x1",
		ExtraData:       "0x",
		GasLimit:        "0x1c9c380",
		GasUsed:         "0x5208",
		Hash:            "0x0000000000000000000000000000000000000000000000000000000000000001",
		Miner:           "0x0000000000000000000000000000000000000001",
		Nonce:           "0x0000000000000000",
		Number:          "0x1",
		ParentHash:      "0x0000000000000000000000000000000000000000000000000000000000000000",
		ReceiptsRoot:    "0x0",
		Sha3Uncles:      "0x0",
		Size:            "0x1",
		StateRoot:       "0x0",
		TimeStamp:       "0x1",
		TotalDifficulty: "0x1",
		TransactionRoot: "0x0",
		Transactions:    []string{"0x00000000000000000000000000000000000000000000000000000000000000aa"},
	}
}

func testFullBlockTx(hash string) *TxJson {
	return &TxJson{
		Hash:             hash,
		From:             "0x00000000000000000000000000000000000000aa",
		To:               "0x00000000000000000000000000000000000000bb",
		Gas:              "0x5208",
		GasPrice:         "0x1",
		Input:            "0x",
		Nonce:            "0x1",
		TransactionIndex: "0x0",
		Type:             "0x0",
		Value:            "0x0",
	}
}
