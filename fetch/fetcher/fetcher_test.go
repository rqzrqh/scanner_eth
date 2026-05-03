package fetcher

import (
	"math/big"
	"testing"

	"scanner_eth/data"

	"github.com/ethereum/go-ethereum/common/hexutil"
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
