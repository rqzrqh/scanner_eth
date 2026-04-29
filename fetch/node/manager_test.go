package node

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"
)

func TestNodeManagerResetRemoteChainTips(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0xa")
	nm.UpdateNodeChainInfo(1, 200, "0xb")
	if got := nm.GetLatestHeight(); got != 200 {
		t.Fatalf("setup latest height: got=%d want=200", got)
	}

	nm.ResetRemoteChainTips()
	if got := nm.GetLatestHeight(); got != 0 {
		t.Fatalf("after reset: got=%d want=0", got)
	}

	nm.UpdateNodeChainInfo(0, 50, "0x32")
	if got := nm.GetLatestHeight(); got != 50 {
		t.Fatalf("after repopulate: got=%d want=50", got)
	}
}
