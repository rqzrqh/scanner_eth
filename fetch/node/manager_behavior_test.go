package node

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethclient"
)

func TestNodeStateGetChainInfoAndNodeManagerMethods(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	if got := nm.NodeCount(); got != 2 {
		t.Fatalf("unexpected node count: %d", got)
	}

	nm.UpdateNodeChainInfo(0, 12, "0x12")
	node := nm.Node(0)
	if node == nil {
		t.Fatal("expected node 0")
	}
	if got := node.GetChainInfo(); got != 12 {
		t.Fatalf("unexpected node chain info height: %d", got)
	}

	// Make all nodes unavailable for target height.
	nm.UpdateNodeState(0, 0, false)
	nm.UpdateNodeState(1, 0, false)
	if _, _, err := nm.GetBestNode(1); err == nil {
		t.Fatal("expected no valid node error")
	}

	nm.UpdateNodeState(1, 5, true)
	nm.UpdateNodeChainInfo(1, 20, "0x20")
	id, _, err := nm.GetBestNode(15)
	if err != nil {
		t.Fatalf("expected best node, got error: %v", err)
	}
	if id != 1 {
		t.Fatalf("unexpected best node id: %d", id)
	}

	// Invalid ids should be no-op and not panic.
	nm.UpdateNodeChainInfo(-1, 1, "0x1")
	nm.UpdateNodeChainInfo(100, 1, "0x1")
	nm.UpdateNodeState(-1, 1, true)
	nm.UpdateNodeState(100, 1, true)
	nm.UpdateNodeState(-1, 0, false)
	nm.UpdateNodeState(100, 0, false)
}
