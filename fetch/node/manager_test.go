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

func TestNodeManagerSnapshotReportsPerNodeState(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0xa")
	nm.UpdateNodeChainInfo(1, 90, "0xb")
	nm.UpdateNodeState(1, 42, false)

	snapshot := nm.Snapshot()
	if snapshot.NodeCount != 2 || snapshot.ReadyCount != 1 || snapshot.LatestHeight != 100 {
		t.Fatalf("unexpected manager snapshot: %+v", snapshot)
	}
	if len(snapshot.Nodes) != 2 {
		t.Fatalf("unexpected node snapshot count: %d", len(snapshot.Nodes))
	}
	if snapshot.Nodes[1].Ready || snapshot.Nodes[1].Delay != 42 || snapshot.Nodes[1].RemoteHeight != 90 {
		t.Fatalf("unexpected node 1 snapshot: %+v", snapshot.Nodes[1])
	}
}
