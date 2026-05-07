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

func TestNodeManagerScorePrefersHealthyLowLatencyNode(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0x64")
	nm.UpdateNodeChainInfo(1, 100, "0x64")

	nm.RecordNodeResult(0, 300_000, true)
	nm.RecordNodeResult(1, 50_000, true)
	id, _, err := nm.GetBestNode(100)
	if err != nil {
		t.Fatalf("expected best node, got error: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected faster node 1, got %d", id)
	}

	nm.RecordNodeResult(1, 10_000, false)
	nm.UpdateNodeChainInfo(1, 101, "0x65")
	id, _, err = nm.GetBestNode(100)
	if err != nil {
		t.Fatalf("expected fallback node, got error: %v", err)
	}
	if id != 0 {
		t.Fatalf("expected node 0 while node 1 cools down after tip update, got %d", id)
	}

	snapshot := nm.Snapshot()
	if snapshot.Nodes[1].FailureCount != 1 || snapshot.Nodes[1].ConsecutiveFailures != 1 || snapshot.Nodes[1].CooldownUntilUnix == 0 {
		t.Fatalf("unexpected failed node snapshot: %+v", snapshot.Nodes[1])
	}
}

func TestGetAllValidNodeOperatorsFiltersAndSortsByScore(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0x64")
	nm.UpdateNodeChainInfo(1, 90, "0x5a")
	nm.UpdateNodeChainInfo(2, 100, "0x64")
	nm.RecordNodeResult(0, 200_000, true)
	nm.RecordNodeResult(2, 20_000, true)

	if got := nm.GetAllValidNodeOperators(100, ""); got != nil {
		t.Fatalf("expected nil operators for empty hash, got %d", len(got))
	}
	ops := nm.GetAllValidNodeOperators(100, "0x64")
	if len(ops) != 2 {
		t.Fatalf("unexpected valid operators: got=%d want=2", len(ops))
	}
	if ops[0].ID() != 2 || ops[1].ID() != 0 {
		t.Fatalf("unexpected operator order: got=%d,%d", ops[0].ID(), ops[1].ID())
	}
}

func TestNodeManagerBalancesCandidatesWithinScoreTolerance(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0x64")
	nm.UpdateNodeChainInfo(1, 100, "0x64")
	nm.RecordNodeResult(0, 1_000, true)

	ops := nm.GetAllValidNodeOperators(100, "0x64")
	if len(ops) != 2 || ops[0].ID() != 0 || ops[1].ID() != 1 {
		t.Fatalf("unexpected first balanced order: %v", operatorIDsForTest(ops))
	}
	ops = nm.GetAllValidNodeOperators(100, "0x64")
	if len(ops) != 2 || ops[0].ID() != 1 || ops[1].ID() != 0 {
		t.Fatalf("unexpected rotated balanced order: %v", operatorIDsForTest(ops))
	}
}

func TestGetBestNodeMarksInflightAndBalancesConcurrentSelection(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0x64")
	nm.UpdateNodeChainInfo(1, 100, "0x64")
	nm.RecordNodeResult(0, 1_000, true)

	firstID, _, err := nm.GetBestNode(100)
	if err != nil {
		t.Fatalf("expected first node, got error: %v", err)
	}
	secondID, _, err := nm.GetBestNode(100)
	if err != nil {
		t.Fatalf("expected second node, got error: %v", err)
	}
	if firstID == secondID {
		t.Fatalf("expected balanced selections, got %d then %d", firstID, secondID)
	}
	snapshot := nm.Snapshot()
	if snapshot.Nodes[firstID].Inflight == 0 || snapshot.Nodes[secondID].Inflight == 0 {
		t.Fatalf("expected selected nodes to be marked inflight, got %+v", snapshot.Nodes)
	}
}

func TestNodeManagerMarkNodeUnavailableExcludesNode(t *testing.T) {
	nm := NewNodeManager([]*ethclient.Client{nil, nil}, 0)
	nm.UpdateNodeChainInfo(0, 100, "0x64")
	nm.UpdateNodeChainInfo(1, 100, "0x64")

	nm.MarkNodeUnavailable(0, "get chain id failed")
	id, _, err := nm.GetBestNode(100)
	if err != nil {
		t.Fatalf("expected fallback node, got error: %v", err)
	}
	if id != 1 {
		t.Fatalf("expected node 1 after node 0 disabled, got %d", id)
	}

	ops := nm.GetAllValidNodeOperators(100, "0x64")
	if len(ops) != 1 || ops[0].ID() != 1 {
		t.Fatalf("unexpected valid operators after disable: %+v", ops)
	}

	snapshot := nm.Snapshot()
	if !snapshot.Nodes[0].Disabled || snapshot.Nodes[0].Ready {
		t.Fatalf("expected disabled node snapshot, got %+v", snapshot.Nodes[0])
	}
	if snapshot.ReadyCount != 1 {
		t.Fatalf("unexpected ready count: %d", snapshot.ReadyCount)
	}
}

func operatorIDsForTest(ops []NodeOperator) []int {
	ids := make([]int, 0, len(ops))
	for _, op := range ops {
		if op == nil {
			continue
		}
		ids = append(ids, op.ID())
	}
	return ids
}
