package fetch

import (
	"fmt"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type NodeState struct {
	operator *NodeOperator
	remote   *RemoteChain
	delay    int64 // Last fetch latency in microseconds; GetBestNode picks the lowest delay among eligible nodes.
	ready    bool  // true if usable (head_notifier or sync OK); false after fetch failure or while syncing.
}

func (n *NodeState) GetChainInfo() uint64 {
	height, _ := n.remote.GetChainInfo()
	return height
}

type NodeManager struct {
	mu    sync.RWMutex
	nodes []*NodeState
}

// NewNodeManager builds a node manager. rpcTimeout is the per-RPC context deadline (config fetch.timeout); 0 uses defaultFetchRPCTimeout.
func NewNodeManager(clients []*ethclient.Client, rpcTimeout time.Duration) *NodeManager {
	nodes := make([]*NodeState, len(clients))
	for i, client := range clients {
		nodes[i] = &NodeState{
			operator: NewNodeOperator(i, client, rpcTimeout),
			remote:   NewRemoteChain(),
			delay:    0,
			ready:    true,
		}
	}
	return &NodeManager{
		nodes: nodes,
	}
}

func (nm *NodeManager) NodeCount() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return len(nm.nodes)
}

func (nm *NodeManager) Clients() []*ethclient.Client {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	clients := make([]*ethclient.Client, len(nm.nodes))
	for i, node := range nm.nodes {
		if node == nil {
			continue
		}
		if node.operator != nil {
			clients[i] = node.operator.EthClient()
		}
	}
	return clients
}

// NodeOperators returns NodeOperators in node index order (index is node id).
func (nm *NodeManager) NodeOperators() []*NodeOperator {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	ops := make([]*NodeOperator, len(nm.nodes))
	for i, node := range nm.nodes {
		if node == nil {
			continue
		}
		ops[i] = node.operator
	}
	return ops
}

// UpdateNodeChainInfo is called on head_notifier: updates the node's chain tip and marks it ready.
func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if id < 0 || id >= len(nm.nodes) {
		return
	}
	node := nm.nodes[id]
	node.remote.Update(height, hash)
	node.ready = true
}

func (nm *NodeManager) UpdateNodeState(id int, delay int64, success bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if id < 0 || id >= len(nm.nodes) {
		return
	}

	node := nm.nodes[id]
	node.delay = delay
	node.ready = success
}

// SetNodeNotReady marks a node unusable (after fetch failure or when selected for sync).
func (nm *NodeManager) SetNodeNotReady(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if id < 0 || id >= len(nm.nodes) {
		return
	}
	nm.nodes[id].ready = false
}

// SetNodeReady marks a node usable again (e.g. release after dispatch popTask failure).
func (nm *NodeManager) SetNodeReady(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	if id < 0 || id >= len(nm.nodes) {
		return
	}
	nm.nodes[id].ready = true
}

// SetAllNodesIdle marks all nodes as idle/ready and resets delay.
func (nm *NodeManager) SetAllNodesIdle() {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	for _, node := range nm.nodes {
		if node == nil {
			continue
		}
		node.delay = 0
		node.ready = true
	}
}

// ResetRemoteChainTips clears each node's observed remote tip (height/hash).
// Used when recreating leader runtime state so GetLatestHeight does not retain
// a stale tip across DB restore / bootstrap; new heads from HeaderNotifier or
// RPC then repopulate monotonically (see RemoteChain.Update).
func (nm *NodeManager) ResetRemoteChainTips() {
	nm.mu.Lock()
	defer nm.mu.Unlock()

	for _, node := range nm.nodes {
		if node == nil {
			continue
		}
		node.remote = NewRemoteChain()
	}
}

// GetLatestHeight returns the max chain tip height across nodes (for scan ranges).
func (nm *NodeManager) GetLatestHeight() uint64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	var latest uint64
	for _, node := range nm.nodes {
		if h, _ := node.remote.GetChainInfo(); h > latest {
			latest = h
		}
	}
	return latest
}

// GetBestNode picks the ready node with remote height >= height and the smallest recorded delay.
func (nm *NodeManager) GetBestNode(height uint64) (int, *NodeOperator, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	nodeId := -1
	bestDelay := int64(0)
	for i, node := range nm.nodes {
		if node == nil {
			continue
		}
		if !node.ready {
			continue
		}
		remoteHeight, _ := node.remote.GetChainInfo()
		if remoteHeight < height {
			continue
		}
		if nodeId == -1 || node.delay < bestDelay {
			nodeId = i
			bestDelay = node.delay
		}
	}
	if nodeId == -1 {
		return -1, nil, fmt.Errorf("no valid node with height >= %d", height)
	}
	return nodeId, nm.nodes[nodeId].operator, nil
}
