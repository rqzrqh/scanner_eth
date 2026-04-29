package node

import (
	"fmt"
	headernotify "scanner_eth/fetch/header_notify"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type NodeState struct {
	operator *NodeOperator
	remote   *headernotify.RemoteChain
	delay    int64
	ready    bool
}

func (n *NodeState) GetChainInfo() uint64 {
	if n == nil || n.remote == nil {
		return 0
	}
	height, _ := n.remote.GetChainInfo()
	return height
}

type NodeManager struct {
	mu    sync.RWMutex
	nodes []*NodeState
}

func NewNodeManager(clients []*ethclient.Client, rpcTimeout time.Duration) *NodeManager {
	nodes := make([]*NodeState, len(clients))
	for i, client := range clients {
		nodes[i] = &NodeState{
			operator: NewNodeOperator(i, client, rpcTimeout),
			remote:   headernotify.NewRemoteChain(),
			delay:    0,
			ready:    true,
		}
	}
	return &NodeManager{nodes: nodes}
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
		if node != nil && node.operator != nil {
			clients[i] = node.operator.EthClient()
		}
	}
	return clients
}

func (nm *NodeManager) NodeOperators() []*NodeOperator {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	ops := make([]*NodeOperator, len(nm.nodes))
	for i, node := range nm.nodes {
		if node != nil {
			ops[i] = node.operator
		}
	}
	return ops
}

func (nm *NodeManager) Node(id int) *NodeState {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	if id < 0 || id >= len(nm.nodes) {
		return nil
	}
	return nm.nodes[id]
}

func (nm *NodeManager) ResetNodeRemoteTip(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id < 0 || id >= len(nm.nodes) || nm.nodes[id] == nil {
		return
	}
	nm.nodes[id].remote = headernotify.NewRemoteChain()
}

func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id < 0 || id >= len(nm.nodes) || nm.nodes[id] == nil {
		return
	}
	nm.nodes[id].remote.Update(height, hash)
	nm.nodes[id].ready = true
}

func (nm *NodeManager) UpdateNodeState(id int, delay int64, success bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id < 0 || id >= len(nm.nodes) || nm.nodes[id] == nil {
		return
	}
	nm.nodes[id].delay = delay
	nm.nodes[id].ready = success
}

func (nm *NodeManager) SetNodeNotReady(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id < 0 || id >= len(nm.nodes) || nm.nodes[id] == nil {
		return
	}
	nm.nodes[id].ready = false
}

func (nm *NodeManager) SetNodeReady(id int) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if id < 0 || id >= len(nm.nodes) || nm.nodes[id] == nil {
		return
	}
	nm.nodes[id].ready = true
}

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

func (nm *NodeManager) ResetRemoteChainTips() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	for _, node := range nm.nodes {
		if node != nil {
			node.remote = headernotify.NewRemoteChain()
		}
	}
}

func (nm *NodeManager) GetLatestHeight() uint64 {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	var latest uint64
	for _, node := range nm.nodes {
		if node == nil || node.remote == nil {
			continue
		}
		if h, _ := node.remote.GetChainInfo(); h > latest {
			latest = h
		}
	}
	return latest
}

func (nm *NodeManager) GetBestNode(height uint64) (int, *NodeOperator, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	nodeId := -1
	bestDelay := int64(0)
	for i, node := range nm.nodes {
		if node == nil || !node.ready || node.remote == nil {
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
