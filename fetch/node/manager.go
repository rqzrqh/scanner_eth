package node

import (
	"fmt"
	headernotify "scanner_eth/fetch/header_notify"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/ethclient"
)

type NodeState struct {
	operator *NodeOperatorImpl
	remote   *headernotify.RemoteChain
	delay    int64
	ready    bool
}

type NodeSnapshot struct {
	ID           int    `json:"id"`
	Ready        bool   `json:"ready"`
	Delay        int64  `json:"delay"`
	RemoteHeight uint64 `json:"remote_height"`
	RemoteHash   string `json:"remote_hash"`
}

type ManagerSnapshot struct {
	NodeCount    uint64         `json:"node_count"`
	ReadyCount   uint64         `json:"ready_count"`
	LatestHeight uint64         `json:"latest_height"`
	Nodes        []NodeSnapshot `json:"nodes"`
}

func (n *NodeState) GetChainInfo() uint64 {
	if n == nil {
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

// validNodeLocked reports whether id is in range and nm.nodes[id] is non-nil.
// Caller must hold nm.mu.
func (nm *NodeManager) validNodeLocked(id int) bool {
	return nm != nil && id >= 0 && id < len(nm.nodes)
}

func (nm *NodeManager) NodeCount() int {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	return len(nm.nodes)
}

func (nm *NodeManager) NodeOperators() []NodeOperator {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	ops := make([]NodeOperator, len(nm.nodes))
	for i, node := range nm.nodes {
		ops[i] = node.operator
	}
	return ops
}

func (nm *NodeManager) EthClients() []*ethclient.Client {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	clients := make([]*ethclient.Client, len(nm.nodes))
	for i, node := range nm.nodes {
		clients[i] = node.operator.EthClient()
	}
	return clients
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
	if !nm.validNodeLocked(id) {
		return
	}
	nm.nodes[id].remote = headernotify.NewRemoteChain()
}

func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	nm.nodes[id].remote.Update(height, hash)
	nm.nodes[id].ready = true
}

func (nm *NodeManager) UpdateNodeState(id int, delay int64, success bool) {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	if !nm.validNodeLocked(id) {
		return
	}
	nm.nodes[id].delay = delay
	nm.nodes[id].ready = success
}

func (nm *NodeManager) SetAllNodesIdle() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	for _, node := range nm.nodes {
		node.delay = 0
		node.ready = true
	}
}

func (nm *NodeManager) ResetRemoteChainTips() {
	nm.mu.Lock()
	defer nm.mu.Unlock()
	for _, node := range nm.nodes {
		node.remote = headernotify.NewRemoteChain()
	}
}

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

func (nm *NodeManager) GetBestNode(height uint64) (int, NodeOperator, error) {
	nm.mu.RLock()
	defer nm.mu.RUnlock()
	nodeId := -1
	bestDelay := int64(0)
	for i, node := range nm.nodes {
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

func (nm *NodeManager) Snapshot() ManagerSnapshot {
	if nm == nil {
		return ManagerSnapshot{Nodes: []NodeSnapshot{}}
	}
	nm.mu.RLock()
	defer nm.mu.RUnlock()

	snapshot := ManagerSnapshot{
		NodeCount: uint64(len(nm.nodes)),
		Nodes:     make([]NodeSnapshot, 0, len(nm.nodes)),
	}
	for id, node := range nm.nodes {
		if node == nil {
			snapshot.Nodes = append(snapshot.Nodes, NodeSnapshot{ID: id})
			continue
		}
		var remoteHeight uint64
		var remoteHash string
		if node.remote != nil {
			remoteHeight, remoteHash = node.remote.GetChainInfo()
		}
		if node.ready {
			snapshot.ReadyCount++
		}
		if remoteHeight > snapshot.LatestHeight {
			snapshot.LatestHeight = remoteHeight
		}
		snapshot.Nodes = append(snapshot.Nodes, NodeSnapshot{
			ID:           id,
			Ready:        node.ready,
			Delay:        node.delay,
			RemoteHeight: remoteHeight,
			RemoteHash:   remoteHash,
		})
	}
	return snapshot
}
