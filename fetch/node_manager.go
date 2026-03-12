package fetch

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethclient"
)

type NodeState struct {
	client  *ethclient.Client
	remote  *RemoteChain
	delay   int64 // 最近一次拉取耗时（微秒），用于 GetBestNode 选延迟最小的节点
	ready   bool  // true=可用(收到 head_notifier 或同步成功)，false=不可用(拉取失败或正在同步)
}

func (n *NodeState) GetChainInfo() uint64 {
	height, _ := n.remote.GetChainInfo()
	return height
}

type NodeManager struct {
	nodes []*NodeState
}

func NewNodeManager(clients []*ethclient.Client) *NodeManager {
	nodes := make([]*NodeState, len(clients))
	for i, client := range clients {
		nodes[i] = &NodeState{
			client: client,
			remote: NewRemoteChain(),
			delay:  0,
			ready:  true,
		}
	}
	return &NodeManager{
		nodes: nodes,
	}
}

func (nm *NodeManager) GetNodeState(id int) *NodeState {
	if id < 0 || id >= len(nm.nodes) {
		return nil
	}
	return nm.nodes[id]
}

func (nm *NodeManager) NodeCount() int {
	return len(nm.nodes)
}

// UpdateNodeChainInfo 收到 head_notifier 消息时调用，更新节点链上高度并置为可用
func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	if id < 0 || id >= len(nm.nodes) {
		return
	}
	node := nm.nodes[id]
	node.remote.Update(height, hash)
	node.ready = true
}

// UpdateNodeMetric 同步成功时调用，记录耗时并将节点置为可用
func (nm *NodeManager) UpdateNodeMetric(id int, delay int64) {
	if id < 0 || id >= len(nm.nodes) {
		return
	}
	node := nm.nodes[id]
	node.delay = delay
	node.ready = true
}

// SetNodeNotReady 将节点置为不可用（拉取失败时或节点被选去同步时调用，同步结束后由成功/失败再更新）
func (nm *NodeManager) SetNodeNotReady(id int) {
	if id < 0 || id >= len(nm.nodes) {
		return
	}
	nm.nodes[id].ready = false
}

// SetNodeReady 将节点恢复为可用（如 dispatch 时 popTask 失败，需释放已选中的节点）
func (nm *NodeManager) SetNodeReady(id int) {
	if id < 0 || id >= len(nm.nodes) {
		return
	}
	nm.nodes[id].ready = true
}

// GetBestNode 在高度 >= height 且 ready 的节点中，返回延迟最小的节点
func (nm *NodeManager) GetBestNode(height uint64) (int, *ethclient.Client, error) {
	nodeId := -1
	for i, node := range nm.nodes {
		if !node.ready {
			continue
		}
		remoteHeight, _ := node.remote.GetChainInfo()
		if remoteHeight < height {
			continue
		}
		if nodeId == -1 || node.delay < nm.nodes[nodeId].delay {
			nodeId = i
		}
	}
	if nodeId == -1 {
		return -1, nil, fmt.Errorf("no valid node with height >= %d", height)
	}
	return nodeId, nm.nodes[nodeId].client, nil
}
