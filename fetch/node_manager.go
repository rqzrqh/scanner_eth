package fetch

import (
	"github.com/ethereum/go-ethereum/rpc"
	"golang.org/x/xerrors"
)

type NodeState struct {
	client  *rpc.Client
	remote  *RemoteChain
	times   uint64
	delay   int64
	isValid bool
	isBusy  bool
}

func (n *NodeState) GetChainInfo() uint64 {
	height, _ := n.remote.GetChainInfo()
	return height
}

type NodeManager struct {
	nodes []*NodeState
}

func NewNodeManager(clients []*rpc.Client) *NodeManager {
	nodes := make([]*NodeState, len(clients))
	for i, client := range clients {
		nodes[i] = &NodeState{
			client:  client,
			remote:  NewRemoteChain(),
			times:   0,
			delay:   0,
			isValid: true,
			isBusy:  false,
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

func (nm *NodeManager) UpdateNodeChainInfo(id int, height uint64, hash string) {
	node := nm.nodes[id]

	oldHeight, _ := node.remote.GetChainInfo()

	node.remote.Update(height, hash)

	if height > oldHeight {
		node.isValid = true
	}
}

func (nm *NodeManager) UpdateNodeMetric(id int, delay int64) {
	node := nm.nodes[id]

	// updat delay
	node.delay = delay
	node.times++
}

func (nm *NodeManager) SetNodeValid(id int) {
	node := nm.nodes[id]
	node.isValid = true
}

func (nm *NodeManager) SetNodeInvalid(id int) {
	node := nm.nodes[id]
	node.isValid = false
}

func (nm *NodeManager) SetNodeBusy(id int) {
	node := nm.nodes[id]
	node.isBusy = true
}

func (nm *NodeManager) SetNodeIdle(id int) {
	node := nm.nodes[id]
	node.isBusy = false
}

func (nm *NodeManager) GetBestNode(height uint64) (int, *rpc.Client, error) {

	nodeId := -1
	for i, node := range nm.nodes {
		if !node.isValid {
			continue
		}

		if node.isBusy {
			continue
		}

		remoteHeight, _ := node.remote.GetChainInfo()
		if remoteHeight < height {
			continue
		}

		if nodeId == -1 {
			nodeId = i
			continue
		}

		currentBestNode := nm.nodes[nodeId]
		if node.times < currentBestNode.times {
			nodeId = i
		} else if node.times == currentBestNode.times {
			if node.delay < currentBestNode.delay {
				nodeId = i
			}
		}
	}

	if nodeId == -1 {
		return -1, nil, xerrors.New("no valid node")
	}

	return nodeId, nm.nodes[nodeId].client, nil
}
