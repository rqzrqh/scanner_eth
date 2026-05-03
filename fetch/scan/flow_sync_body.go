package scan

import (
	"context"

	"scanner_eth/blocktree"
	fetchserialstore "scanner_eth/fetch/serial_store"
	fetchstore "scanner_eth/fetch/store"
	fetchtask "scanner_eth/fetch/taskpool"

	"github.com/sirupsen/logrus"
)

type branchProcessState struct {
	Hash        string
	ParentReady bool
}

func (sf *Flow) RunSyncBodyStage(ctx context.Context) []fetchserialstore.Branch {
	if !sf.canRunScanStage(ctx) {
		return nil
	}
	bodyBranches := sf.collectStoreBranchesForBodySync()
	sf.requestMissingBodySync(bodyBranches)
	return bodyBranches
}

func (sf *Flow) EnqueueMissingBodyTasks(branches []fetchserialstore.Branch) {
	sf.requestMissingBodySync(branches)
}

func (sf *Flow) collectStoreBranchesForBodySync() []fetchserialstore.Branch {
	return sf.BuildStoreBranches()
}

func (sf *Flow) requestMissingBodySync(branches []fetchserialstore.Branch) {
	if sf == nil || sf.taskPool == nil {
		return
	}
	for _, branch := range branches {
		for _, node := range branch.Nodes {
			hash := sf.normalize(node.Hash)
			if hash == "" || node.BlockData != nil {
				continue
			}
			sf.taskPool.EnqueueTaskWithPriority(hash, fetchtask.TaskPriorityHigh)
		}
	}
}

func (sf *Flow) ProcessBranchNode(ctx context.Context, node *blocktree.LinkedNode) bool {
	if node == nil || sf == nil {
		return false
	}
	hash := sf.normalize(node.Key)
	if hash == "" {
		return false
	}
	if sf.storedBlocks != nil && sf.storedBlocks.IsStored(hash) {
		return true
	}
	if sf.storeWorker != nil && sf.storeWorker.IsInflight(hash) {
		return false
	}

	state, ok := sf.BuildBranchProcessState(node)
	if !ok || !state.ParentReady {
		return false
	}
	nodeData := sf.getPendingBody(node.Key)
	if nodeData == nil {
		sf.EnqueueNodeBodySync(state)
		return false
	}
	return sf.StoreNodeBodyData(ctx, node, nodeData, state)
}

func (sf *Flow) BuildBranchProcessState(node *blocktree.LinkedNode) (branchProcessState, bool) {
	if sf == nil {
		return branchProcessState{}, false
	}
	hash := sf.normalize(node.Key)
	if hash == "" || sf.storedBlocks.IsStored(hash) || (sf.storeWorker != nil && sf.storeWorker.IsInflight(hash)) {
		return branchProcessState{}, false
	}
	parentHash := sf.normalize(node.ParentKey)
	return branchProcessState{
		Hash:        hash,
		ParentReady: parentHash == "" || sf.storedBlocks.IsStored(parentHash),
	}, true
}

func (sf *Flow) EnqueueNodeBodySync(state branchProcessState) {
	if sf == nil {
		return
	}
	priority := fetchtask.TaskPriorityNormal
	if state.ParentReady {
		priority = fetchtask.TaskPriorityHigh
	}
	sf.taskPool.EnqueueTaskWithPriority(state.Hash, priority)
}

func (sf *Flow) StoreNodeBodyData(ctx context.Context, node *blocktree.LinkedNode, nodeData *fetchstore.EventBlockData, state branchProcessState) bool {
	if ctx != nil && ctx.Err() != nil {
		return false
	}
	if !sf.HasStorableNodeData(nodeData) || !state.ParentReady {
		return false
	}
	if sf.storeWorker == nil {
		logrus.Warn("store block worker is not initialized")
		return false
	}
	if err := sf.storeWorker.Submit(ctx, state.Hash, node.Height, nodeData); err != nil {
		logrus.Warnf("store block failed. height:%v hash:%v err:%v", node.Height, state.Hash, err)
		return false
	}
	return true
}

func (sf *Flow) HasStorableNodeData(nodeData *fetchstore.EventBlockData) bool {
	return nodeData != nil && nodeData.StorageFullBlock != nil
}
