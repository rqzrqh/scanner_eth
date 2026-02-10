package fetch

import (
	"scanner_eth/types"

	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

type FetchManager struct {
	nodeManager              *NodeManager
	localChain               *LocalChain
	pendingBlocks            *PendingBlocks
	taskManager              *TaskManager
	forkVersion              uint64
	eventID                  uint64
	remoteChainUpdateChannel <-chan *types.RemoteChainUpdate
	fetchResultNotifyChannel chan *FetchResult
	storeOperationChannel    chan<- *types.StoreOperation
}

func NewFetchManager(clients []*rpc.Client, localChain *LocalChain, endHeight uint64, maxUnorganizedBlockCount int, remoteChainUpdateChannel <-chan *types.RemoteChainUpdate,
	storeOperationChannel chan<- *types.StoreOperation) *FetchManager {

	fetchResultNotifyChannel := make(chan *FetchResult, 100)

	return &FetchManager{
		nodeManager:              NewNodeManager(clients),
		localChain:               localChain,
		pendingBlocks:            NewPendingBlocks(),
		taskManager:              NewTaskManager(endHeight, maxUnorganizedBlockCount),
		forkVersion:              0,
		eventID:                  0,
		remoteChainUpdateChannel: remoteChainUpdateChannel,
		fetchResultNotifyChannel: fetchResultNotifyChannel,
		storeOperationChannel:    storeOperationChannel,
	}
}

func (fm *FetchManager) addBlock(data *types.FullBlock, forkVersion uint64) {

	if forkVersion != fm.forkVersion {
		logrus.Infof("addblock find old fork version. height:%v version:%v currentVersion:%v", data.Block.Height, forkVersion, fm.forkVersion)
		return
	}

	fm.pendingBlocks.addBlock(data)
	fm.taskManager.fetchSuccess(data.Block.Height)

	for {
		currentHeight, _ := fm.localChain.GetChainInfo()
		nextHeight := currentHeight + 1

		// query block
		fullblock, err := fm.pendingBlocks.getBlock(nextHeight)
		if err != nil {
			logrus.Tracef("addblock can not get next. height:%v fork_version:%v event_id:%v", nextHeight, fm.forkVersion, fm.eventID)
			break
		}

		fm.eventID++

		if err := fm.localChain.Grow(nextHeight, fullblock.Block.Hash, fullblock.Block.ParentHash); err != nil {
			fm.forkVersion++
			logrus.Infof("local chain revert. height:%v fork_version:%v event_id:%v err:%v", currentHeight, fm.forkVersion, fm.eventID, err)
			fm.taskManager.clear()
			fm.pendingBlocks.clear()
			// revert one block

			fm.localChain.Revert(currentHeight)

			storeOperation := &types.StoreOperation{
				Type:   types.StoreRollback,
				Height: currentHeight,
			}
			fm.storeOperationChannel <- storeOperation

			break
		} else {
			fm.taskManager.processSuccess(nextHeight)
			fm.pendingBlocks.removeData(nextHeight)
			logrus.Infof("local chain grow. height:%v fork_version:%v event_id:%v", nextHeight, fm.forkVersion, fm.eventID)
			storeOperation := &types.StoreOperation{
				Type:      types.StoreApply,
				Height:    fullblock.Block.Height,
				FullBlock: fullblock,
			}
			fm.storeOperationChannel <- storeOperation
		}
	}
}

func (fm *FetchManager) Run() {
	go func() {
		for {
			select {
			case remoteChainUpdate := <-fm.remoteChainUpdateChannel:
				fm.nodeManager.UpdateNodeChainInfo(remoteChainUpdate.NodeId, remoteChainUpdate.Height, remoteChainUpdate.BlockHash)
				fm.updateTask()
				fm.dispatchTask()

			case fetchResult := <-fm.fetchResultNotifyChannel:
				fm.nodeManager.SetNodeIdle(fetchResult.NodeId)
				if fetchResult.FullBlock != nil {
					fm.addBlock(fetchResult.FullBlock, fetchResult.ForkVersion)
					fm.nodeManager.UpdateNodeMetric(fetchResult.NodeId, fetchResult.CostTime.Microseconds())
				} else {
					fm.nodeManager.UpdateNodeMetric(fetchResult.NodeId, fetchResult.CostTime.Microseconds())
					fm.nodeManager.SetNodeInvalid(fetchResult.NodeId)
					fm.taskManager.fetchFailed(fetchResult.Height)
				}

				fm.updateTask()
				fm.dispatchTask()
			}
		}
	}()
}

func (fm *FetchManager) updateTask() {

	localHeight, _ := fm.localChain.GetChainInfo()

	for i := 0; i < len(fm.nodeManager.nodes); i++ {
		remoteHeight := fm.nodeManager.GetNodeState(i).GetChainInfo()

		if remoteHeight == 0 {
			continue
		}
		if remoteHeight > localHeight {
			// [startHeight, endHeight)
			startHeight := localHeight
			endHeight := remoteHeight
			fm.taskManager.extendTask(startHeight, endHeight)
		}
	}
}

func (fm *FetchManager) dispatchTask() {
	height, err := fm.taskManager.getTask()
	if err != nil {
		return
	}

	nodeId, client, err := fm.nodeManager.GetBestNode(height)
	if err != nil {
		return
	}

	taskId, height, err := fm.taskManager.popTask()
	if err != nil {
		return
	}
	fm.nodeManager.SetNodeBusy(nodeId)

	worker := NewFetchWorker(nodeId, taskId, client, height, fm.forkVersion, fm.fetchResultNotifyChannel)
	worker.Run()
}
