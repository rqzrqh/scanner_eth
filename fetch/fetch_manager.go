package fetch

import (
	"sync"
	"sync_eth/store"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
)

type FetchManager struct {
	mtx               sync.Mutex
	differ            *Differ
	mc                *MemoryChain
	sm                *store.StoreManager
	dm                *DataManager
	tm                *TaskManager
	forkVersion       uint64
	eventID           uint64
	storeEventChannel chan *types.ChainEvent
}

func NewFetchManager(differ *Differ, mc *MemoryChain, sm *store.StoreManager, maxTaskCount int, storeEventChannel chan *types.ChainEvent) *FetchManager {
	return &FetchManager{
		mtx:               sync.Mutex{},
		differ:            differ,
		mc:                mc,
		sm:                sm,
		dm:                NewDataManager(),
		tm:                NewTaskManager(maxTaskCount),
		forkVersion:       0,
		eventID:           0,
		storeEventChannel: storeEventChannel,
	}
}

func (fm *FetchManager) addBlock(data *types.FullBlock, forkVersion uint64) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	if forkVersion != fm.forkVersion {
		logrus.Infof("addblock find old fork version. height:%v version:%v currentVersion:%v", data.Block.Height, forkVersion, fm.forkVersion)
		return
	}

	fm.dm.addBlock(data)
	fm.tm.fetchSuccess(data.Block.Height)

	for {
		currentHeight, _ := fm.mc.GetChainInfo()
		nextHeight := currentHeight + 1

		// query block
		fullblock, err := fm.dm.getBlock(nextHeight)
		if err != nil {
			logrus.Tracef("addblock can not get next. height:%v fork_version:%v event_id:%v", nextHeight, fm.forkVersion, fm.eventID)
			break
		}

		fm.eventID++

		if err := fm.mc.Grow(nextHeight, fullblock.Block.BlockHash, fullblock.Block.ParentHash); err != nil {
			fm.forkVersion++
			logrus.Infof("memory chain revert. height:%v fork_version:%v event_id:%v", currentHeight, fm.forkVersion, fm.eventID)
			fm.tm.clear()
			fm.dm.clear()
			// revert one block

			fm.mc.Revert(currentHeight)

			ev := &types.ChainEvent{
				Type: types.Revert,
				Data: &types.RevertData{
					Height:      currentHeight,
					ForkVersion: fm.forkVersion,
					EventID:     fm.eventID,
				},
			}
			fm.storeEventChannel <- ev
			break
		} else {
			fm.tm.processSuccess(nextHeight)
			fm.dm.removeData(nextHeight)
			logrus.Infof("memory chain grow. height:%v fork_version:%v event_id:%v", nextHeight, fm.forkVersion, fm.eventID)
			ev := &types.ChainEvent{
				Type: types.Apply,
				Data: &types.ApplyData{
					FullBlock:   fullblock,
					ForkVersion: fm.forkVersion,
					EventID:     fm.eventID,
				},
			}

			fm.storeEventChannel <- ev
		}
	}

	// [startHeight, endHeight)
	startHeight, endHeight, err := fm.differ.CalcDiffer()
	if err == nil {
		fm.tm.extendTask(startHeight, endHeight)
	}
}

func (fm *FetchManager) getTask() (uint64, uint64, error) {
	fm.mtx.Lock()
	defer fm.mtx.Unlock()

	height, err := fm.tm.getTask()
	return height, fm.forkVersion, err
}

func (fm *FetchManager) Run() {
	go func() {
		for {
			time.Sleep(1 * time.Second)
			fm.mtx.Lock()

			// [startHeight, endHeight)
			startHeight, endHeight, err := fm.differ.CalcDiffer()
			if err == nil {
				fm.tm.extendTask(startHeight, endHeight)
			}
			fm.mtx.Unlock()
		}
	}()
}
