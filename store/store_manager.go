package store

import (
	"os"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type StoreManager struct {
	db                      *gorm.DB
	batchSize               int
	storeOperationChannel   chan *types.StoreOperation
	storeTaskChannel        chan *StoreTask
	storeCompleteChannel    chan *StoreComplete
	publishOperationChannel chan<- *types.PublishOperation
	storeWorkers            []*StoreWorker
}

func NewStoreManager(db *gorm.DB, batchSize int, storeWorkerCount int, storeOperationChannel chan *types.StoreOperation,
	publishOperationChannel chan<- *types.PublishOperation) *StoreManager {

	storeTaskChannel := make(chan *StoreTask, 10)
	storeCompleteChannel := make(chan *StoreComplete, 10)

	storeWorkers := make([]*StoreWorker, storeWorkerCount)
	for i := 0; i < storeWorkerCount; i++ {
		worker := NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel)
		storeWorkers[i] = worker
	}

	return &StoreManager{
		db:                      db,
		batchSize:               batchSize,
		storeOperationChannel:   storeOperationChannel,
		storeTaskChannel:        storeTaskChannel,
		storeCompleteChannel:    storeCompleteChannel,
		publishOperationChannel: publishOperationChannel,
		storeWorkers:            storeWorkers,
	}
}

func (sm *StoreManager) Run() {
	for _, sw := range sm.storeWorkers {
		sw.Run()
	}

	go func() {
		for op := range sm.storeOperationChannel {
			if op.Type == types.StoreApply {
				data := op.Data.(*types.StoreApplyData)
				height := data.FullBlock.Block.Height

				if err := StoreFullBlock(sm.db, data.FullBlock, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel); err != nil {
					logrus.Errorf("write fullblock failed. height:%v err:%v", height, err)
					os.Exit(0)
				}

				publishOperation := &types.PublishOperation{
					Type: types.PublishApply,
					Data: &types.PublishApplyData{
						FullBlock: data.FullBlock,
					},
				}
				sm.publishOperationChannel <- publishOperation

			} else if op.Type == types.StoreRollback {
				data := op.Data.(*types.StoreRollbackData)
				height := data.Height

				tryCount := 0
				for {
					tryCount++

					if err := Revert(sm.db, height); err == nil {
						break
					}

					time.Sleep(1 * time.Second)
					if tryCount >= 2 {
						logrus.Errorf("db revert out of try count. height:%v", height)
						os.Exit(0)
					}
				}

				publishOperation := &types.PublishOperation{
					Type: types.PublishRollback,
					Data: &types.PublishRollbackData{
						Height: height,
					},
				}
				sm.publishOperationChannel <- publishOperation
			}
		}
	}()
}
