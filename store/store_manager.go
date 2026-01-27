package store

import (
	"os"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type StoreManager struct {
	db                    *gorm.DB
	batchSize             int
	storeOperationChannel chan *types.StoreOperation
	storeTaskChannel      chan *StoreTask
	storeCompleteChannel  chan *StoreComplete
}

func NewStoreManager(db *gorm.DB, batchSize int, storeOperationChannel chan *types.StoreOperation,
	storeTaskChannel chan *StoreTask, storeCompleteChannel chan *StoreComplete) *StoreManager {

	return &StoreManager{
		db:                    db,
		batchSize:             batchSize,
		storeOperationChannel: storeOperationChannel,
		storeTaskChannel:      storeTaskChannel,
		storeCompleteChannel:  storeCompleteChannel,
	}
}

func (sm *StoreManager) Run() {
	go func() {
		for op := range sm.storeOperationChannel {
			if op.Type == types.StoreApply {
				data := op.Data.(*types.StoreApplyData)
				height := data.FullBlock.Block.Height

				if err := StoreFullBlock(sm.db, data.FullBlock, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel); err != nil {
					logrus.Errorf("write fullblock failed. height:%v err:%v", height, err)
					os.Exit(0)
				}
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
			}
		}
	}()
}
