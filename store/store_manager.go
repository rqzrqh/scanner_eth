package store

import (
	"os"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type StoreManager struct {
	db                   *gorm.DB
	batchSize            int
	storeEventChannel    chan *types.ChainEvent
	publishEventChannel  chan *types.ChainEvent
	storeTaskChannel     chan *StoreTask
	storeCompleteChannel chan *StoreComplete
}

func NewStoreManager(db *gorm.DB, batchSize int, storeEventChannel chan *types.ChainEvent, publishEventChannel chan *types.ChainEvent,
	storeTaskChannel chan *StoreTask, storeCompleteChannel chan *StoreComplete) *StoreManager {

	return &StoreManager{
		db:                   db,
		batchSize:            batchSize,
		storeEventChannel:    storeEventChannel,
		publishEventChannel:  publishEventChannel,
		storeTaskChannel:     storeTaskChannel,
		storeCompleteChannel: storeCompleteChannel,
	}
}

func (sm *StoreManager) Run() {
	go func() {
		for {
			select {
			case ev := <-sm.storeEventChannel:
				if ev.Type == types.Apply {
					data := ev.Data.(*types.ApplyData)
					height := data.FullBlock.Block.Height

					if err := StoreFullBlock(sm.db, data.FullBlock, data.ForkVersion, data.EventID, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel); err != nil {
						logrus.Errorf("write fullblock failed. height:%v err:%v", height, err)
						os.Exit(0)
					}
				} else if ev.Type == types.Revert {
					data := ev.Data.(*types.RevertData)
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

				sm.publishEventChannel <- ev
			}
		}
	}()
}
