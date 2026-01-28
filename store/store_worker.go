package store

import (
	"context"
	"sync_eth/model"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var taskCounter = uint64(0)

func deleteData(db *gorm.DB, height uint64) error {
	result := db.Where("height = ?", height).Delete(&model.Block{})
	if result.Error != nil {
		logrus.Errorf("deleteData delete block failed %v %v", result.Error, height)
		return result.Error
	}

	logrus.Infof("delete block affected count %v", result.RowsAffected)

	return nil
}

func Restore(db *gorm.DB, height uint64) error {
	if err := deleteData(db, height); err != nil {
		logrus.Errorf("restore delete data failed.err:%v height:%v", err, height)
		return err
	}

	logrus.Infof("database restore success height:%v", height)

	return nil
}

func Revert(db *gorm.DB, height uint64) error {
	if err := deleteData(db, height); err != nil {
		logrus.Errorf("revert delete data failed. err:%v height:%v", err, height)
		return err
	}

	logrus.Infof("database revert success height:%v", height)

	return nil
}

func StoreFullBlock(db *gorm.DB, fullblock *types.FullBlock, batchSize int, storeTaskChannel chan *StoreTask, storeCompleteChannel chan *StoreComplete) error {
	startTime := time.Now()

	height := fullblock.Block.Height

	dispatchComplete := make(chan map[uint64]struct{}, 1)

	grp, _ := errgroup.WithContext(context.Background())

	grp.Go(func() error {
		taskSet := make(map[uint64]struct{})
		splitTx(fullblock.TxList, batchSize, height, storeTaskChannel, taskSet)
		splitEventLog(fullblock.EventLogList, batchSize, height, storeTaskChannel, taskSet)
		splitEventErc20Transfer(fullblock.EventErc20TransferList, batchSize, height, storeTaskChannel, taskSet)
		splitEventErc721Transfer(fullblock.EventErc721TransferList, batchSize, height, storeTaskChannel, taskSet)
		splitEventErc1155Transfer(fullblock.EventErc1155TransferList, batchSize, height, storeTaskChannel, taskSet)
		splitBalance(fullblock.BalanceList, batchSize, height, storeTaskChannel, taskSet)
		splitContractErc20(fullblock.ContractErc20List, batchSize, height, storeTaskChannel, taskSet)
		splitContractErc721(fullblock.ContractErc721List, batchSize, height, storeTaskChannel, taskSet)

		dispatchComplete <- taskSet

		return nil
	})

	storeFailed := false

	grp.Go(func() error {
		var dispatchTaskSet map[uint64]struct{}
		storeCompleteTaskSet := make(map[uint64]struct{})

	Loop:
		for {
			select {
			case c := <-storeCompleteChannel:
				if c.err != nil {
					storeFailed = true
				}

				storeCompleteTaskSet[c.taskID] = struct{}{}

				if dispatchTaskSet != nil {
					for k := range storeCompleteTaskSet {
						delete(dispatchTaskSet, k)
					}

					if len(dispatchTaskSet) == 0 {
						break Loop
					}
				}
			case e := <-dispatchComplete:
				dispatchTaskSet = e
				for k := range storeCompleteTaskSet {
					delete(dispatchTaskSet, k)
				}

				if len(dispatchTaskSet) == 0 {
					break Loop
				}
			}
		}

		return nil
	})

	if err := grp.Wait(); err != nil {
		logrus.Errorf("store fullblock failed %v %v", err, height)
		panic("grp unknown error")
		return err
	}

	if storeFailed {
		logrus.Errorf("store fullblock failed %v", height)
		return xerrors.New("store fullblock failed")
	}

	startTime2 := time.Now()
	if err := db.Clauses(clause.OnConflict{UpdateAll: true}).Create(fullblock.Block).Error; err != nil {
		logrus.Errorf("store chain block failed %v", err)
		return err
	}

	logrus.Debugf("store block. height:%v cost:%v", height, time.Since(startTime2).String())

	prevHash := fullblock.Block.ParentHash
	blockHash := fullblock.Block.BlockHash

	logrus.Infof("store success. height:%v hash:%v prev_hash:%v cost:%v",
		height, blockHash, prevHash, time.Since(startTime).String())

	return nil
}

func splitTask(taskType StoreTaskType, modelList []interface{}, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	round := count / batchSize
	left := count % batchSize

	for i := 0; i < round; i++ {
		taskCounter++
		ay := modelList[batchSize*i : batchSize*(i+1)]
		taskSet[taskCounter] = struct{}{}

		storeTaskChannel <- &StoreTask{
			height:   height,
			taskID:   taskCounter,
			taskType: taskType,
			data:     ay,
		}
	}

	if left > 0 {
		taskCounter++
		ay := modelList[batchSize*round:]
		taskSet[taskCounter] = struct{}{}

		storeTaskChannel <- &StoreTask{
			height:   height,
			taskID:   taskCounter,
			taskType: taskType,
			data:     ay,
		}
	}
}

func splitTx(modelList []*model.Tx, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(Tx, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split tx. height:%v count:%v", height, count)
}

func splitEventLog(modelList []*model.EventLog, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventLog, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event log. height:%v count:%v", height, count)
}

func splitEventErc20Transfer(modelList []*model.EventErc20Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc20Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc20 transfer. height:%v count:%v", height, count)
}

func splitEventErc721Transfer(modelList []*model.EventErc721Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc721Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc721 transfer. height:%v count:%v", height, count)
}

func splitEventErc1155Transfer(modelList []*model.EventErc1155Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc1155Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc1155 transfer. height:%v count:%v", height, count)
}

func splitBalance(modelList []*model.Balance, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	// balance should not be split, overwise cause database error(primary error?)
	splitTask(Balance, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split balance. height:%v count:%v", height, count)
}

func splitContractErc20(modelList []*model.ContractErc20, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(ContractErc20, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split contract erc20. height:%v count:%v", height, count)
}

func splitContractErc721(modelList []*model.ContractErc721, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(ContractErc721, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split contract erc721. height:%v count:%v", height, count)
}

type StoreTaskType byte

const (
	Tx StoreTaskType = iota
	EventLog
	Balance
	EventErc20Transfer
	EventErc721Transfer
	EventErc1155Transfer
	ContractErc20
	ContractErc721
)

type StoreTask struct {
	height   uint64
	taskID   uint64
	taskType StoreTaskType
	data     []interface{}
}

type StoreComplete struct {
	taskID uint64
	err    error
}

type StoreWorker struct {
	id                   int
	db                   *gorm.DB
	storeTaskChannel     chan *StoreTask
	storeCompleteChannel chan *StoreComplete
}

func NewStoreWorker(id int, db *gorm.DB, storeTaskChannel chan *StoreTask, storeCompleteChannel chan *StoreComplete) *StoreWorker {
	return &StoreWorker{
		id:                   id,
		db:                   db,
		storeTaskChannel:     storeTaskChannel,
		storeCompleteChannel: storeCompleteChannel,
	}
}

func (sw *StoreWorker) Run() {
	go func() {
		for {
			for tsk := range sw.storeTaskChannel {
				tryCount := 0
			Retry:
				tryCount++

				var err error
				taskType := tsk.taskType
				taskID := tsk.taskID
				height := tsk.height
				startTime := time.Now()

				switch taskType {
				case Tx:
					data := make([]*model.Tx, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.Tx))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventLog:
					data := make([]*model.EventLog, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.EventLog))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc20Transfer:
					data := make([]*model.EventErc20Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.EventErc20Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc721Transfer:
					data := make([]*model.EventErc721Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.EventErc721Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc1155Transfer:
					data := make([]*model.EventErc1155Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.EventErc1155Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case Balance:
					data := make([]*model.Balance, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.Balance))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case ContractErc20:
					data := make([]*model.ContractErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.ContractErc20))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case ContractErc721:
					data := make([]*model.ContractErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.ContractErc721))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				default:
					panic("unknown task type")
				}

				if err != nil {
					logrus.Errorf("store failed err:%v id:%v height:%v type:%v task_id:%v try_count:%v cost:%v",
						err, sw.id, height, taskType, taskID, tryCount, time.Since(startTime).String())
					if tryCount <= 2 {
						time.Sleep(100 * time.Millisecond)
						goto Retry
					}
				}

				sw.storeCompleteChannel <- &StoreComplete{taskID: taskID, err: err}
			}
		}
	}()
}
