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
		splitTxErc20(fullblock.TxErc20List, batchSize, height, storeTaskChannel, taskSet)
		splitTxErc721(fullblock.TxErc721List, batchSize, height, storeTaskChannel, taskSet)
		splitTxErc1155(fullblock.TxErc1155List, batchSize, height, storeTaskChannel, taskSet)
		splitBalance(fullblock.BalanceList, batchSize, height, storeTaskChannel, taskSet)

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
	if err := db.Create(fullblock.Block).Error; err != nil {
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

func splitTxErc20(modelList []*model.TxErc20, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(TxErc20, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split tx erc20. height:%v count:%v", height, count)
}

func splitTxErc721(modelList []*model.TxErc721, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(TxErc721, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split tx erc721. height:%v count:%v", height, count)
}

func splitTxErc1155(modelList []*model.TxErc1155, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(TxErc1155, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split tx erc1155. height:%v count:%v", height, count)
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

type StoreTaskType byte

const (
	Tx StoreTaskType = iota
	EventLog
	Balance
	TxErc20
	TxErc721
	TxErc1155
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
			select {
			case tsk := <-sw.storeTaskChannel:
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
					err = sw.db.Create(data).Error
				case EventLog:
					data := make([]*model.EventLog, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.EventLog))
					}
					err = sw.db.Create(data).Error
				case TxErc20:
					data := make([]*model.TxErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.TxErc20))
					}
					err = sw.db.Create(data).Error
				case TxErc721:
					data := make([]*model.TxErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.TxErc721))
					}
					err = sw.db.Create(data).Error
				case TxErc1155:
					data := make([]*model.TxErc1155, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.TxErc1155))
					}
					err = sw.db.Create(data).Error
				case Balance:
					data := make([]*model.Balance, 0)
					for _, v := range tsk.data {
						data = append(data, v.(*model.Balance))
					}
					err = sw.db.Clauses(clause.Insert{Modifier: "IGNORE"}).Create(data).Error
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
