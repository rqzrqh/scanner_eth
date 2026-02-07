package store

import (
	"context"
	"os"
	"scanner_eth/model"
	"scanner_eth/protocol"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var taskCounter = uint64(0)

func Revert(db *gorm.DB, height uint64, chainBinlog *protocol.ChainBinlog, binlogData []byte) (uint64, error) {

	modelChainBinlog := &model.ChainBinlog{
		MessageId:  chainBinlog.MessageId,
		ActionType: int(protocol.ChainActionRollback),
		Height:     height,
		BinlogData: binlogData,
	}

	if err := db.Transaction(func(tx *gorm.DB) error {

		// Delete Block
		if err := tx.Delete(&model.Block{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete block failed %v", err)
			return err
		}

		// Delete Tx
		if err := tx.Delete(&model.Tx{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete tx failed %v", err)
			return err
		}

		// Delete TxInternal
		if err := tx.Delete(&model.TxInternal{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete tx internal failed %v", err)
			return err
		}

		// Delete EventLog
		if err := tx.Delete(&model.EventLog{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete event log failed %v", err)
			return err
		}

		// Delete EventErc20Transfer
		if err := tx.Delete(&model.EventErc20Transfer{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete event erc20 transfer failed %v", err)
			return err
		}

		// Delete EventErc721Transfer
		if err := tx.Delete(&model.EventErc721Transfer{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete event erc721 transfer failed %v", err)
			return err
		}

		// Delete EventErc1155Transfer
		if err := tx.Delete(&model.EventErc1155Transfer{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete event erc1155 transfer failed %v", err)
			return err
		}
		/*
			// Delete Contract
			if err := tx.Delete(&model.Contract{}, "height = ?", height).Error; err != nil {
				logrus.Errorf("delete contract failed %v", err)
				return err
			}

			// Delete ContractErc20
			if err := tx.Delete(&model.ContractErc20{}, "height = ?", height).Error; err != nil {
				logrus.Errorf("delete contract erc20 failed %v", err)
				return err
			}

			// Delete ContractErc721
			if err := tx.Delete(&model.ContractErc721{}, "height = ?", height).Error; err != nil {
				logrus.Errorf("delete contract erc721 failed %v", err)
				return err
			}
		*/
		// Create ChainBinlog
		if err := tx.Create(modelChainBinlog).Error; err != nil {
			logrus.Errorf("store chain binlog failed %v", err)
			return err
		}

		expectMessageId := modelChainBinlog.MessageId - 1

		var result *gorm.DB
		if result = tx.Model(&model.ScannerInfo{}).Where("chain_id = ? AND message_id = ?", chainBinlog.ChainId, expectMessageId).Update("message_id", modelChainBinlog.MessageId); result.Error != nil {
			logrus.Fatalf("update scanner info failed %v", result.Error)
			return result.Error
		}

		if result.RowsAffected == 0 {
			logrus.Fatalf("update scanner info failed, expect message id not match, may be there are multiple processes. expect:%v", expectMessageId)
			os.Exit(0)
		}

		return nil
	}); err != nil {
		logrus.Errorf("store chain block failed %v", err)
		return 0, err
	}

	logrus.Infof("database revert success height:%v", height)

	return modelChainBinlog.Id, nil
}

func StoreFullBlock(db *gorm.DB, fullblock *StorageFullBlock, chainBinlog *protocol.ChainBinlog, binlogData []byte, batchSize int, storeTaskChannel chan *StoreTask, storeCompleteChannel chan *StoreComplete) (uint64, error) {

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
		splitBalanceNative(fullblock.BalanceNativeList, batchSize, height, storeTaskChannel, taskSet)
		splitBalanceErc20(fullblock.BalanceErc20List, batchSize, height, storeTaskChannel, taskSet)
		splitContract(fullblock.ContractList, batchSize, height, storeTaskChannel, taskSet)
		splitContractErc20(fullblock.ContractErc20List, batchSize, height, storeTaskChannel, taskSet)
		splitContractErc721(fullblock.ContractErc721List, batchSize, height, storeTaskChannel, taskSet)
		splitTokenErc721(fullblock.TokenErc721List, batchSize, height, storeTaskChannel, taskSet)

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
		return 0, err
	}

	if storeFailed {
		logrus.Errorf("store fullblock failed %v", height)
		return 0, xerrors.New("store fullblock failed")
	}

	startTime2 := time.Now()

	modelChainBinlog := &model.ChainBinlog{
		MessageId:  chainBinlog.MessageId,
		ActionType: int(chainBinlog.ActionType),
		Height:     chainBinlog.Height,
		BinlogData: binlogData,
	}

	if err := db.Transaction(func(tx *gorm.DB) error {

		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&fullblock.Block).Error; err != nil {
			logrus.Errorf("store chain block failed %v", err)
			return err
		}

		if err := tx.Create(modelChainBinlog).Error; err != nil {
			logrus.Errorf("store chain binlog failed %v", err)
			return err
		}

		expectMessageId := modelChainBinlog.MessageId - 1

		var result *gorm.DB
		if result = tx.Model(&model.ScannerInfo{}).Where("chain_id = ? AND message_id = ?", chainBinlog.ChainId, expectMessageId).Update("message_id", modelChainBinlog.MessageId); result.Error != nil {
			logrus.Fatalf("update scanner info failed %v", result.Error)
			return result.Error
		}

		if result.RowsAffected == 0 {
			logrus.Fatalf("update scanner info failed, expect message id not match, may be there are multiple processes. expect:%v", expectMessageId)
			os.Exit(0)
		}

		var prevBlock model.Block
		if err := tx.Where("height = ?", height-1).First(&prevBlock).Error; err != nil {
			logrus.Errorf("get prev block failed %v", err)
			return err
		}

		if prevBlock.BlockHash != fullblock.Block.ParentHash {
			logrus.Fatalf("prev block hash not match. height:%v prev_hash:%v current_parent_hash:%v", height, prevBlock.BlockHash, fullblock.Block.ParentHash)
			os.Exit(0)
			return xerrors.New("prev block hash not match")
		}

		return nil
	}); err != nil {
		logrus.Errorf("store chain block failed %v", err)
		return 0, err
	}

	logrus.Debugf("store block. height:%v cost:%v id:%v", height, time.Since(startTime2).String(), modelChainBinlog.Id)

	return modelChainBinlog.Id, nil
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

func splitTx(modelList []model.Tx, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(Tx, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split tx. height:%v count:%v", height, count)
}

func splitEventLog(modelList []model.EventLog, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventLog, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event log. height:%v count:%v", height, count)
}

func splitEventErc20Transfer(modelList []model.EventErc20Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc20Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc20 transfer. height:%v count:%v", height, count)
}

func splitEventErc721Transfer(modelList []model.EventErc721Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc721Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc721 transfer. height:%v count:%v", height, count)
}

func splitEventErc1155Transfer(modelList []model.EventErc1155Transfer, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(EventErc1155Transfer, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split event erc1155 transfer. height:%v count:%v", height, count)
}

func splitBalanceNative(modelList []model.BalanceNative, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	// balance should not be split, overwise cause database error(primary error?)
	splitTask(BalanceNative, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split balance native. height:%v count:%v", height, count)
}

func splitBalanceErc20(modelList []model.BalanceErc20, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	// balance erc20 should not be split, overwise cause database error(primary error?)
	splitTask(BalanceErc20, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split balance erc20. height:%v count:%v", height, count)
}

func splitContract(modelList []model.Contract, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(Contract, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split contract. height:%v count:%v", height, count)
}

func splitContractErc20(modelList []model.ContractErc20, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(ContractErc20, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split contract erc20. height:%v count:%v", height, count)
}

func splitContractErc721(modelList []model.ContractErc721, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(ContractErc721, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split contract erc721. height:%v count:%v", height, count)
}

func splitTokenErc721(modelList []model.TokenErc721, batchSize int, height uint64, storeTaskChannel chan *StoreTask, taskSet map[uint64]struct{}) {
	count := len(modelList)

	list := make([]interface{}, 0)
	for _, v := range modelList {
		list = append(list, v)
	}

	splitTask(TokenErc721, list, batchSize, height, storeTaskChannel, taskSet)

	logrus.Debugf("split token erc721. height:%v count:%v", height, count)
}

type StoreTaskType byte

const (
	Tx StoreTaskType = iota
	EventLog
	BalanceNative
	BalanceErc20
	EventErc20Transfer
	EventErc721Transfer
	EventErc1155Transfer
	Contract
	ContractErc20
	ContractErc721
	TokenErc721
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
					data := make([]model.Tx, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.Tx))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventLog:
					data := make([]model.EventLog, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventLog))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc20Transfer:
					data := make([]model.EventErc20Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc20Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc721Transfer:
					data := make([]model.EventErc721Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc721Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case EventErc1155Transfer:
					data := make([]model.EventErc1155Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc1155Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case BalanceNative:
					data := make([]model.BalanceNative, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.BalanceNative))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case BalanceErc20:
					data := make([]model.BalanceErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.BalanceErc20))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case Contract:
					data := make([]model.Contract, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.Contract))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case ContractErc20:
					data := make([]model.ContractErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.ContractErc20))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case ContractErc721:
					data := make([]model.ContractErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.ContractErc721))
					}
					err = sw.db.Clauses(clause.OnConflict{UpdateAll: true}).Create(data).Error
				case TokenErc721:
					data := make([]model.TokenErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.TokenErc721))
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
