package fetch

import (
	"os"
	"scanner_eth/model"
	"scanner_eth/protocol"
	"time"

	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var (
	taskCounter          = uint64(0)
	batchSize            int
	storeTaskChannel     chan *StoreTask
	storeCompleteChannel chan *StoreComplete
)

func InitStore(db *gorm.DB, _batchSize int, _workerCount int) {
	batchSize = _batchSize
	if _batchSize <= 0 {
		batchSize = 128
	}

	workerCount := _workerCount
	if workerCount <= 0 {
		workerCount = 8
	}

	storeTaskChannel := make(chan *StoreTask, workerCount*2)
	storeCompleteChannel := make(chan *StoreComplete, workerCount*2)
	for i := 0; i < workerCount; i++ {
		NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel).Run()
	}
}

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

		// Delete Contract
		if err := tx.Delete(&model.Contract{}, "height = ?", height).Error; err != nil {
			logrus.Errorf("delete contract failed %v", err)
			return err
		}

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

func assignStorageBlockID(fullblock *StorageFullBlock, blockID uint64) {
	for i := range fullblock.TxList {
		fullblock.TxList[i].BlockId = blockID
	}
	for i := range fullblock.TxInternalList {
		fullblock.TxInternalList[i].BlockId = blockID
	}
	for i := range fullblock.EventLogList {
		fullblock.EventLogList[i].BlockId = blockID
	}
	for i := range fullblock.EventErc20TransferList {
		fullblock.EventErc20TransferList[i].BlockId = blockID
	}
	for i := range fullblock.EventErc721TransferList {
		fullblock.EventErc721TransferList[i].BlockId = blockID
	}
	for i := range fullblock.EventErc1155TransferList {
		fullblock.EventErc1155TransferList[i].BlockId = blockID
	}
	for i := range fullblock.ContractList {
		fullblock.ContractList[i].BlockId = blockID
	}
}

func StoreFullBlock(db *gorm.DB, fullblock *StorageFullBlock, chainBinlog *protocol.ChainBinlog, binlogData []byte) (uint64, error) {

	height := fullblock.Block.Height

	var blockID uint64
	if err := db.Transaction(func(tx *gorm.DB) error {
		fullblock.Block.Complete = false
		if err := tx.Create(&fullblock.Block).Error; err != nil {
			logrus.Errorf("store chain block failed %v", err)
			return err
		}
		blockID = fullblock.Block.Id

		var prevBlock model.Block
		if err := tx.Where("height = ?", height-1).First(&prevBlock).Error; err != nil {
			logrus.Errorf("get prev block failed %v", err)
			return err
		}
		if prevBlock.Hash != fullblock.Block.ParentHash {
			logrus.Fatalf("prev block hash not match. height:%v prev_hash:%v current_parent_hash:%v", height, prevBlock.Hash, fullblock.Block.ParentHash)
			os.Exit(0)
			return xerrors.New("prev block hash not match")
		}
		return nil
	}); err != nil {
		logrus.Errorf("store block row failed %v", err)
		return 0, err
	}

	if blockID == 0 {
		logrus.Errorf("block_id is 0, height:%v", height)
		return 0, xerrors.New("block_id cannot be empty")
	}
	assignStorageBlockID(fullblock, blockID)

	var allTasks []*StoreTask
	allTasks = append(allTasks, splitTask(Tx, toInterfaceSlice(fullblock.TxList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(TxInternalRow, toInterfaceSlice(fullblock.TxInternalList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(EventLog, toInterfaceSlice(fullblock.EventLogList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(EventErc20Transfer, toInterfaceSlice(fullblock.EventErc20TransferList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(EventErc721Transfer, toInterfaceSlice(fullblock.EventErc721TransferList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(EventErc1155Transfer, toInterfaceSlice(fullblock.EventErc1155TransferList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(BalanceNative, toInterfaceSlice(fullblock.BalanceNativeList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(BalanceErc20, toInterfaceSlice(fullblock.BalanceErc20List), batchSize, height)...)
	allTasks = append(allTasks, splitTask(BalanceErc1155, toInterfaceSlice(fullblock.BalanceErc1155List), batchSize, height)...)
	allTasks = append(allTasks, splitTask(TokenErc721, toInterfaceSlice(fullblock.TokenErc721List), batchSize, height)...)
	allTasks = append(allTasks, splitTask(Contract, toInterfaceSlice(fullblock.ContractList), batchSize, height)...)
	allTasks = append(allTasks, splitTask(ContractErc20, toInterfaceSlice(fullblock.ContractErc20List), batchSize, height)...)
	allTasks = append(allTasks, splitTask(ContractErc721, toInterfaceSlice(fullblock.ContractErc721List), batchSize, height)...)

	taskSet := make(map[uint64]struct{})
	for _, t := range allTasks {
		taskSet[t.taskID] = struct{}{}
	}

	go func() {
		for _, t := range allTasks {
			storeTaskChannel <- t
		}
	}()

	storeFailed := false

	go func() {
	Loop:
		for {
			select {
			case c := <-storeCompleteChannel:
				if c.err != nil {
					storeFailed = true
				}

				delete(taskSet, c.taskID)
				if len(taskSet) == 0 {
					break Loop
				}
			}
		}
	}()

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

		if err := tx.Model(&model.Block{}).Where("id = ?", blockID).Update("complete", true).Error; err != nil {
			logrus.Errorf("mark block complete failed %v", err)
			return err
		}

		return nil
	}); err != nil {
		logrus.Errorf("finalize store fullblock failed %v", err)
		return 0, err
	}

	logrus.Debugf("store block. height:%v cost:%v id:%v", height, time.Since(startTime2).String(), modelChainBinlog.Id)

	return modelChainBinlog.Id, nil
}

func splitTask(taskType StoreTaskType, modelList []interface{}, batchSize int, height uint64) []*StoreTask {
	var tasks []*StoreTask
	count := len(modelList)
	round := count / batchSize
	left := count % batchSize

	for i := 0; i < round; i++ {
		taskCounter++
		ay := modelList[batchSize*i : batchSize*(i+1)]
		tasks = append(tasks, &StoreTask{
			height:   height,
			taskID:   taskCounter,
			taskType: taskType,
			data:     ay,
		})
	}
	if left > 0 {
		taskCounter++
		ay := modelList[batchSize*round:]
		tasks = append(tasks, &StoreTask{
			height:   height,
			taskID:   taskCounter,
			taskType: taskType,
			data:     ay,
		})
	}
	return tasks
}

func toInterfaceSlice[T any](s []T) []interface{} {
	r := make([]interface{}, len(s))
	for i, v := range s {
		r[i] = v
	}
	return r
}

type StoreTaskType byte

const (
	Tx StoreTaskType = iota
	EventLog
	BalanceNative
	BalanceErc20
	BalanceErc1155
	TokenErc721
	EventErc20Transfer
	EventErc721Transfer
	EventErc1155Transfer
	Contract
	ContractErc20
	ContractErc721
	TxInternalRow
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
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case TxInternalRow:
					data := make([]model.TxInternal, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.TxInternal))
					}
					err = sw.db.Create(data).Error
				case EventLog:
					data := make([]model.EventLog, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventLog))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc20Transfer:
					data := make([]model.EventErc20Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc20Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc721Transfer:
					data := make([]model.EventErc721Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc721Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc1155Transfer:
					data := make([]model.EventErc1155Transfer, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.EventErc1155Transfer))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case BalanceNative:
					data := make([]model.BalanceNative, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.BalanceNative))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"balance", "update_height"})}).Create(data).Error
				case BalanceErc20:
					data := make([]model.BalanceErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.BalanceErc20))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"balance", "update_height"})}).Create(data).Error
				case BalanceErc1155:
					data := make([]model.BalanceErc1155, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.BalanceErc1155))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"addr", "balance", "update_height"})}).Create(data).Error
				case TokenErc721:
					data := make([]model.TokenErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.TokenErc721))
					}
					// update
					err = sw.db.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"owner_addr", "token_uri", "token_meta_data", "update_height"})}).Create(data).Error
				case Contract:
					data := make([]model.Contract, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.Contract))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case ContractErc20:
					data := make([]model.ContractErc20, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.ContractErc20))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case ContractErc721:
					data := make([]model.ContractErc721, 0)
					for _, v := range tsk.data {
						data = append(data, v.(model.ContractErc721))
					}
					err = sw.db.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
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
