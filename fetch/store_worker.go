package fetch

import (
	"encoding/json"
	"scanner_eth/model"
	"scanner_eth/protocol"
	"sync"
	"sync/atomic"
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
	storeTaskMu          sync.Mutex
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

	storeTaskChannel = make(chan *StoreTask, workerCount*2)
	storeCompleteChannel = make(chan *StoreComplete, workerCount*2)
	for i := 0; i < workerCount; i++ {
		NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel).Run()
	}
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

func StoreFullBlock(db *gorm.DB, fullblock *StorageFullBlock, protocolBlock *protocol.FullBlock) (uint64, error) {
	storeTaskMu.Lock()
	defer storeTaskMu.Unlock()

	height := fullblock.Block.Height
	if storeTaskChannel == nil || storeCompleteChannel == nil {
		return 0, xerrors.New("store worker is not initialized")
	}

	var si model.ScannerInfo
	if err := db.Where("chain_id = ?", protocolBlock.ChainId).First(&si).Error; err != nil {
		return 0, err
	}

	protocolBlock.MessageId = si.MessageId + 1

	protocolData, err := json.Marshal(protocolBlock)
	if err != nil {
		return 0, err
	}

	var blockID uint64
	fullblock.Block.Complete = false
	if err := db.Clauses(clause.OnConflict{DoNothing: true}).Create(&fullblock.Block).Error; err != nil {
		logrus.Errorf("store chain block failed %v", err)
		logrus.Errorf("store block row failed %v", err)
		return 0, err
	}

	var storedBlock model.Block
	if err := db.Where("height = ?", height).First(&storedBlock).Error; err != nil {
		logrus.Errorf("query block row failed. height:%v err:%v", height, err)
		return 0, err
	}
	if storedBlock.Complete {
		logrus.Errorf("block already complete. height:%v hash:%v", storedBlock.Height, storedBlock.Hash)
		return 0, xerrors.Errorf("block already complete. height:%v", storedBlock.Height)
	}
	blockID = storedBlock.Id
	fullblock.Block.Id = blockID

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

	for _, t := range allTasks {
		storeTaskChannel <- t
	}

	storeFailed := false
	for len(taskSet) > 0 {
		c := <-storeCompleteChannel
		if c == nil {
			continue
		}
		if _, ok := taskSet[c.taskID]; !ok {
			continue
		}
		if c.err != nil {
			storeFailed = true
		}

		delete(taskSet, c.taskID)
	}

	if storeFailed {
		logrus.Errorf("store fullblock failed %v", height)
		return 0, xerrors.New("store fullblock failed")
	}

	startTime2 := time.Now()

	modelChainEvent := &model.ChainBinlog{
		MessageId:  protocolBlock.MessageId,
		Height:     protocolBlock.Block.Height,
		Hash:       protocolBlock.Block.Hash,
		ParentHash: protocolBlock.Block.ParentHash,
		Data:       protocolData,
	}

	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(modelChainEvent).Error; err != nil {
			logrus.Errorf("store chain event failed %v", err)
			return err
		}

		if err := tx.Model(&model.ScannerInfo{}).Where("chain_id = ?", protocolBlock.ChainId).Update("message_id", modelChainEvent.MessageId).Error; err != nil {
			logrus.Errorf("update scanner info failed %v", err)
			return err
		}

		updateResult := tx.Model(&model.Block{}).Where("id = ? AND complete = ?", blockID, false).Update("complete", true)
		if updateResult.Error != nil {
			logrus.Errorf("mark block complete failed %v", updateResult.Error)
			return updateResult.Error
		}
		if updateResult.RowsAffected == 0 {
			logrus.Errorf("mark block complete failed, no rows affected. block_id:%v height:%v", blockID, height)
			return xerrors.Errorf("mark block complete failed, no rows affected. block_id:%v", blockID)
		}

		return nil
	}); err != nil {
		logrus.Errorf("finalize store fullblock failed %v", err)
		return 0, err
	}

	logrus.Infof("store block. height:%v cost:%v id:%v", height, time.Since(startTime2).String(), modelChainEvent.Id)

	return modelChainEvent.Id, nil
}

func splitTask(taskType StoreTaskType, modelList []interface{}, batchSize int, height uint64) []*StoreTask {
	var tasks []*StoreTask
	count := len(modelList)
	round := count / batchSize
	left := count % batchSize

	for i := 0; i < round; i++ {
		taskID := atomic.AddUint64(&taskCounter, 1)
		ay := modelList[batchSize*i : batchSize*(i+1)]
		tasks = append(tasks, &StoreTask{
			height:   height,
			taskID:   taskID,
			taskType: taskType,
			data:     ay,
		})
	}
	if left > 0 {
		taskID := atomic.AddUint64(&taskCounter, 1)
		ay := modelList[batchSize*round:]
		tasks = append(tasks, &StoreTask{
			height:   height,
			taskID:   taskID,
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
