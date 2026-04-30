package store

import (
	"context"
	"errors"
	"scanner_eth/model"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type TaskType byte

const (
	Tx TaskType = iota
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

type Task struct {
	Ctx      context.Context
	Height   uint64
	TaskID   uint64
	TaskType TaskType
	Data     []interface{}
}

type Complete struct {
	TaskID uint64
	Err    error
}

type Worker struct {
	ID                   int
	DB                   *gorm.DB
	StoreTaskChannel     chan *Task
	StoreCompleteChannel chan *Complete
}

func NewWorker(id int, db *gorm.DB, storeTaskChannel chan *Task, storeCompleteChannel chan *Complete) *Worker {
	return &Worker{
		ID:                   id,
		DB:                   db,
		StoreTaskChannel:     storeTaskChannel,
		StoreCompleteChannel: storeCompleteChannel,
	}
}

func (sw *Worker) Run() {
	go func() {
		for {
			for tsk := range sw.StoreTaskChannel {
				tryCount := 0
			Retry:
				tryCount++

				var err error
				taskType := tsk.TaskType
				taskID := tsk.TaskID
				height := tsk.Height
				startTime := time.Now()

				workDB := sw.DB
				if tsk.Ctx != nil {
					workDB = sw.DB.WithContext(tsk.Ctx)
				}

				switch taskType {
				case Tx:
					data := make([]model.Tx, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.Tx))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case TxInternalRow:
					data := make([]model.TxInternal, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.TxInternal))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventLog:
					data := make([]model.EventLog, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.EventLog))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc20Transfer:
					data := make([]model.EventErc20Transfer, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.EventErc20Transfer))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc721Transfer:
					data := make([]model.EventErc721Transfer, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.EventErc721Transfer))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case EventErc1155Transfer:
					data := make([]model.EventErc1155Transfer, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.EventErc1155Transfer))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case BalanceNative:
					data := make([]model.BalanceNative, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.BalanceNative))
					}
					err = workDB.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"balance", "update_height"})}).Create(data).Error
				case BalanceErc20:
					data := make([]model.BalanceErc20, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.BalanceErc20))
					}
					err = workDB.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"balance", "update_height"})}).Create(data).Error
				case BalanceErc1155:
					data := make([]model.BalanceErc1155, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.BalanceErc1155))
					}
					err = workDB.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"addr", "balance", "update_height"})}).Create(data).Error
				case TokenErc721:
					data := make([]model.TokenErc721, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.TokenErc721))
					}
					err = workDB.Clauses(clause.OnConflict{DoUpdates: clause.AssignmentColumns([]string{"owner_addr", "token_uri", "token_meta_data", "update_height"})}).Create(data).Error
				case Contract:
					data := make([]model.Contract, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.Contract))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case ContractErc20:
					data := make([]model.ContractErc20, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.ContractErc20))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				case ContractErc721:
					data := make([]model.ContractErc721, 0)
					for _, v := range tsk.Data {
						data = append(data, v.(model.ContractErc721))
					}
					err = workDB.Clauses(clause.OnConflict{DoNothing: true}).Create(data).Error
				default:
					panic("unknown task type")
				}

				if err != nil {
					logrus.Errorf("store failed err:%v id:%v height:%v type:%v task_id:%v try_count:%v cost:%v",
						err, sw.ID, height, taskType, taskID, tryCount, time.Since(startTime).String())
					if tryCount <= 2 && !errors.Is(err, context.Canceled) {
						time.Sleep(100 * time.Millisecond)
						goto Retry
					}
				}

				sw.StoreCompleteChannel <- &Complete{TaskID: taskID, Err: err}
			}
		}
	}()
}

func SplitTasks(taskType TaskType, modelList []interface{}, batchSize int, nextTaskID func() uint64, height uint64) []*Task {
	var tasks []*Task
	count := len(modelList)
	round := count / batchSize
	left := count % batchSize

	for i := 0; i < round; i++ {
		taskID := nextTaskID()
		ay := modelList[batchSize*i : batchSize*(i+1)]
		tasks = append(tasks, &Task{
			Height:   height,
			TaskID:   taskID,
			TaskType: taskType,
			Data:     ay,
		})
	}
	if left > 0 {
		taskID := nextTaskID()
		ay := modelList[batchSize*round:]
		tasks = append(tasks, &Task{
			Height:   height,
			TaskID:   taskID,
			TaskType: taskType,
			Data:     ay,
		})
	}
	return tasks
}

func ToInterfaceSlice[T any](s []T) []interface{} {
	r := make([]interface{}, len(s))
	for i, v := range s {
		r[i] = v
	}
	return r
}
