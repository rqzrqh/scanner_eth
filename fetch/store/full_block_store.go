package store

import (
	"context"
	"time"

	"scanner_eth/model"

	"github.com/sirupsen/logrus"
	"golang.org/x/xerrors"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type StorageFullBlock struct {
	Block                    model.Block
	TxList                   []model.Tx
	TxInternalList           []model.TxInternal
	EventLogList             []model.EventLog
	EventErc20TransferList   []model.EventErc20Transfer
	EventErc721TransferList  []model.EventErc721Transfer
	EventErc1155TransferList []model.EventErc1155Transfer

	ContractList       []model.Contract
	ContractErc20List  []model.ContractErc20
	ContractErc721List []model.ContractErc721

	BalanceNativeList  []model.BalanceNative
	BalanceErc20List   []model.BalanceErc20
	BalanceErc1155List []model.BalanceErc1155
	TokenErc721List    []model.TokenErc721
}

type FullBlockHandler interface {
	Height() uint64
	EnsureReady(context.Context, *gorm.DB, int64) error
	InsertOrReuseBlock(context.Context, *gorm.DB) (uint64, error)
	AssignBlockID(uint64)
	BuildTasks(int, func() uint64) []*Task
	Finalize(context.Context, *gorm.DB, uint64) (uint64, error)
}

func (fullblock *StorageFullBlock) Height() uint64 {
	if fullblock == nil {
		return 0
	}
	return fullblock.Block.Height
}

func (fullblock *StorageFullBlock) EnsureReady(ctx context.Context, db *gorm.DB, chainID int64) error {
	return db.Where("chain_id = ?", chainID).First(&model.ScannerInfo{}).Error
}

func (fullblock *StorageFullBlock) InsertOrReuseBlock(ctx context.Context, db *gorm.DB) (uint64, error) {
	if fullblock == nil {
		return 0, gorm.ErrInvalidData
	}
	height := fullblock.Block.Height
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
	if storedBlock.Id == 0 {
		logrus.Errorf("block_id is 0, height:%v", height)
		return 0, xerrors.New("block_id cannot be empty")
	}
	return storedBlock.Id, nil
}

func (fullblock *StorageFullBlock) AssignBlockID(blockID uint64) {
	if fullblock == nil {
		return
	}
	fullblock.Block.Id = blockID
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

func (fullblock *StorageFullBlock) BuildTasks(batchSize int, nextTaskID func() uint64) []*Task {
	if fullblock == nil {
		return nil
	}
	height := fullblock.Block.Height
	var allTasks []*Task
	allTasks = append(allTasks, SplitTasks(Tx, ToInterfaceSlice(fullblock.TxList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(TxInternalRow, ToInterfaceSlice(fullblock.TxInternalList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(EventLog, ToInterfaceSlice(fullblock.EventLogList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(EventErc20Transfer, ToInterfaceSlice(fullblock.EventErc20TransferList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(EventErc721Transfer, ToInterfaceSlice(fullblock.EventErc721TransferList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(EventErc1155Transfer, ToInterfaceSlice(fullblock.EventErc1155TransferList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(BalanceNative, ToInterfaceSlice(fullblock.BalanceNativeList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(BalanceErc20, ToInterfaceSlice(fullblock.BalanceErc20List), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(BalanceErc1155, ToInterfaceSlice(fullblock.BalanceErc1155List), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(TokenErc721, ToInterfaceSlice(fullblock.TokenErc721List), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(Contract, ToInterfaceSlice(fullblock.ContractList), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(ContractErc20, ToInterfaceSlice(fullblock.ContractErc20List), batchSize, nextTaskID, height)...)
	allTasks = append(allTasks, SplitTasks(ContractErc721, ToInterfaceSlice(fullblock.ContractErc721List), batchSize, nextTaskID, height)...)
	return allTasks
}

func (fullblock *StorageFullBlock) Finalize(ctx context.Context, db *gorm.DB, blockID uint64) (uint64, error) {
	if fullblock == nil {
		return 0, gorm.ErrInvalidData
	}
	var scannerMsg *model.ScannerMessage
	if err := db.Transaction(func(tx *gorm.DB) error {
		scannerMsg = &model.ScannerMessage{
			Height:     fullblock.Block.Height,
			Hash:       fullblock.Block.Hash,
			ParentHash: fullblock.Block.ParentHash,
			Pushed:     false,
		}
		if err := tx.Create(scannerMsg).Error; err != nil {
			logrus.Errorf("store scanner_message failed %v", err)
			return err
		}

		updateResult := tx.Model(&model.Block{}).Where("id = ? AND complete = ?", blockID, false).Update("complete", true)
		if updateResult.Error != nil {
			logrus.Errorf("mark block complete failed %v", updateResult.Error)
			return updateResult.Error
		}
		if updateResult.RowsAffected == 0 {
			logrus.Errorf("mark block complete failed, no rows affected. block_id:%v height:%v", blockID, fullblock.Block.Height)
			return xerrors.Errorf("mark block complete failed, no rows affected. block_id:%v", blockID)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	return scannerMsg.Id, nil
}

func StoreFullBlock(ctx context.Context, db *gorm.DB, chainID int64, runtime *Runtime, handler FullBlockHandler) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime == nil {
		runtime = DefaultRuntime()
	}
	if handler == nil {
		return 0, gorm.ErrInvalidData
	}

	height := handler.Height()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	dbc := db.WithContext(ctx)

	if err := handler.EnsureReady(ctx, dbc, chainID); err != nil {
		return 0, err
	}

	blockID, err := handler.InsertOrReuseBlock(ctx, dbc)
	if err != nil {
		return 0, err
	}
	handler.AssignBlockID(blockID)

	allTasks := handler.BuildTasks(runtime.BatchSize(), runtime.NextTaskID)
	if err := runtime.RunTasks(ctx, allTasks); err != nil {
		if err == ErrStoreFullBlockFailed {
			logrus.Errorf("store fullblock failed %v", height)
		}
		return 0, err
	}

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	startTime := time.Now()
	messageID, err := handler.Finalize(ctx, dbc, blockID)
	if err != nil {
		logrus.Errorf("finalize store fullblock failed %v", err)
		return 0, err
	}

	logrus.Infof("store block. height:%v cost:%v message_id:%v", height, time.Since(startTime).String(), messageID)
	return messageID, nil
}
