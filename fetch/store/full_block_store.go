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

func (fullblock *StorageFullBlock) ensureReady(ctx context.Context, db *gorm.DB, chainID int64) error {
	return db.Where("chain_id = ?", chainID).First(&model.ScannerInfo{}).Error
}

func (fullblock *StorageFullBlock) insertOrReuseBlock(ctx context.Context, db *gorm.DB) (uint64, error) {
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

func (fullblock *StorageFullBlock) assignBlockID(blockID uint64) {
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

func (fullblock *StorageFullBlock) buildTasks(batchSize int, nextTaskID func() uint64) []*Task {
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

func (fullblock *StorageFullBlock) finalize(ctx context.Context, db *gorm.DB, blockID uint64) (uint64, error) {
	if fullblock == nil {
		return 0, gorm.ErrInvalidData
	}
	var scannerMsg *model.ScannerMessage
	reusedScannerMsg := false
	reusedCompletedBlock := false
	if err := db.Transaction(func(tx *gorm.DB) error {
		scannerMsg = &model.ScannerMessage{
			Height:     fullblock.Block.Height,
			Hash:       fullblock.Block.Hash,
			ParentHash: fullblock.Block.ParentHash,
			Pushed:     false,
		}
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(scannerMsg).Error; err != nil {
			logrus.Errorf("store scanner_message failed %v", err)
			return err
		}
		if scannerMsg.Id == 0 {
			if err := tx.Where("hash = ?", fullblock.Block.Hash).First(scannerMsg).Error; err != nil {
				logrus.Errorf("query scanner_message by hash failed %v", err)
				return err
			}
			reusedScannerMsg = true
		}

		updateResult := tx.Model(&model.Block{}).Where("id = ? AND complete = ?", blockID, false).Update("complete", true)
		if updateResult.Error != nil {
			logrus.Errorf("mark block complete failed %v", updateResult.Error)
			return updateResult.Error
		}
		if updateResult.RowsAffected == 0 {
			var storedBlock model.Block
			if err := tx.Select("hash", "complete").Where("id = ?", blockID).First(&storedBlock).Error; err != nil {
				logrus.Errorf("query block complete state failed. block_id:%v err:%v", blockID, err)
				return err
			}
			if storedBlock.Complete && storedBlock.Hash == fullblock.Block.Hash {
				reusedCompletedBlock = true
				return nil
			}
			logrus.Errorf("mark block complete failed, no rows affected. block_id:%v height:%v", blockID, fullblock.Block.Height)
			return xerrors.Errorf("mark block complete failed, no rows affected. block_id:%v", blockID)
		}
		return nil
	}); err != nil {
		return 0, err
	}
	if reusedScannerMsg {
		logrus.Debugf("reuse scanner_message by hash. height:%v hash:%v message_id:%v", fullblock.Block.Height, fullblock.Block.Hash, scannerMsg.Id)
	}
	if reusedCompletedBlock {
		logrus.Debugf("block already complete with same hash during finalize. height:%v hash:%v block_id:%v", fullblock.Block.Height, fullblock.Block.Hash, blockID)
	}
	return scannerMsg.Id, nil
}

func StoreFullBlock(ctx context.Context, db *gorm.DB, chainID int64, runtime *Runtime, handler *StorageFullBlock) (uint64, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime == nil {
		runtime = DefaultRuntime()
	}
	if handler == nil {
		return 0, gorm.ErrInvalidData
	}

	height := handler.Block.Height
	hash := handler.Block.Hash
	storeStartedAt := time.Now()

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	default:
	}

	dbc := db.WithContext(ctx)

	readyStartedAt := time.Now()
	if err := handler.ensureReady(ctx, dbc, chainID); err != nil {
		return 0, err
	}
	readyCost := time.Since(readyStartedAt)
	logrus.Debugf("store fullblock ready check. height:%v hash:%v cost:%v", height, hash, readyCost)

	blockRowStartedAt := time.Now()
	blockID, err := handler.insertOrReuseBlock(ctx, dbc)
	if err != nil {
		return 0, err
	}
	blockRowCost := time.Since(blockRowStartedAt)
	logrus.Debugf("store fullblock block row. height:%v hash:%v block_id:%v cost:%v", height, hash, blockID, blockRowCost)
	handler.assignBlockID(blockID)

	allTasks := handler.buildTasks(runtime.BatchSize(), runtime.NextTaskID)
	dataStartedAt := time.Now()
	if err := runtime.RunTasks(ctx, allTasks); err != nil {
		if err == ErrStoreFullBlockFailed {
			logrus.Errorf("store fullblock failed %v", height)
		}
		logrus.Errorf("store block data failed. height:%v hash:%v block_id:%v tasks:%v cost:%v err:%v", height, hash, blockID, len(allTasks), time.Since(storeStartedAt).String(), err)
		return 0, err
	}
	dataCost := time.Since(dataStartedAt)
	logrus.Debugf("store fullblock data tasks. height:%v hash:%v block_id:%v tasks:%v cost:%v", height, hash, blockID, len(allTasks), dataCost)

	if err := ctx.Err(); err != nil {
		return 0, err
	}

	finalizeStartTime := time.Now()
	messageID, err := handler.finalize(ctx, dbc, blockID)
	if err != nil {
		logrus.Errorf("finalize store fullblock failed %v", err)
		return 0, err
	}
	finalizeCost := time.Since(finalizeStartTime)
	totalCost := time.Since(storeStartedAt)

	logrus.Infof("store fullblock. height:%v hash:%v block_id:%v message_id:%v txs:%v internal_txs:%v event_logs:%v erc20_events:%v erc721_events:%v erc1155_events:%v contracts:%v erc20_contracts:%v erc721_contracts:%v native_balances:%v erc20_balances:%v erc1155_balances:%v tokens_erc721:%v tasks:%v ready_cost:%v block_row_cost:%v data_cost:%v finalize_cost:%v total_cost:%v",
		height,
		hash,
		blockID,
		messageID,
		len(handler.TxList),
		len(handler.TxInternalList),
		len(handler.EventLogList),
		len(handler.EventErc20TransferList),
		len(handler.EventErc721TransferList),
		len(handler.EventErc1155TransferList),
		len(handler.ContractList),
		len(handler.ContractErc20List),
		len(handler.ContractErc721List),
		len(handler.BalanceNativeList),
		len(handler.BalanceErc20List),
		len(handler.BalanceErc1155List),
		len(handler.TokenErc721List),
		len(allTasks),
		readyCost.String(),
		blockRowCost.String(),
		dataCost.String(),
		finalizeCost.String(),
		totalCost.String(),
	)
	return messageID, nil
}
