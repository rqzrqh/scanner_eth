package store

import (
	"encoding/json"
	"os"
	"sync_eth/model"
	"sync_eth/protocol"
	"sync_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

type StorageFullBlock struct {
	Block                    model.Block
	TxList                   []model.Tx
	TxInternalList           []model.TxInternal
	EventLogList             []model.EventLog
	EventErc20TransferList   []model.EventErc20Transfer
	EventErc721TransferList  []model.EventErc721Transfer
	EventErc1155TransferList []model.EventErc1155Transfer
	TokenErc721List          []model.TokenErc721

	ContractList       []model.Contract
	ContractErc20List  []model.ContractErc20
	ContractErc721List []model.ContractErc721

	BalanceNativeList  []model.BalanceNative
	BalanceErc20List   []model.BalanceErc20
	BalanceErc1155List []model.BalanceErc1155
}

type StoreManager struct {
	db                      *gorm.DB
	batchSize               int
	storeOperationChannel   chan *types.StoreOperation
	storeTaskChannel        chan *StoreTask
	storeCompleteChannel    chan *StoreComplete
	publishOperationChannel chan<- *types.PublishOperation
	storeWorkers            []*StoreWorker
}

func NewStoreManager(db *gorm.DB, batchSize int, storeWorkerCount int, storeOperationChannel chan *types.StoreOperation,
	publishOperationChannel chan<- *types.PublishOperation) *StoreManager {

	storeTaskChannel := make(chan *StoreTask, 10)
	storeCompleteChannel := make(chan *StoreComplete, 10)

	storeWorkers := make([]*StoreWorker, storeWorkerCount)
	for i := 0; i < storeWorkerCount; i++ {
		worker := NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel)
		storeWorkers[i] = worker
	}

	return &StoreManager{
		db:                      db,
		batchSize:               batchSize,
		storeOperationChannel:   storeOperationChannel,
		storeTaskChannel:        storeTaskChannel,
		storeCompleteChannel:    storeCompleteChannel,
		publishOperationChannel: publishOperationChannel,
		storeWorkers:            storeWorkers,
	}
}

func (sm *StoreManager) Run() {
	for _, sw := range sm.storeWorkers {
		sw.Run()
	}

	go func() {
		for op := range sm.storeOperationChannel {
			switch op.Type {
			case types.StoreApply:
				data := op.Data.(*types.StoreApplyData)
				height := data.FullBlock.Block.Height

				storageFullBlock := convertStorageFullBlock(data.FullBlock)
				protocolFullBlock := convertProtocolFullBlock(data.FullBlock)
				var protocolFullBlockData []byte
				var err error
				if protocolFullBlockData, err = json.Marshal(protocolFullBlock); err != nil {
					logrus.Errorf("marshal protocol fullblock failed. height:%v err:%v", height, err)
					os.Exit(0)
				}

				var publishActionRecordId uint64
				tryCount := 0
				for {
					tryCount++
					startTime := time.Now()

					if publishActionRecordId, err = StoreFullBlock(sm.db, storageFullBlock, protocolFullBlockData, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel); err != nil {
						logrus.Errorf("db revert failed. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
						time.Sleep(3 * time.Second)
						continue
					}

					prevHash := data.FullBlock.Block.ParentHash
					blockHash := data.FullBlock.Block.BlockHash

					logrus.Infof("store fullblock success. height:%v hash:%v prev_hash:%v cost:%v",
						height, blockHash, prevHash, time.Since(startTime).String())

					publishOperation := &types.PublishOperation{
						Type:              types.PublishApply,
						Id:                publishActionRecordId,
						Height:            height,
						ProtocolFullBlock: protocolFullBlockData,
					}
					sm.publishOperationChannel <- publishOperation
					break
				}

			case types.StoreRollback:
				data := op.Data.(*types.StoreRollbackData)
				height := data.Height

				tryCount := 0
				for {
					tryCount++
					startTime := time.Now()
					// TODO: get publishActionRecordId from db
					var publishActionRecordId uint64
					publishActionRecordId = 0

					if err := Revert(sm.db, height); err != nil {
						logrus.Errorf("db revert failed. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
						time.Sleep(3 * time.Second)
						continue
					}

					logrus.Infof("store revert success. height:%v cost:%v",
						height, time.Since(startTime).String())

					publishOperation := &types.PublishOperation{
						Type:   types.PublishRollback,
						Id:     publishActionRecordId,
						Height: height,
					}
					sm.publishOperationChannel <- publishOperation
					break
				}

			case types.StorePublishSuccess:
				data := op.Data.(*types.StorePublishSuccessData)
				id := data.Id
				height := data.Height

				tryCount := 0
				for {
					tryCount++
					startTime := time.Now()

					if err := sm.db.Delete(&model.PublishAction{}, id).Error; err != nil {
						logrus.Errorf("delete publish action failed. id:%v err:%v tryCount:%v", id, err, tryCount)
						time.Sleep(3 * time.Second)
						continue
					}

					logrus.Infof("delete publish action success. height:%v cost:%v",
						height, time.Since(startTime).String())
					break
				}
			}
		}
	}()
}

func convertStorageFullBlock(fullblock *types.FullBlock) *StorageFullBlock {
	blockHeight := fullblock.Block.Height
	modelBlock := model.Block{
		Height:          fullblock.Block.Height,
		BlockHash:       fullblock.Block.BlockHash,
		ParentHash:      fullblock.Block.ParentHash,
		BlockTimestamp:  fullblock.Block.BlockTimestamp,
		TxsCount:        fullblock.Block.TxsCount,
		Miner:           fullblock.Block.Miner,
		Size:            fullblock.Block.Size,
		Nonce:           fullblock.Block.Nonce,
		BaseFee:         fullblock.Block.BaseFee,
		BurntFees:       fullblock.Block.BurntFees,
		GasLimit:        fullblock.Block.GasLimit,
		GasUsed:         fullblock.Block.GasUsed,
		UnclesCount:     fullblock.Block.UnclesCount,
		Difficulty:      fullblock.Block.Difficulty,
		TotalDifficulty: fullblock.Block.TotalDifficulty,
		StateRoot:       fullblock.Block.StateRoot,
		TransactionRoot: fullblock.Block.TransactionRoot,
		ReceiptRoot:     fullblock.Block.ReceiptRoot,
		ExtraData:       fullblock.Block.ExtraData,
	}

	txList := make([]model.Tx, 0, len(fullblock.TxList))
	for _, tx := range fullblock.TxList {
		modelTx := model.Tx{
			Height:               blockHeight,
			TxHash:               tx.TxHash,
			TxIndex:              tx.TxIndex,
			TxType:               tx.TxType,
			From:                 tx.From,
			To:                   tx.To,
			Nonce:                tx.Nonce,
			GasLimit:             tx.GasLimit,
			GasPrice:             tx.GasPrice,
			GasUsed:              tx.GasUsed,
			BaseFee:              tx.BaseFee,
			BurntFees:            tx.BurntFees,
			MaxFeePerGas:         tx.MaxFeePerGas,
			MaxPriorityFeePerGas: tx.MaxPriorityFeePerGas,
			Value:                tx.Value,
			Input:                tx.Input,
			ExecStatus:           tx.ExecStatus,
			IsCallContract:       tx.IsCallContract,
			IsCreateContract:     tx.IsCreateContract,
		}
		txList = append(txList, modelTx)
	}

	// TxInternalList conversion (if exists in types.FullBlock)
	txInternalList := make([]model.TxInternal, 0)
	// Uncomment and adjust if TxInternalList exists in fullblock:
	// for _, txInternal := range fullblock.TxInternalList {
	// 	modelTxInternal := model.TxInternal{
	// 		Height:    blockHeight,
	// 		BlockHash: blockHash,
	// 		// ... map other fields
	// 	}
	// 	txInternalList = append(txInternalList, modelTxInternal)
	// }

	eventLogList := make([]model.EventLog, 0, len(fullblock.EventLogList))
	for _, log := range fullblock.EventLogList {
		modelLog := model.EventLog{
			Height:       blockHeight,
			TxHash:       log.TxHash,
			ContractAddr: log.ContractAddr,
			TopicCount:   log.TopicCount,
			Topic0:       log.Topic0,
			Topic1:       log.Topic1,
			Topic2:       log.Topic2,
			Topic3:       log.Topic3,
			Data:         log.Data,
			Index:        log.Index,
		}
		eventLogList = append(eventLogList, modelLog)
	}

	eventErc20TransferList := make([]model.EventErc20Transfer, 0, len(fullblock.EventErc20TransferList))
	for _, transfer := range fullblock.EventErc20TransferList {
		modelTransfer := model.EventErc20Transfer{
			Height:       blockHeight,
			TxHash:       transfer.TxHash,
			ContractAddr: transfer.ContractAddr,
			From:         transfer.From,
			To:           transfer.To,
			Amount:       transfer.Amount,
			AmountOrigin: transfer.AmountOrigin,
			Index:        transfer.Index,
		}
		eventErc20TransferList = append(eventErc20TransferList, modelTransfer)
	}

	eventErc721TransferList := make([]model.EventErc721Transfer, 0, len(fullblock.EventErc721TransferList))
	for _, transfer := range fullblock.EventErc721TransferList {
		modelTransfer := model.EventErc721Transfer{
			Height:       blockHeight,
			TxHash:       transfer.TxHash,
			ContractAddr: transfer.ContractAddr,
			From:         transfer.From,
			To:           transfer.To,
			TokenId:      transfer.TokenId,
			Index:        transfer.Index,
		}
		eventErc721TransferList = append(eventErc721TransferList, modelTransfer)
	}

	eventErc1155TransferList := make([]model.EventErc1155Transfer, 0, len(fullblock.EventErc1155TransferList))
	for _, transfer := range fullblock.EventErc1155TransferList {
		modelTransfer := model.EventErc1155Transfer{
			Height:       blockHeight,
			TxHash:       transfer.TxHash,
			ContractAddr: transfer.ContractAddr,
			Operator:     transfer.Operator,
			From:         transfer.From,
			To:           transfer.To,
			TokenId:      transfer.TokenId,
			Amount:       transfer.Amount,
			Index:        transfer.Index,
			IndexInBatch: transfer.IndexInBatch,
		}
		eventErc1155TransferList = append(eventErc1155TransferList, modelTransfer)
	}

	tokenErc721List := make([]model.TokenErc721, 0, len(fullblock.TokenErc721List))
	for _, token := range fullblock.TokenErc721List {
		modelToken := model.TokenErc721{
			Height:        blockHeight,
			ContractAddr:  token.ContractAddr,
			TokenId:       token.TokenId,
			OwnerAddr:     token.OwnerAddr,
			TokenUri:      token.TokenUri,
			TokenMetaData: token.TokenMetaData,
			UpdateHeight:  blockHeight,
		}
		tokenErc721List = append(tokenErc721List, modelToken)
	}

	contractList := make([]model.Contract, 0, len(fullblock.ContractList))
	for _, contract := range fullblock.ContractList {
		modelContract := model.Contract{
			Height:       blockHeight,
			TxHash:       contract.TxHash,
			ContractAddr: contract.ContractAddr,
			CreatorAddr:  contract.CreatorAddr,
			ExecStatus:   contract.ExecStatus,
		}
		contractList = append(contractList, modelContract)
	}

	contractErc20List := make([]model.ContractErc20, 0, len(fullblock.ContractErc20List))
	for _, contract := range fullblock.ContractErc20List {
		modelContract := model.ContractErc20{
			Height:            blockHeight,
			TxHash:            contract.TxHash,
			ContractAddr:      contract.ContractAddr,
			CreatorAddr:       contract.CreatorAddr,
			Name:              contract.Name,
			Symbol:            contract.Symbol,
			Decimals:          contract.Decimals,
			TotalSupply:       contract.TotalSupply,
			TotalSupplyOrigin: contract.TotalSupplyOrigin,
		}
		contractErc20List = append(contractErc20List, modelContract)
	}

	contractErc721List := make([]model.ContractErc721, 0, len(fullblock.ContractErc721List))
	for _, contract := range fullblock.ContractErc721List {
		modelContract := model.ContractErc721{
			Height:       blockHeight,
			TxHash:       contract.TxHash,
			ContractAddr: contract.ContractAddr,
			CreatorAddr:  contract.CreatorAddr,
			Name:         contract.Name,
			Symbol:       contract.Symbol,
		}
		contractErc721List = append(contractErc721List, modelContract)
	}

	balanceNativeList := make([]model.BalanceNative, 0, len(fullblock.BalanceNativeList))
	for _, balance := range fullblock.BalanceNativeList {
		modelBalance := model.BalanceNative{
			Addr:         balance.Addr,
			Balance:      balance.Balance,
			UpdateHeight: blockHeight,
		}
		balanceNativeList = append(balanceNativeList, modelBalance)
	}

	balanceErc20List := make([]model.BalanceErc20, 0, len(fullblock.BalanceErc20List))
	for _, balance := range fullblock.BalanceErc20List {
		modelBalance := model.BalanceErc20{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			Balance:      balance.Balance,
			UpdateHeight: blockHeight,
		}
		balanceErc20List = append(balanceErc20List, modelBalance)
	}

	balanceErc1155List := make([]model.BalanceErc1155, 0, len(fullblock.BalanceErc1155List))
	for _, balance := range fullblock.BalanceErc1155List {
		modelBalance := model.BalanceErc1155{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			TokenId:      balance.TokenId,
			Balance:      balance.Balance,
			UpdateHeight: blockHeight,
		}
		balanceErc1155List = append(balanceErc1155List, modelBalance)
	}

	return &StorageFullBlock{
		Block:                    modelBlock,
		TxList:                   txList,
		TxInternalList:           txInternalList,
		EventLogList:             eventLogList,
		EventErc20TransferList:   eventErc20TransferList,
		EventErc721TransferList:  eventErc721TransferList,
		EventErc1155TransferList: eventErc1155TransferList,
		TokenErc721List:          tokenErc721List,
		ContractList:             contractList,
		ContractErc20List:        contractErc20List,
		ContractErc721List:       contractErc721List,
		BalanceNativeList:        balanceNativeList,
		BalanceErc20List:         balanceErc20List,
		BalanceErc1155List:       balanceErc1155List,
	}
}

func convertProtocolFullBlock(fullblock *types.FullBlock) *protocol.FullBlock {

	fullTxList := make([]*protocol.FullTx, 0, len(fullblock.TxList))
	for _, tx := range fullblock.TxList {
		fullEventLogList := make([]*protocol.FullEventLog, 0)
		for _, log := range fullblock.EventLogList {
			if log.TxHash == tx.TxHash {
				protocolEventLog := &protocol.EventLog{
					ContractAddr: log.ContractAddr,
					TopicCount:   log.TopicCount,
					Topic0:       log.Topic0,
					Topic1:       log.Topic1,
					Topic2:       log.Topic2,
					Topic3:       log.Topic3,
					Data:         log.Data,
					Index:        log.Index,
				}

				fullEventLog := &protocol.FullEventLog{
					EventLog: protocolEventLog,
				}

				// Check for ERC20 Transfer
				for _, erc20Transfer := range fullblock.EventErc20TransferList {
					if erc20Transfer.TxHash == tx.TxHash && erc20Transfer.Index == log.Index {
						fullEventLog.EventErc20Transfer = &protocol.EventErc20Transfer{
							ContractAddr: erc20Transfer.ContractAddr,
							From:         erc20Transfer.From,
							To:           erc20Transfer.To,
							Amount:       erc20Transfer.Amount,
							AmountOrigin: erc20Transfer.AmountOrigin,
							Index:        erc20Transfer.Index,
						}
						break
					}
				}

				// Check for ERC721 Transfer
				for _, erc721Transfer := range fullblock.EventErc721TransferList {
					if erc721Transfer.TxHash == tx.TxHash && erc721Transfer.Index == log.Index {
						fullEventLog.EventErc721Transfer = &protocol.EventErc721Transfer{
							ContractAddr: erc721Transfer.ContractAddr,
							From:         erc721Transfer.From,
							To:           erc721Transfer.To,
							TokenId:      erc721Transfer.TokenId,
							Index:        erc721Transfer.Index,
						}
						break
					}
				}

				// Check for ERC1155 Transfer
				for _, erc1155Transfer := range fullblock.EventErc1155TransferList {
					if erc1155Transfer.TxHash == tx.TxHash && erc1155Transfer.Index == log.Index {
						fullEventLog.EventErc1155Transfer = &protocol.EventErc1155Transfer{
							ContractAddr: erc1155Transfer.ContractAddr,
							Operator:     erc1155Transfer.Operator,
							From:         erc1155Transfer.From,
							To:           erc1155Transfer.To,
							TokenId:      erc1155Transfer.TokenId,
							Amount:       erc1155Transfer.Amount,
							Index:        erc1155Transfer.Index,
							IndexInBatch: erc1155Transfer.IndexInBatch,
						}
						break
					}
				}

				fullEventLogList = append(fullEventLogList, fullEventLog)
			}
		}

		txInternalListForTx := make([]*protocol.TxInternal, 0)
		for _, txInternal := range fullblock.TxInternalList {
			if txInternal.TxHash == tx.TxHash {
				txInternalListForTx = append(txInternalListForTx, &protocol.TxInternal{
					Index:        txInternal.Index,
					From:         txInternal.From,
					To:           txInternal.To,
					OpCode:       txInternal.OpCode,
					Value:        txInternal.Value,
					Success:      txInternal.Success,
					Depth:        txInternal.Depth,
					Gas:          txInternal.Gas,
					GasUsed:      txInternal.GasUsed,
					Input:        txInternal.Input,
					Output:       txInternal.Output,
					TraceAddress: txInternal.TraceAddress,
				})
			}
		}

		protocolTx := &protocol.Tx{
			TxHash:               tx.TxHash,
			TxIndex:              tx.TxIndex,
			TxType:               tx.TxType,
			From:                 tx.From,
			To:                   tx.To,
			Nonce:                tx.Nonce,
			GasLimit:             tx.GasLimit,
			GasPrice:             tx.GasPrice,
			GasUsed:              tx.GasUsed,
			BaseFee:              tx.BaseFee,
			BurntFees:            tx.BurntFees,
			MaxFeePerGas:         tx.MaxFeePerGas,
			MaxPriorityFeePerGas: tx.MaxPriorityFeePerGas,
			Value:                tx.Value,
			Input:                tx.Input,
			ExecStatus:           tx.ExecStatus,
			IsCallContract:       tx.IsCallContract,
			IsCreateContract:     tx.IsCreateContract,
		}

		fullTx := &protocol.FullTx{
			Tx:               protocolTx,
			FullEventLogList: fullEventLogList,
			TxInternalList:   txInternalListForTx,
		}
		fullTxList = append(fullTxList, fullTx)
	}

	tokenErc721List := make([]*protocol.TokenErc721, 0, len(fullblock.TokenErc721List))
	for _, token := range fullblock.TokenErc721List {
		tokenErc721List = append(tokenErc721List, &protocol.TokenErc721{
			ContractAddr:  token.ContractAddr,
			TokenId:       token.TokenId,
			OwnerAddr:     token.OwnerAddr,
			TokenUri:      token.TokenUri,
			TokenMetaData: token.TokenMetaData,
		})
	}

	contractList := make([]*protocol.Contract, 0, len(fullblock.ContractList))
	for _, contract := range fullblock.ContractList {
		contractList = append(contractList, &protocol.Contract{
			TxHash:       contract.TxHash,
			ContractAddr: contract.ContractAddr,
			CreatorAddr:  contract.CreatorAddr,
			ExecStatus:   contract.ExecStatus,
		})
	}

	contractErc20List := make([]*protocol.ContractErc20, 0, len(fullblock.ContractErc20List))
	for _, contract := range fullblock.ContractErc20List {
		contractErc20List = append(contractErc20List, &protocol.ContractErc20{
			TxHash:            contract.TxHash,
			ContractAddr:      contract.ContractAddr,
			CreatorAddr:       contract.CreatorAddr,
			Name:              contract.Name,
			Symbol:            contract.Symbol,
			Decimals:          contract.Decimals,
			TotalSupply:       contract.TotalSupply,
			TotalSupplyOrigin: contract.TotalSupplyOrigin,
		})
	}

	contractErc721List := make([]*protocol.ContractErc721, 0, len(fullblock.ContractErc721List))
	for _, contract := range fullblock.ContractErc721List {
		contractErc721List = append(contractErc721List, &protocol.ContractErc721{
			TxHash:       contract.TxHash,
			ContractAddr: contract.ContractAddr,
			CreatorAddr:  contract.CreatorAddr,
			Name:         contract.Name,
			Symbol:       contract.Symbol,
		})
	}

	balanceNativeList := make([]*protocol.BalanceNative, 0, len(fullblock.BalanceNativeList))
	for _, balance := range fullblock.BalanceNativeList {
		balanceNativeList = append(balanceNativeList, &protocol.BalanceNative{
			Addr:    balance.Addr,
			Balance: balance.Balance,
		})
	}

	balanceErc20List := make([]*protocol.BalanceErc20, 0, len(fullblock.BalanceErc20List))
	for _, balance := range fullblock.BalanceErc20List {
		balanceErc20List = append(balanceErc20List, &protocol.BalanceErc20{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			Balance:      balance.Balance,
		})
	}

	balanceErc1155List := make([]*protocol.BalanceErc1155, 0, len(fullblock.BalanceErc1155List))
	for _, balance := range fullblock.BalanceErc1155List {
		balanceErc1155List = append(balanceErc1155List, &protocol.BalanceErc1155{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			TokenId:      balance.TokenId,
			Balance:      balance.Balance,
		})
	}

	protocolFullBlock := &protocol.FullBlock{
		Height:          fullblock.Block.Height,
		BlockHash:       fullblock.Block.BlockHash,
		ParentHash:      fullblock.Block.ParentHash,
		BlockTimestamp:  fullblock.Block.BlockTimestamp,
		TxsCount:        fullblock.Block.TxsCount,
		Miner:           fullblock.Block.Miner,
		Size:            fullblock.Block.Size,
		Nonce:           fullblock.Block.Nonce,
		BaseFee:         fullblock.Block.BaseFee,
		BurntFees:       fullblock.Block.BurntFees,
		GasLimit:        fullblock.Block.GasLimit,
		GasUsed:         fullblock.Block.GasUsed,
		UnclesCount:     fullblock.Block.UnclesCount,
		Difficulty:      fullblock.Block.Difficulty,
		TotalDifficulty: fullblock.Block.TotalDifficulty,
		StateRoot:       fullblock.Block.StateRoot,
		TransactionRoot: fullblock.Block.TransactionRoot,
		ReceiptRoot:     fullblock.Block.ReceiptRoot,
		ExtraData:       fullblock.Block.ExtraData,
		FullTxList:      fullTxList,
		StateSet: &protocol.StateSet{
			TokenErc721List:    tokenErc721List,
			ContractList:       contractList,
			ContractErc20List:  contractErc20List,
			ContractErc721List: contractErc721List,
			BalanceNativeList:  balanceNativeList,
			BalanceErc20List:   balanceErc20List,
			BalanceErc1155List: balanceErc1155List,
		},
	}

	return protocolFullBlock
}
