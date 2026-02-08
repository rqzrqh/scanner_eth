package store

import (
	"encoding/json"
	"os"
	"scanner_eth/model"
	"scanner_eth/protocol"
	"scanner_eth/types"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

var (
	optinalTx                   bool
	optinalTxInternal           bool
	optinalEventLog             bool
	optinalBalanceNative        bool
	optinalBalanceErc20         bool
	optinalBalanceErc1155       bool
	optinalEventErc20Transfer   bool
	optinalEventErc721Transfer  bool
	optinalEventErc1155Transfer bool
	optinalContract             bool
	optinalContractErc20        bool
	optinalContractErc721       bool
	optinalTokenErc721          bool
)

func SetOptionalFeatures(optinalFeature map[string]struct{}) {
	_, optinalTx = optinalFeature[model.Tx.TableName(model.Tx{})]
	_, optinalTxInternal = optinalFeature[model.TxInternal.TableName(model.TxInternal{})]
	_, optinalEventLog = optinalFeature[model.EventLog.TableName(model.EventLog{})]
	_, optinalBalanceNative = optinalFeature[model.BalanceNative.TableName(model.BalanceNative{})]
	_, optinalBalanceErc20 = optinalFeature[model.BalanceErc20.TableName(model.BalanceErc20{})]
	_, optinalBalanceErc1155 = optinalFeature[model.BalanceErc1155.TableName(model.BalanceErc1155{})]
	_, optinalEventErc20Transfer = optinalFeature[model.EventErc20Transfer.TableName(model.EventErc20Transfer{})]
	_, optinalEventErc721Transfer = optinalFeature[model.EventErc721Transfer.TableName(model.EventErc721Transfer{})]
	_, optinalEventErc1155Transfer = optinalFeature[model.EventErc1155Transfer.TableName(model.EventErc1155Transfer{})]
	_, optinalContract = optinalFeature[model.Contract.TableName(model.Contract{})]
	_, optinalContractErc20 = optinalFeature[model.ContractErc20.TableName(model.ContractErc20{})]
	_, optinalContractErc721 = optinalFeature[model.ContractErc721.TableName(model.ContractErc721{})]
	_, optinalTokenErc721 = optinalFeature[model.TokenErc721.TableName(model.TokenErc721{})]
}

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

type StoreManager struct {
	db                              *gorm.DB
	chainId                         int64
	messageId                       uint64
	batchSize                       int
	storeOperationChannel           chan *types.StoreOperation
	publishFeedbackOperationChannel <-chan *types.PublishFeedbackOperation
	storeTaskChannel                chan *StoreTask
	storeCompleteChannel            chan *StoreComplete
	publishOperationChannel         chan<- *types.PublishOperation
	storeWorkers                    []*StoreWorker
}

func NewStoreManager(db *gorm.DB, chainId int64, messageId uint64, batchSize int, storeWorkerCount int, storeOperationChannel chan *types.StoreOperation, publishFeedbackOperationChannel <-chan *types.PublishFeedbackOperation,
	publishOperationChannel chan<- *types.PublishOperation) *StoreManager {

	storeTaskChannel := make(chan *StoreTask, 10)
	storeCompleteChannel := make(chan *StoreComplete, 10)

	storeWorkers := make([]*StoreWorker, storeWorkerCount)
	for i := 0; i < storeWorkerCount; i++ {
		worker := NewStoreWorker(i, db, storeTaskChannel, storeCompleteChannel)
		storeWorkers[i] = worker
	}

	return &StoreManager{
		chainId:                         chainId,
		messageId:                       messageId,
		db:                              db,
		batchSize:                       batchSize,
		storeOperationChannel:           storeOperationChannel,
		publishFeedbackOperationChannel: publishFeedbackOperationChannel,
		storeTaskChannel:                storeTaskChannel,
		storeCompleteChannel:            storeCompleteChannel,
		publishOperationChannel:         publishOperationChannel,
		storeWorkers:                    storeWorkers,
	}
}

func (sm *StoreManager) Run() {
	for _, sw := range sm.storeWorkers {
		sw.Run()
	}

	go func() {

		for {
			select {
			case op := <-sm.storeOperationChannel:
				switch op.Type {
				case types.StoreApply:
					fullblock := op.FullBlock
					height := op.Height

					sm.messageId++

					storageFullBlock := convertStorageFullBlock(fullblock)
					protocolFullBlock := convertProtocolFullBlock(fullblock)

					chainBinlog := &protocol.ChainBinlog{
						ChainId:    sm.chainId,
						MessageId:  sm.messageId,
						ActionType: protocol.ChainActionApply,
						Height:     height,
						FullBlock:  protocolFullBlock,
					}

					var binlogData []byte
					var err error
					if binlogData, err = json.Marshal(chainBinlog); err != nil {
						logrus.Errorf("marshal protocol fullblock failed. height:%v err:%v", height, err)
						os.Exit(0)
					}

					tryCount := 0
					for {
						tryCount++
						startTime := time.Now()

						var binlogRecordId uint64
						if binlogRecordId, err = StoreFullBlock(sm.db, storageFullBlock, chainBinlog, binlogData, sm.batchSize, sm.storeTaskChannel, sm.storeCompleteChannel); err != nil {
							logrus.Errorf("store fullblock failed. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
							time.Sleep(3 * time.Second)
							continue
						}

						prevHash := fullblock.Block.ParentHash
						blockHash := fullblock.Block.Hash

						logrus.Infof("store fullblock success. height:%v hash:%v prev_hash:%v cost:%v",
							height, blockHash, prevHash, time.Since(startTime).String())

						publishOperation := &types.PublishOperation{
							BinlogRecordId: binlogRecordId,
							MessageId:      chainBinlog.MessageId,
							Height:         height,
							BinlogData:     binlogData,
						}
						sm.publishOperationChannel <- publishOperation
						break
					}

				case types.StoreRollback:
					height := op.Height

					sm.messageId++

					var err error
					tryCount := 0
					for {
						tryCount++
						startTime := time.Now()

						chainBinlog := &protocol.ChainBinlog{
							ChainId:    sm.chainId,
							MessageId:  sm.messageId,
							ActionType: protocol.ChainActionRollback,
							Height:     height,
						}

						var binlogData []byte
						if binlogData, err = json.Marshal(chainBinlog); err != nil {
							logrus.Errorf("marshal protocol fullblock failed. height:%v err:%v", height, err)
							os.Exit(0)
						}

						var binlogRecordId uint64
						if binlogRecordId, err = Revert(sm.db, height, chainBinlog, binlogData); err != nil {
							logrus.Errorf("store revert failed. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
							time.Sleep(3 * time.Second)
							continue
						}

						logrus.Infof("store revert success. height:%v cost:%v",
							height, time.Since(startTime).String())

						publishOperation := &types.PublishOperation{
							BinlogRecordId: binlogRecordId,
							MessageId:      sm.messageId,
							Height:         height,
						}
						sm.publishOperationChannel <- publishOperation
						break
					}
				}
			case op := <-sm.publishFeedbackOperationChannel:
				binlogRecordId := op.BinlogRecordId
				messageId := op.MessageId
				height := op.Height

				tryCount := 0
				for {
					tryCount++
					startTime := time.Now()

					if err := sm.db.Delete(&model.ChainBinlog{}, binlogRecordId).Error; err != nil {
						logrus.Errorf("delete chain binlog failed. binlog_record_id:%v message_id:%v height:%v err:%v tryCount:%v", binlogRecordId, messageId, height, err, tryCount)
						time.Sleep(3 * time.Second)
						continue
					}

					logrus.Infof("delete chain binlog success. binlog_record_id:%v message_id:%v height:%v cost:%v",
						binlogRecordId, messageId, height, time.Since(startTime).String())
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
		Hash:            fullblock.Block.Hash,
		ParentHash:      fullblock.Block.ParentHash,
		Timestamp:       fullblock.Block.Timestamp,
		TxCount:         fullblock.Block.TxCount,
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
	if optinalTx {
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
	}

	// TxInternalList conversion (if exists in types.FullBlock)
	txInternalList := make([]model.TxInternal, 0)
	if optinalTxInternal {
		// for _, txInternal := range fullblock.TxInternalList {
		// 	modelTxInternal := model.TxInternal{
		// 		Height:    blockHeight,
		// 		BlockHash: blockHash,
		// 		// ... map other fields
		// 	}
		// 	txInternalList = append(txInternalList, modelTxInternal)
		// }
	}

	eventLogList := make([]model.EventLog, 0, len(fullblock.EventLogList))
	if optinalEventLog {
		for _, log := range fullblock.EventLogList {
			modelLog := model.EventLog{
				Height:       blockHeight,
				TxHash:       log.TxHash,
				IndexInTx:    log.IndexInTx,
				IndexInBlock: log.IndexInBlock,
				ContractAddr: log.ContractAddr,
				TopicCount:   log.TopicCount,
				Topic0:       log.Topic0,
				Topic1:       log.Topic1,
				Topic2:       log.Topic2,
				Topic3:       log.Topic3,
				Data:         log.Data,
			}
			eventLogList = append(eventLogList, modelLog)
		}
	}

	eventErc20TransferList := make([]model.EventErc20Transfer, 0, len(fullblock.EventErc20TransferList))
	if optinalEventErc20Transfer {
		for _, transfer := range fullblock.EventErc20TransferList {
			modelTransfer := model.EventErc20Transfer{
				Height:       blockHeight,
				TxHash:       transfer.TxHash,
				IndexInBlock: transfer.IndexInBlock,
				ContractAddr: transfer.ContractAddr,
				From:         transfer.From,
				To:           transfer.To,
				Amount:       transfer.Amount,
			}
			eventErc20TransferList = append(eventErc20TransferList, modelTransfer)
		}
	}

	eventErc721TransferList := make([]model.EventErc721Transfer, 0, len(fullblock.EventErc721TransferList))
	if optinalEventErc721Transfer {
		for _, transfer := range fullblock.EventErc721TransferList {
			modelTransfer := model.EventErc721Transfer{
				Height:       blockHeight,
				TxHash:       transfer.TxHash,
				IndexInBlock: transfer.IndexInBlock,
				ContractAddr: transfer.ContractAddr,
				From:         transfer.From,
				To:           transfer.To,
				TokenId:      transfer.TokenId,
			}
			eventErc721TransferList = append(eventErc721TransferList, modelTransfer)
		}
	}

	eventErc1155TransferList := make([]model.EventErc1155Transfer, 0, len(fullblock.EventErc1155TransferList))
	if optinalEventErc1155Transfer {
		for _, transfer := range fullblock.EventErc1155TransferList {
			modelTransfer := model.EventErc1155Transfer{
				Height:       blockHeight,
				TxHash:       transfer.TxHash,
				IndexInBlock: transfer.IndexInBlock,
				IndexInBatch: transfer.IndexInBatch,
				ContractAddr: transfer.ContractAddr,
				Operator:     transfer.Operator,
				From:         transfer.From,
				To:           transfer.To,
				TokenId:      transfer.TokenId,
				Amount:       transfer.Amount,
			}
			eventErc1155TransferList = append(eventErc1155TransferList, modelTransfer)
		}
	}

	contractList := make([]model.Contract, 0, len(fullblock.ContractList))
	if optinalContract {
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
	}

	contractErc20List := make([]model.ContractErc20, 0, len(fullblock.ContractErc20List))
	if optinalContractErc20 {
		for _, contract := range fullblock.ContractErc20List {
			modelContract := model.ContractErc20{
				ContractAddr: contract.ContractAddr,
				Name:         contract.Name,
				Symbol:       contract.Symbol,
				Decimals:     contract.Decimals,
				TotalSupply:  contract.TotalSupply,
			}
			contractErc20List = append(contractErc20List, modelContract)
		}
	}

	contractErc721List := make([]model.ContractErc721, 0, len(fullblock.ContractErc721List))
	if optinalContractErc721 {
		for _, contract := range fullblock.ContractErc721List {
			modelContract := model.ContractErc721{
				ContractAddr: contract.ContractAddr,
				Name:         contract.Name,
				Symbol:       contract.Symbol,
			}
			contractErc721List = append(contractErc721List, modelContract)
		}
	}

	balanceNativeList := make([]model.BalanceNative, 0, len(fullblock.BalanceNativeList))
	if optinalBalanceNative {
		for _, balance := range fullblock.BalanceNativeList {
			modelBalance := model.BalanceNative{
				Addr:         balance.Addr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			}
			balanceNativeList = append(balanceNativeList, modelBalance)
		}
	}

	balanceErc20List := make([]model.BalanceErc20, 0, len(fullblock.BalanceErc20List))
	if optinalBalanceErc20 {
		for _, balance := range fullblock.BalanceErc20List {
			modelBalance := model.BalanceErc20{
				Addr:         balance.Addr,
				ContractAddr: balance.ContractAddr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			}
			balanceErc20List = append(balanceErc20List, modelBalance)
		}
	}

	balanceErc1155List := make([]model.BalanceErc1155, 0, len(fullblock.BalanceErc1155List))
	if optinalBalanceErc1155 {
		for _, balance := range fullblock.BalanceErc1155List {
			modelBalance := model.BalanceErc1155{
				Addr:         balance.Addr,
				ContractAddr: balance.ContractAddr,
				TokenId:      balance.TokenId,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			}
			balanceErc1155List = append(balanceErc1155List, modelBalance)
		}
	}

	tokenErc721List := make([]model.TokenErc721, 0, len(fullblock.TokenErc721List))
	if optinalTokenErc721 {
		for _, token := range fullblock.TokenErc721List {
			modelToken := model.TokenErc721{
				ContractAddr:  token.ContractAddr,
				TokenId:       token.TokenId,
				OwnerAddr:     token.OwnerAddr,
				TokenUri:      token.TokenUri,
				TokenMetaData: token.TokenMetaData,
				UpdateHeight:  token.UpdateHeight,
			}
			tokenErc721List = append(tokenErc721List, modelToken)
		}
	}

	return &StorageFullBlock{
		Block:                    modelBlock,
		TxList:                   txList,
		TxInternalList:           txInternalList,
		EventLogList:             eventLogList,
		EventErc20TransferList:   eventErc20TransferList,
		EventErc721TransferList:  eventErc721TransferList,
		EventErc1155TransferList: eventErc1155TransferList,
		ContractList:             contractList,
		ContractErc20List:        contractErc20List,
		ContractErc721List:       contractErc721List,
		BalanceNativeList:        balanceNativeList,
		BalanceErc20List:         balanceErc20List,
		BalanceErc1155List:       balanceErc1155List,
		TokenErc721List:          tokenErc721List,
	}
}

func convertProtocolFullBlock(fullblock *types.FullBlock) *protocol.FullBlock {

	fullTxList := make([]*protocol.FullTx, 0, len(fullblock.TxList))
	for _, tx := range fullblock.TxList {
		fullEventLogList := make([]*protocol.FullEventLog, 0)
		for _, log := range fullblock.EventLogList {
			if log.TxHash == tx.TxHash {
				protocolEventLog := &protocol.EventLog{
					IndexInBlock: log.IndexInBlock,
					ContractAddr: log.ContractAddr,
					TopicCount:   log.TopicCount,
					Topic0:       log.Topic0,
					Topic1:       log.Topic1,
					Topic2:       log.Topic2,
					Topic3:       log.Topic3,
					Data:         log.Data,
				}

				fullEventLog := &protocol.FullEventLog{
					EventLog: protocolEventLog,
				}

				// Check for ERC20 Transfer
				for _, erc20Transfer := range fullblock.EventErc20TransferList {
					if erc20Transfer.TxHash == tx.TxHash && erc20Transfer.IndexInBlock == log.IndexInBlock {
						fullEventLog.EventErc20Transfer = &protocol.EventErc20Transfer{
							IndexInBlock: erc20Transfer.IndexInBlock,
							ContractAddr: erc20Transfer.ContractAddr,
							From:         erc20Transfer.From,
							To:           erc20Transfer.To,
							Amount:       erc20Transfer.Amount,
						}
						break
					}
				}

				// Check for ERC721 Transfer
				for _, erc721Transfer := range fullblock.EventErc721TransferList {
					if erc721Transfer.TxHash == tx.TxHash && erc721Transfer.IndexInBlock == log.IndexInBlock {
						fullEventLog.EventErc721Transfer = &protocol.EventErc721Transfer{
							IndexInBlock: erc721Transfer.IndexInBlock,
							ContractAddr: erc721Transfer.ContractAddr,
							From:         erc721Transfer.From,
							To:           erc721Transfer.To,
							TokenId:      erc721Transfer.TokenId,
						}
						break
					}
				}

				// Check for ERC1155 Transfer
				for _, erc1155Transfer := range fullblock.EventErc1155TransferList {
					if erc1155Transfer.TxHash == tx.TxHash && erc1155Transfer.IndexInBlock == log.IndexInBlock {
						fullEventLog.EventErc1155Transfer = &protocol.EventErc1155Transfer{
							IndexInBlock: erc1155Transfer.IndexInBlock,
							IndexInBatch: erc1155Transfer.IndexInBatch,
							ContractAddr: erc1155Transfer.ContractAddr,
							Operator:     erc1155Transfer.Operator,
							From:         erc1155Transfer.From,
							To:           erc1155Transfer.To,
							TokenId:      erc1155Transfer.TokenId,
							Amount:       erc1155Transfer.Amount,
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

		contractList := make([]*protocol.Contract, 0)
		for _, contract := range fullblock.ContractList {
			if contract.TxHash == tx.TxHash {
				contractList = append(contractList, &protocol.Contract{
					ContractAddr: contract.ContractAddr,
					CreatorAddr:  contract.CreatorAddr,
					ExecStatus:   contract.ExecStatus,
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
			ContractList:     contractList,
		}
		fullTxList = append(fullTxList, fullTx)
	}

	contractErc20List := make([]*protocol.ContractErc20, 0, len(fullblock.ContractErc20List))
	for _, contract := range fullblock.ContractErc20List {
		contractErc20List = append(contractErc20List, &protocol.ContractErc20{
			ContractAddr: contract.ContractAddr,
			Name:         contract.Name,
			Symbol:       contract.Symbol,
			Decimals:     contract.Decimals,
			TotalSupply:  contract.TotalSupply,
		})
	}

	contractErc721List := make([]*protocol.ContractErc721, 0, len(fullblock.ContractErc721List))
	for _, contract := range fullblock.ContractErc721List {
		contractErc721List = append(contractErc721List, &protocol.ContractErc721{
			ContractAddr: contract.ContractAddr,
			Name:         contract.Name,
			Symbol:       contract.Symbol,
		})
	}

	balanceNativeList := make([]*protocol.BalanceNative, 0, len(fullblock.BalanceNativeList))
	for _, balance := range fullblock.BalanceNativeList {
		balanceNativeList = append(balanceNativeList, &protocol.BalanceNative{
			Addr:         balance.Addr,
			Balance:      balance.Balance,
			UpdateHeight: balance.UpdateHeight,
		})
	}

	balanceErc20List := make([]*protocol.BalanceErc20, 0, len(fullblock.BalanceErc20List))
	for _, balance := range fullblock.BalanceErc20List {
		balanceErc20List = append(balanceErc20List, &protocol.BalanceErc20{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			Balance:      balance.Balance,
			UpdateHeight: balance.UpdateHeight,
		})
	}

	balanceErc1155List := make([]*protocol.BalanceErc1155, 0, len(fullblock.BalanceErc1155List))
	for _, balance := range fullblock.BalanceErc1155List {
		balanceErc1155List = append(balanceErc1155List, &protocol.BalanceErc1155{
			Addr:         balance.Addr,
			ContractAddr: balance.ContractAddr,
			TokenId:      balance.TokenId,
			Balance:      balance.Balance,
			UpdateHeight: balance.UpdateHeight,
		})
	}

	tokenErc721List := make([]*protocol.TokenErc721, 0, len(fullblock.TokenErc721List))
	for _, token := range fullblock.TokenErc721List {
		tokenErc721List = append(tokenErc721List, &protocol.TokenErc721{
			ContractAddr:  token.ContractAddr,
			TokenId:       token.TokenId,
			OwnerAddr:     token.OwnerAddr,
			TokenUri:      token.TokenUri,
			TokenMetaData: token.TokenMetaData,
			UpdateHeight:  token.UpdateHeight,
		})
	}

	protocolFullBlock := &protocol.FullBlock{
		Height:          fullblock.Block.Height,
		Hash:            fullblock.Block.Hash,
		ParentHash:      fullblock.Block.ParentHash,
		Timestamp:       fullblock.Block.Timestamp,
		TxCount:         fullblock.Block.TxCount,
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
			ContractErc20List:  contractErc20List,
			ContractErc721List: contractErc721List,
			BalanceNativeList:  balanceNativeList,
			BalanceErc20List:   balanceErc20List,
			BalanceErc1155List: balanceErc1155List,
			TokenErc721List:    tokenErc721List,
		},
	}

	return protocolFullBlock
}
