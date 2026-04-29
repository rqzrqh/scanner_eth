package convert

import (
	"scanner_eth/blocktree"
	"scanner_eth/data"
	fetchstore "scanner_eth/fetch/store"
	"scanner_eth/model"
	"strings"
)

var (
	optionalTx                   bool
	optionalTxInternal           bool
	optionalEventLog             bool
	optionalBalanceNative        bool
	optionalBalanceErc20         bool
	optionalBalanceErc1155       bool
	optionalEventErc20Transfer   bool
	optionalEventErc721Transfer  bool
	optionalEventErc1155Transfer bool
	optionalContract             bool
	optionalContractErc20        bool
	optionalContractErc721       bool
	optionalTokenErc721          bool
)

func normalizeHash(hash string) string {
	if hash == "" {
		return ""
	}
	return strings.ToLower(hash)
}

func SetOptionalFeatures(optionalFeature map[string]struct{}) {
	_, optionalTx = optionalFeature[model.Tx.TableName(model.Tx{})]
	_, optionalTxInternal = optionalFeature[model.TxInternal.TableName(model.TxInternal{})]
	_, optionalEventLog = optionalFeature[model.EventLog.TableName(model.EventLog{})]
	_, optionalBalanceNative = optionalFeature[model.BalanceNative.TableName(model.BalanceNative{})]
	_, optionalBalanceErc20 = optionalFeature[model.BalanceErc20.TableName(model.BalanceErc20{})]
	_, optionalBalanceErc1155 = optionalFeature[model.BalanceErc1155.TableName(model.BalanceErc1155{})]
	_, optionalEventErc20Transfer = optionalFeature[model.EventErc20Transfer.TableName(model.EventErc20Transfer{})]
	_, optionalEventErc721Transfer = optionalFeature[model.EventErc721Transfer.TableName(model.EventErc721Transfer{})]
	_, optionalEventErc1155Transfer = optionalFeature[model.EventErc1155Transfer.TableName(model.EventErc1155Transfer{})]
	_, optionalContract = optionalFeature[model.Contract.TableName(model.Contract{})]
	_, optionalContractErc20 = optionalFeature[model.ContractErc20.TableName(model.ContractErc20{})]
	_, optionalContractErc721 = optionalFeature[model.ContractErc721.TableName(model.ContractErc721{})]
	_, optionalTokenErc721 = optionalFeature[model.TokenErc721.TableName(model.TokenErc721{})]
}

func ConvertStorageFullBlock(fullblock *data.FullBlock, irreversible blocktree.IrreversibleNode) *fetchstore.StorageFullBlock {
	if fullblock == nil {
		return nil
	}
	if fullblock.Block != nil {
		fullblock.Block.IrreversibleHeight = irreversible.Height
		fullblock.Block.IrreversibleHash = normalizeHash(irreversible.Key)
	}
	blockHeight := fullblock.Block.Height
	modelBlock := model.Block{
		Height:             fullblock.Block.Height,
		Hash:               fullblock.Block.Hash,
		ParentHash:         fullblock.Block.ParentHash,
		IrreversibleHeight: irreversible.Height,
		IrreversibleHash:   irreversible.Key,
		Timestamp:          fullblock.Block.Timestamp,
		TxCount:            fullblock.Block.TxCount,
		Miner:              fullblock.Block.Miner,
		Size:               fullblock.Block.Size,
		Nonce:              fullblock.Block.Nonce,
		BaseFee:            fullblock.Block.BaseFee,
		BurntFees:          fullblock.Block.BurntFees,
		GasLimit:           fullblock.Block.GasLimit,
		GasUsed:            fullblock.Block.GasUsed,
		UnclesCount:        fullblock.Block.UnclesCount,
		Difficulty:         fullblock.Block.Difficulty,
		TotalDifficulty:    fullblock.Block.TotalDifficulty,
		StateRoot:          fullblock.Block.StateRoot,
		TransactionRoot:    fullblock.Block.TransactionRoot,
		ReceiptRoot:        fullblock.Block.ReceiptRoot,
		ExtraData:          fullblock.Block.ExtraData,
	}

	txList := make([]model.Tx, 0, len(fullblock.FullTxList))
	txInternalList := make([]model.TxInternal, 0)
	eventLogList := make([]model.EventLog, 0)
	eventErc20TransferList := make([]model.EventErc20Transfer, 0)
	eventErc721TransferList := make([]model.EventErc721Transfer, 0)
	eventErc1155TransferList := make([]model.EventErc1155Transfer, 0)

	for txIndex, fullTx := range fullblock.FullTxList {
		txHash := fullTx.Tx.TxHash
		if optionalTx {
			tx := fullTx.Tx
			modelTx := model.Tx{
				Height:               blockHeight,
				TxHash:               tx.TxHash,
				TxIndex:              txIndex,
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

		if optionalTxInternal {
			for _, txInternal := range fullTx.TxInternalList {
				modelTxInternal := model.TxInternal{
					Height:       blockHeight,
					TxHash:       txInternal.TxHash,
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
				}
				txInternalList = append(txInternalList, modelTxInternal)
			}
		}

		for indexInTx, fullEventLog := range fullTx.FullEventLogList {
			if optionalEventLog && fullEventLog.EventLog != nil {
				log := fullEventLog.EventLog
				modelLog := model.EventLog{
					Height:       blockHeight,
					TxHash:       txHash,
					IndexInTx:    uint(indexInTx),
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

			if optionalEventErc20Transfer && fullEventLog.EventErc20Transfer != nil {
				transfer := fullEventLog.EventErc20Transfer
				modelTransfer := model.EventErc20Transfer{
					Height:       blockHeight,
					TxHash:       txHash,
					IndexInTx:    uint(indexInTx),
					ContractAddr: transfer.ContractAddr,
					From:         transfer.From,
					To:           transfer.To,
					Amount:       transfer.Amount,
				}
				eventErc20TransferList = append(eventErc20TransferList, modelTransfer)
			}

			if optionalEventErc721Transfer && fullEventLog.EventErc721Transfer != nil {
				transfer := fullEventLog.EventErc721Transfer
				modelTransfer := model.EventErc721Transfer{
					Height:       blockHeight,
					TxHash:       txHash,
					IndexInTx:    uint(indexInTx),
					ContractAddr: transfer.ContractAddr,
					From:         transfer.From,
					To:           transfer.To,
					TokenId:      transfer.TokenId,
				}
				eventErc721TransferList = append(eventErc721TransferList, modelTransfer)
			}

			if optionalEventErc1155Transfer && fullEventLog.EventErc1155Transfers != nil {
				for indexInBatch, transfer := range fullEventLog.EventErc1155Transfers {
					modelTransfer := model.EventErc1155Transfer{
						Height:       blockHeight,
						TxHash:       txHash,
						IndexInTx:    uint(indexInTx),
						IndexInBatch: indexInBatch,
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
		}
	}

	stateSet := fullblock.StateSet

	contractList := make([]model.Contract, 0, len(stateSet.ContractList))
	if optionalContract {
		for _, contract := range stateSet.ContractList {
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

	contractErc20List := make([]model.ContractErc20, 0, len(stateSet.ContractErc20List))
	if optionalContractErc20 {
		for _, contract := range stateSet.ContractErc20List {
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

	contractErc721List := make([]model.ContractErc721, 0, len(stateSet.ContractErc721List))
	if optionalContractErc721 {
		for _, contract := range stateSet.ContractErc721List {
			modelContract := model.ContractErc721{
				ContractAddr: contract.ContractAddr,
				Name:         contract.Name,
				Symbol:       contract.Symbol,
			}
			contractErc721List = append(contractErc721List, modelContract)
		}
	}

	balanceNativeList := make([]model.BalanceNative, 0, len(stateSet.BalanceNativeList))
	if optionalBalanceNative {
		for _, balance := range stateSet.BalanceNativeList {
			modelBalance := model.BalanceNative{
				Addr:         balance.Addr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			}
			balanceNativeList = append(balanceNativeList, modelBalance)
		}
	}

	balanceErc20List := make([]model.BalanceErc20, 0, len(stateSet.BalanceErc20List))
	if optionalBalanceErc20 {
		for _, balance := range stateSet.BalanceErc20List {
			modelBalance := model.BalanceErc20{
				Addr:         balance.Addr,
				ContractAddr: balance.ContractAddr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			}
			balanceErc20List = append(balanceErc20List, modelBalance)
		}
	}

	balanceErc1155List := make([]model.BalanceErc1155, 0, len(stateSet.BalanceErc1155List))
	if optionalBalanceErc1155 {
		for _, balance := range stateSet.BalanceErc1155List {
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

	tokenErc721List := make([]model.TokenErc721, 0, len(stateSet.TokenErc721List))
	if optionalTokenErc721 {
		for _, token := range stateSet.TokenErc721List {
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

	return &fetchstore.StorageFullBlock{
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
