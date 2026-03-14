package fetch

import (
	"scanner_eth/data"
	"scanner_eth/model"
	"scanner_eth/protocol"
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

func ConvertStorageFullBlock(fullblock *data.FullBlock) *StorageFullBlock {
	if fullblock == nil {
		return nil
	}
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

		// TxInternalList conversion (if exists in types.FullBlock)
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

func ConvertProtocolFullBlock(fullblock *data.FullBlock) *protocol.FullBlock {
	if fullblock == nil {
		return nil
	}

	var protocolBlock *protocol.Block
	if fullblock.Block != nil {
		protocolBlock = &protocol.Block{
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
	}

	protocolStateSet := &protocol.StateSet{
		ContractErc20List:  make([]*protocol.ContractErc20, 0),
		ContractErc721List: make([]*protocol.ContractErc721, 0),
		BalanceNativeList:  make([]*protocol.BalanceNative, 0),
		BalanceErc20List:   make([]*protocol.BalanceErc20, 0),
		BalanceErc1155List: make([]*protocol.BalanceErc1155, 0),
		TokenErc721List:    make([]*protocol.TokenErc721, 0),
	}

	if fullblock.StateSet != nil {
		for _, contract := range fullblock.StateSet.ContractErc20List {
			if contract == nil {
				continue
			}
			protocolStateSet.ContractErc20List = append(protocolStateSet.ContractErc20List, &protocol.ContractErc20{
				ContractAddr: contract.ContractAddr,
				Name:         contract.Name,
				Symbol:       contract.Symbol,
				Decimals:     contract.Decimals,
				TotalSupply:  contract.TotalSupply,
			})
		}

		for _, contract := range fullblock.StateSet.ContractErc721List {
			if contract == nil {
				continue
			}
			protocolStateSet.ContractErc721List = append(protocolStateSet.ContractErc721List, &protocol.ContractErc721{
				ContractAddr: contract.ContractAddr,
				Name:         contract.Name,
				Symbol:       contract.Symbol,
			})
		}

		for _, balance := range fullblock.StateSet.BalanceNativeList {
			if balance == nil {
				continue
			}
			protocolStateSet.BalanceNativeList = append(protocolStateSet.BalanceNativeList, &protocol.BalanceNative{
				Addr:         balance.Addr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			})
		}

		for _, balance := range fullblock.StateSet.BalanceErc20List {
			if balance == nil {
				continue
			}
			protocolStateSet.BalanceErc20List = append(protocolStateSet.BalanceErc20List, &protocol.BalanceErc20{
				Addr:         balance.Addr,
				ContractAddr: balance.ContractAddr,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			})
		}

		for _, balance := range fullblock.StateSet.BalanceErc1155List {
			if balance == nil {
				continue
			}
			protocolStateSet.BalanceErc1155List = append(protocolStateSet.BalanceErc1155List, &protocol.BalanceErc1155{
				Addr:         balance.Addr,
				ContractAddr: balance.ContractAddr,
				TokenId:      balance.TokenId,
				Balance:      balance.Balance,
				UpdateHeight: balance.UpdateHeight,
			})
		}

		for _, token := range fullblock.StateSet.TokenErc721List {
			if token == nil {
				continue
			}
			protocolStateSet.TokenErc721List = append(protocolStateSet.TokenErc721List, &protocol.TokenErc721{
				ContractAddr:  token.ContractAddr,
				TokenId:       token.TokenId,
				OwnerAddr:     token.OwnerAddr,
				TokenUri:      token.TokenUri,
				TokenMetaData: token.TokenMetaData,
				UpdateHeight:  token.UpdateHeight,
			})
		}
	}

	memeEvents := make([]interface{}, 0)
	erc20PaymentEvents := make([]interface{}, 0)
	hybridNftEvents := make([]interface{}, 0)
	nftMarketplaceEvents := make([]interface{}, 0)
	uniswapV2Events := make([]interface{}, 0)

	iterateEvents := func(raw interface{}, handler func(interface{})) {
		if raw == nil {
			return
		}

		switch events := raw.(type) {
		case []interface{}:
			for _, event := range events {
				handler(event)
			}
		case []*data.NftMarketplaceItemListed:
			for _, event := range events {
				handler(event)
			}
		default:
			handler(raw)
		}
	}

	for _, fullTx := range fullblock.FullTxList {
		if fullTx == nil || fullTx.Tx == nil {
			continue
		}

		txHash := fullTx.Tx.TxHash

		for _, fullEventLog := range fullTx.FullEventLogList {
			if fullEventLog == nil {
				continue
			}

			var eventIndex uint
			if fullEventLog.EventLog != nil {
				eventIndex = fullEventLog.EventLog.IndexInBlock
			}

			iterateEvents(fullEventLog.MemeEvent, func(event interface{}) {
				switch v := event.(type) {
				case *data.MemeTokenLaunched:
					memeEvents = append(memeEvents, &protocol.MemeTokenLaunched{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, Name: v.Name, Symbol: v.Symbol, Creator: v.Creator, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken, TotalSupply: v.TotalSupply, AutoBuyAmount: v.AutoBuyAmount, Pair: v.Pair})
				case data.MemeTokenLaunched:
					memeEvents = append(memeEvents, &protocol.MemeTokenLaunched{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, Name: v.Name, Symbol: v.Symbol, Creator: v.Creator, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken, TotalSupply: v.TotalSupply, AutoBuyAmount: v.AutoBuyAmount, Pair: v.Pair})
				case *data.MemeTrade:
					memeEvents = append(memeEvents, &protocol.MemeTrade{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, EthAmount: v.EthAmount, EthFeeAmount: v.EthFeeAmount, TokenAmount: v.TokenAmount, IsBuy: v.IsBuy, User: v.User, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken})
				case data.MemeTrade:
					memeEvents = append(memeEvents, &protocol.MemeTrade{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, EthAmount: v.EthAmount, EthFeeAmount: v.EthFeeAmount, TokenAmount: v.TokenAmount, IsBuy: v.IsBuy, User: v.User, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken})
				case *data.MemeLiquiditySwapped:
					memeEvents = append(memeEvents, &protocol.MemeLiquiditySwapped{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, EthAmount: v.EthAmount, TokenAmount: v.TokenAmount, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken})
				case data.MemeLiquiditySwapped:
					memeEvents = append(memeEvents, &protocol.MemeLiquiditySwapped{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, EthAmount: v.EthAmount, TokenAmount: v.TokenAmount, VReserveEth: v.VReserveEth, VReserveToken: v.VReserveToken})
				}
			})

			iterateEvents(fullEventLog.Erc20PaymentEvent, func(event interface{}) {
				switch v := event.(type) {
				case *data.Erc20PaymentTransferEvent:
					erc20PaymentEvents = append(erc20PaymentEvents, &protocol.Erc20PaymentTransferEvent{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, From: v.From, To: v.To, Amount: v.Amount, Memo: v.Memo})
				case data.Erc20PaymentTransferEvent:
					erc20PaymentEvents = append(erc20PaymentEvents, &protocol.Erc20PaymentTransferEvent{TxHash: txHash, EventIndex: eventIndex, Token: v.Token, From: v.From, To: v.To, Amount: v.Amount, Memo: v.Memo})
				}
			})

			iterateEvents(fullEventLog.HybridNftEvent, func(event interface{}) {
				switch v := event.(type) {
				case *data.HybridPublicConfigChanged:
					hybridNftEvents = append(hybridNftEvents, &protocol.HybridPublicConfigChanged{TxHash: txHash, EventIndex: eventIndex, RoyaltyBasisPoints: v.RoyaltyBasisPoints, MaxSupply: v.MaxSupply, MaxMintPerWallet: v.MaxMintPerWallet, MintPrice: v.MintPrice, MintStartTime: v.MintStartTime, MintEndTime: v.MintEndTime})
				case data.HybridPublicConfigChanged:
					hybridNftEvents = append(hybridNftEvents, &protocol.HybridPublicConfigChanged{TxHash: txHash, EventIndex: eventIndex, RoyaltyBasisPoints: v.RoyaltyBasisPoints, MaxSupply: v.MaxSupply, MaxMintPerWallet: v.MaxMintPerWallet, MintPrice: v.MintPrice, MintStartTime: v.MintStartTime, MintEndTime: v.MintEndTime})
				case *data.HybridWhitelistConfigChanged:
					hybridNftEvents = append(hybridNftEvents, &protocol.HybridWhitelistConfigChanged{TxHash: txHash, EventIndex: eventIndex, RoyaltyBasisPoints: v.RoyaltyBasisPoints, MaxSupply: v.MaxSupply, MaxMintPerWallet: v.MaxMintPerWallet, WhitelistPrice: v.WhitelistPrice, PublicPrice: v.PublicPrice, WhitelistStartTime: v.WhitelistStartTime, WhitelistEndTime: v.WhitelistEndTime, PublicEndTime: v.PublicEndTime})
				case data.HybridWhitelistConfigChanged:
					hybridNftEvents = append(hybridNftEvents, &protocol.HybridWhitelistConfigChanged{TxHash: txHash, EventIndex: eventIndex, RoyaltyBasisPoints: v.RoyaltyBasisPoints, MaxSupply: v.MaxSupply, MaxMintPerWallet: v.MaxMintPerWallet, WhitelistPrice: v.WhitelistPrice, PublicPrice: v.PublicPrice, WhitelistStartTime: v.WhitelistStartTime, WhitelistEndTime: v.WhitelistEndTime, PublicEndTime: v.PublicEndTime})
				}
			})

			iterateEvents(fullEventLog.NftMarketplaceEvent, func(event interface{}) {
				switch v := event.(type) {
				case *data.NftMarketplaceItemListed:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceItemListed{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Seller: v.Seller, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case data.NftMarketplaceItemListed:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceItemListed{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Seller: v.Seller, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case *data.ItemSold:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.ItemSold{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Buyer: v.Buyer, Seller: v.Seller, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case data.ItemSold:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.ItemSold{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Buyer: v.Buyer, Seller: v.Seller, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case *data.NftMarketplaceOffer:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOffer{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case data.NftMarketplaceOffer:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOffer{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case *data.NftMarketplaceBatchOffer:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOffer{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Buyer: v.Buyer, NFTContract: v.NFTContract, Quantity: v.Quantity, PricePerItem: v.PricePerItem, PaymentToken: v.PaymentToken, TotalValue: v.TotalValue})
				case data.NftMarketplaceBatchOffer:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOffer{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Buyer: v.Buyer, NFTContract: v.NFTContract, Quantity: v.Quantity, PricePerItem: v.PricePerItem, PaymentToken: v.PaymentToken, TotalValue: v.TotalValue})
				case *data.NftMarketplaceListingCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceListingCancelled{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Seller: v.Seller})
				case data.NftMarketplaceListingCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceListingCancelled{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, Seller: v.Seller})
				case *data.NftMarketplacePriceUpdated:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplacePriceUpdated{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, NewPrice: v.NewPrice})
				case data.NftMarketplacePriceUpdated:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplacePriceUpdated{TxHash: txHash, EventIndex: eventIndex, ListingId: v.ListingId, NewPrice: v.NewPrice})
				case *data.NftMarketplaceOfferAccepted:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOfferAccepted{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Seller: v.Seller, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case data.NftMarketplaceOfferAccepted:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOfferAccepted{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Seller: v.Seller, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenId: v.TokenId, Price: v.Price, PaymentToken: v.PaymentToken})
				case *data.NftMarketplaceOfferCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOfferCancelled{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Buyer: v.Buyer})
				case data.NftMarketplaceOfferCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceOfferCancelled{TxHash: txHash, EventIndex: eventIndex, OfferId: v.OfferId, Buyer: v.Buyer})
				case *data.NftMarketplaceBatchOfferAccepted:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOfferAccepted{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Seller: v.Seller, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenIds: v.TokenIds, TotalValue: v.TotalValue, PaymentToken: v.PaymentToken})
				case data.NftMarketplaceBatchOfferAccepted:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOfferAccepted{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Seller: v.Seller, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenIds: v.TokenIds, TotalValue: v.TotalValue, PaymentToken: v.PaymentToken})
				case *data.NftMarketplaceBatchOfferCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOfferCancelled{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Buyer: v.Buyer})
				case data.NftMarketplaceBatchOfferCancelled:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchOfferCancelled{TxHash: txHash, EventIndex: eventIndex, BatchOfferId: v.BatchOfferId, Buyer: v.Buyer})
				case *data.NftMarketplaceBatchPurchase:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchPurchase{TxHash: txHash, EventIndex: eventIndex, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenIds: v.TokenIds, TotalPrice: v.TotalPrice, PaymentToken: v.PaymentToken})
				case data.NftMarketplaceBatchPurchase:
					nftMarketplaceEvents = append(nftMarketplaceEvents, &protocol.NftMarketplaceBatchPurchase{TxHash: txHash, EventIndex: eventIndex, Buyer: v.Buyer, NFTContract: v.NFTContract, TokenIds: v.TokenIds, TotalPrice: v.TotalPrice, PaymentToken: v.PaymentToken})
				}
			})

			iterateEvents(fullEventLog.UniswapV2Event, func(event interface{}) {
				switch v := event.(type) {
				case *data.UniswapV2Swap:
					uniswapV2Events = append(uniswapV2Events, &protocol.UniswapV2Swap{TxHash: txHash, EventIndex: eventIndex, Pair: v.Pair, Sender: v.Sender, Amount0In: v.Amount0In, Amount1In: v.Amount1In, Amount0Out: v.Amount0Out, Amount1Out: v.Amount1Out, To: v.To})
				case data.UniswapV2Swap:
					uniswapV2Events = append(uniswapV2Events, &protocol.UniswapV2Swap{TxHash: txHash, EventIndex: eventIndex, Pair: v.Pair, Sender: v.Sender, Amount0In: v.Amount0In, Amount1In: v.Amount1In, Amount0Out: v.Amount0Out, Amount1Out: v.Amount1Out, To: v.To})
				}
			})
		}
	}

	return &protocol.FullBlock{
		Block:               protocolBlock,
		StateSet:            protocolStateSet,
		MemeEvent:           memeEvents,
		Erc20PaymentEvent:   erc20PaymentEvents,
		HybridNftEvent:      hybridNftEvents,
		NftMarketplaceEvent: nftMarketplaceEvents,
		UniswapV2Event:      uniswapV2Events,
	}
}
