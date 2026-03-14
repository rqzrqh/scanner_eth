package fetch

import (
	"context"
	"fmt"
	"math/big"
	"scanner_eth/data"
	"scanner_eth/filter"
	"scanner_eth/util"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

var (
	enableInternalTx bool
)

func SetEnableInternalTx(enable bool) {
	enableInternalTx = enable
}

type TokenErc721KeyValue struct {
	ContractAddr string
	TokenId      string
}

type FetchResult struct {
	NodeId      int
	TaskId      int
	ForkVersion uint64
	Height      uint64
	FullBlock   *data.FullBlock
	CostTime    time.Duration
}

type BlockFetcher interface {
	FetchBlockHeaderByHeight(ctx context.Context, nodeOp *NodeOperator, taskId int, height uint64) *BlockHeaderJson
	FetchBlockHeaderByHash(ctx context.Context, nodeOp *NodeOperator, taskId int, hash string) *BlockHeaderJson
	FetchFullBlock(ctx context.Context, nodeOp *NodeOperator, taskId int, header *BlockHeaderJson) *data.FullBlock
}

type fetchManagerBlockFetcher struct {
	db *gorm.DB
}

func NewBlockFetcher(db *gorm.DB) BlockFetcher {
	return newFetchManagerBlockFetcher(db)
}

func newFetchManagerBlockFetcher(
	db *gorm.DB,
) BlockFetcher {
	return &fetchManagerBlockFetcher{
		db: db,
	}
}

func (bf *fetchManagerBlockFetcher) FetchBlockHeaderByHeight(ctx context.Context, nodeOp *NodeOperator, taskId int, height uint64) *BlockHeaderJson {
	return FetchBlockHeaderByHeight(ctx, nodeOp, taskId, height)
}

func (bf *fetchManagerBlockFetcher) FetchBlockHeaderByHash(ctx context.Context, nodeOp *NodeOperator, taskId int, hash string) *BlockHeaderJson {
	return FetchBlockHeaderByHash(ctx, nodeOp, taskId, hash)
}

func (bf *fetchManagerBlockFetcher) FetchFullBlock(ctx context.Context, nodeOp *NodeOperator, taskId int, header *BlockHeaderJson) *data.FullBlock {
	return FetchFullBlock(ctx, nodeOp, taskId, bf.db, header)
}

func transTraceAddressToString(opcode string, traceAddress []uint64) string {
	var res = strings.ToLower(opcode)
	for _, addr := range traceAddress {
		res = fmt.Sprintf("%s_%d", res, addr)
	}
	return res
}

type TxParseResult struct {
	FullTxList            []*data.FullTx
	ContractList          []*data.Contract
	BalanceNativeAddress  map[string]struct{}
	BalanceErc20Address   map[string]map[string]struct{}
	BalanceErc1155Address map[string]map[string]string
	Erc20ContractAddrs    map[string]struct{}
	Erc721ContractAddrs   map[string]struct{}
	TokenErc721Set        map[TokenErc721KeyValue]TokenErc721KeyValue
}

type InternalTxParseResult struct {
	InternalTxList               []*data.TxInternal
	InternalContractList         []*data.Contract
	InternalBalanceNativeAddress map[string]struct{}
}

func FetchBlockHeaderByHeight(ctx context.Context, nodeOp *NodeOperator, taskId int, height uint64) *BlockHeaderJson {
	if nodeOp == nil {
		return nil
	}
	return nodeOp.FetchBlockHeaderByHeight(ctx, taskId, height)
}

func FetchBlockHeaderByHash(ctx context.Context, nodeOp *NodeOperator, taskId int, hash string) *BlockHeaderJson {
	if nodeOp == nil {
		return nil
	}
	return nodeOp.FetchBlockHeaderByHash(ctx, taskId, hash)
}

// FetchFullBlock loads txs, receipts, internal traces (if enabled), and token/balance state from RPC using the given header (height follows header; archive nodes may be required for full historical state).
func FetchFullBlock(ctx context.Context, nodeOp *NodeOperator, taskId int, db *gorm.DB, header *BlockHeaderJson) *data.FullBlock {
	if ctx == nil {
		ctx = context.Background()
	}
	if nodeOp == nil {
		return nil
	}
	nodeId := nodeOp.ID()

	height := hexutil.MustDecodeUint64(header.Number)

	gasUsed := hexutil.MustDecodeUint64(header.GasUsed)

	var baseFee *big.Int
	if header.BaseFeePerGas != "" {
		baseFee = hexutil.MustDecodeBig(header.BaseFeePerGas)
	}

	// eip1559 set burnt fees
	burntFees := new(big.Int)
	if baseFee != nil {
		burntFees = burntFees.Mul(new(big.Int).SetUint64(gasUsed), baseFee)
	}

	if baseFee == nil {
		baseFee = big.NewInt(0)
	}

	decodeBigOrZero := func(s string) *big.Int {
		if s == "" {
			return big.NewInt(0)
		}
		d, err := hexutil.DecodeBig(s)
		if err != nil || d == nil {
			return big.NewInt(0)
		}
		return d
	}
	difficulty := decodeBigOrZero(header.Difficulty)
	totalDifficulty := decodeBigOrZero(header.TotalDifficulty)

	blk := &data.Block{
		Height:     hexutil.MustDecodeUint64(header.Number),
		Hash:       header.Hash,
		ParentHash: header.ParentHash,
		Timestamp:  int64(hexutil.MustDecodeUint64(header.TimeStamp)),
		TxCount:    len(header.Transactions),
		Miner:      header.Miner,
		Size:       int(hexutil.MustDecodeUint64(header.Size)),
		Nonce:      header.Nonce,
		BaseFee:    baseFee.String(),
		BurntFees:  burntFees.String(),
		GasLimit:   hexutil.MustDecodeUint64(header.GasLimit),
		GasUsed:    gasUsed,

		UnclesCount: len(header.Uncles),

		Difficulty:      difficulty.String(),
		TotalDifficulty: totalDifficulty.String(),
		StateRoot:       header.StateRoot,
		TransactionRoot: header.TransactionRoot,
		ReceiptRoot:     header.ReceiptsRoot,
		ExtraData:       header.ExtraData,
	}

	// Fetch transactions strictly by header transaction hashes.
	txList := make([]*TxJson, 0, len(header.Transactions))
	if len(header.Transactions) > 0 {
		startTime := time.Now()
		txSlots := make([]*TxJson, len(header.Transactions))
		for i := range header.Transactions {
			txSlots[i] = &TxJson{}
		}
		if err := nodeOp.FetchTransactionsByHashBatch(ctx, header.Transactions, txSlots); err != nil {
			logrus.Warnf("fetch tx by header hash failed. nodeId:%v taskId:%v height:%v error:%v", nodeId, taskId, height, err)
			return nil
		}
		for idx, tx := range txSlots {
			if tx == nil || tx.Hash == "" {
				logrus.Warnf("fetch tx by header hash elem invalid. nodeId:%v taskId:%v height:%v elem(%v)", nodeId, taskId, height, idx)
				return nil
			}
			txList = append(txList, tx)
		}

		logrus.Debugf("fetch tx by header hash success. nodeId:%v taskId:%v height:%v txs:%v cost:%v", nodeId, taskId, height, len(txList), time.Since(startTime).String())
	}

	// fetch receipts
	receipts := make(map[string]*eth_types.Receipt)
	{
		startTime := time.Now()
		receiptList := make([]*eth_types.Receipt, len(header.Transactions))
		for i, txHash := range header.Transactions {
			r := &eth_types.Receipt{}
			receiptList[i] = r
			receipts[txHash] = r
		}

		if len(receiptList) > 0 {
			if err := nodeOp.FetchReceiptsBatch(ctx, header.Transactions, receiptList); err != nil {
				logrus.Warnf("fetch receipts failed. nodeId:%v taskId:%v height:%v error:%v", nodeId, taskId, height, err)
				return nil
			}
		}
		logrus.Debugf("fetch receipts success. nodeId:%v taskId:%v txs:%v height:%v cost:%v", nodeId, taskId, len(txList), height, time.Since(startTime).String())
	}

	// fetch internal tx
	txInternalJsonList := make([]*TxInternalTraceResultJson, 0)
	if enableInternalTx {
		var err error
		txInternalJsonList, err = nodeOp.FetchInternalTxTracesByBlockHash(ctx, taskId, header.Hash, height)
		if err != nil {
			return nil
		}
	}

	// parse txs
	txParseResult := parseTx(txList, receipts, height, baseFee)

	// parse internal txs
	internalTxParseResult := parseTxInternal(txInternalJsonList, height)

	balanceNativeAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)

	// Merge contracts by address (dedupe).
	contractSeen := make(map[string]struct{})
	contractList := make([]*data.Contract, 0, len(txParseResult.ContractList)+len(internalTxParseResult.InternalContractList))
	for _, c := range txParseResult.ContractList {
		if _, ok := contractSeen[c.ContractAddr]; !ok {
			contractSeen[c.ContractAddr] = struct{}{}
			contractList = append(contractList, c)
		}
	}
	for _, c := range internalTxParseResult.InternalContractList {
		if _, ok := contractSeen[c.ContractAddr]; !ok {
			contractSeen[c.ContractAddr] = struct{}{}
			contractList = append(contractList, c)
		}
	}

	// get all accounts's native token balance whose native balance has been changed
	for k := range txParseResult.BalanceNativeAddress {
		balanceNativeAddress[k] = struct{}{}
	}
	for k := range internalTxParseResult.InternalBalanceNativeAddress {
		balanceNativeAddress[k] = struct{}{}
	}

	// get all accounts's erc20 token balance whose erc20 balance has been changed
	for k, v := range txParseResult.BalanceErc20Address {
		for c := range v {
			if _, ok := balanceErc20Address[k]; !ok {
				balanceErc20Address[k] = make(map[string]struct{}, 0)
			}
			balanceErc20Address[k][c] = struct{}{}
		}
	}

	// fetch native token balance
	balanceNativeList := make([]*data.BalanceNative, 0, len(balanceNativeAddress))
	{
		for addr := range balanceNativeAddress {
			balanceNativeList = append(balanceNativeList, &data.BalanceNative{
				Addr: addr,
			})
		}

		if err := nodeOp.FetchBalanceNative(ctx, balanceNativeList, height); err != nil {
			logrus.Warnf("fetch balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}
	}

	// fetch erc20 token balance
	balanceErc20List := make([]*data.BalanceErc20, 0)
	{
		for addr, v := range balanceErc20Address {
			for contractAddr := range v {
				balanceErc20List = append(balanceErc20List, &data.BalanceErc20{
					Addr:         addr,
					ContractAddr: contractAddr,
				})
			}
		}

		if err := nodeOp.FetchErc20BalancesBatch(ctx, balanceErc20List, height); err != nil {
			logrus.Warnf("fetch erc20balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}
	}

	balanceErc1155List := make([]*data.BalanceErc1155, 0)
	{
		for contractAddr, v := range txParseResult.BalanceErc1155Address {
			for tokenId, addr := range v {
				balanceErc1155List = append(balanceErc1155List, &data.BalanceErc1155{
					Addr:         addr,
					ContractAddr: contractAddr,
					TokenId:      tokenId,
				})
			}
		}

		if err := nodeOp.FetchErc1155BalancesBatch(ctx, balanceErc1155List, height); err != nil {
			logrus.Warnf("fetch erc1155balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}
	}

		// ERC20 metadata: cache first, then RPC if missing.
	contractErc20List := make([]*data.ContractErc20, 0, len(txParseResult.Erc20ContractAddrs))
	{
		for k := range txParseResult.Erc20ContractAddrs {
			contractErc20, ok := tokenCacheInst.Get(k, db)
			if !ok {
				addr := common.HexToAddress(k)
				var err error
				contractErc20, err = nodeOp.FetchContractErc20(ctx, &addr, height)
				if err != nil {
					return nil
				}
			}
			contractErc20List = append(contractErc20List, contractErc20)
		}
	}

		// ERC721 contract metadata: cache first, then RPC if missing.
	contractErc721List := make([]*data.ContractErc721, 0, len(txParseResult.Erc721ContractAddrs))
	{
		for k := range txParseResult.Erc721ContractAddrs {
			contractErc721, ok := erc721ContractCacheInst.Get(k, db)
			if !ok {
				addr := common.HexToAddress(k)
				var err error
				contractErc721, err = nodeOp.FetchContractErc721(ctx, &addr)
				if err != nil {
					return nil
				}
			}
			contractErc721List = append(contractErc721List, contractErc721)
		}
	}

		// ERC721 token metadata: always from chain (no cache).
	tokenErc721List := make([]*data.TokenErc721, 0, len(txParseResult.TokenErc721Set))
	{
		for k := range txParseResult.TokenErc721Set {
			contractAddr := common.HexToAddress(k.ContractAddr)
			tokenId, ok := new(big.Int).SetString(k.TokenId, 10)
			if !ok {
				logrus.Warnf("set string failed for contract:%v tokenId: %v", k.ContractAddr, k.TokenId)
				continue
			}
			tokenErc721, err := nodeOp.FetchTokenErc721(ctx, &contractAddr, tokenId)
			if err != nil {
				return nil
			}
			tokenErc721List = append(tokenErc721List, tokenErc721)
		}
	}
	txInternalMap := make(map[string][]*data.TxInternal, len(txParseResult.FullTxList))
	for _, txInternal := range internalTxParseResult.InternalTxList {
		txInternalMap[txInternal.TxHash] = append(txInternalMap[txInternal.TxHash], txInternal)
	}
	for _, fullTx := range txParseResult.FullTxList {
		if internalList, ok := txInternalMap[fullTx.Tx.TxHash]; ok {
			fullTx.TxInternalList = internalList
		} else {
			fullTx.TxInternalList = make([]*data.TxInternal, 0)
		}
	}

	fullblock := &data.FullBlock{
		Block:      blk,
		FullTxList: txParseResult.FullTxList,

		StateSet: &data.StateSet{
			ContractList:       contractList,
			ContractErc20List:  contractErc20List,
			ContractErc721List: contractErc721List,

			BalanceNativeList:  balanceNativeList,
			BalanceErc20List:   balanceErc20List,
			BalanceErc1155List: balanceErc1155List,
			TokenErc721List:    tokenErc721List,
		},
	}

	return fullblock
}

func parseTx(jsonTxList []*TxJson, receipts map[string]*eth_types.Receipt, height uint64, baseFee *big.Int) *TxParseResult {

	fullTxList := make([]*data.FullTx, 0, len(jsonTxList))
	contractList := make([]*data.Contract, 0)

	balanceNativeAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)
	balanceErc1155Address := make(map[string]map[string]string, 0)

	erc20ContractAddrs := make(map[string]struct{}, 0)
	erc721ContractAddrs := make(map[string]struct{}, 0)
	tokenErc721Set := make(map[TokenErc721KeyValue]TokenErc721KeyValue, 0)

	for _, txJson := range jsonTxList {
		txHash := txJson.Hash
		receipt := receipts[txHash]
		var (
			toHex            string = txJson.To
			isCreateContract bool
		)

		fromAddr := strings.ToLower(txJson.From)
		toAddr := strings.ToLower(txJson.To)

		// get new create contract
		if toHex == "" || toHex == "0x" || receipt.ContractAddress.Hex() != util.ZeroAddress {
			if receipt.Status == 1 {
				isCreateContract = true
				contract := &data.Contract{
					TxHash:       txHash,
					ContractAddr: strings.ToLower(receipt.ContractAddress.Hex()),
					CreatorAddr:  fromAddr,
					ExecStatus:   receipt.Status,
				}
				contractList = append(contractList, contract)
			}
		}

		isCallContract := false
		if len(txJson.Input) != 0 && txJson.Input != "0x" {
			isCallContract = true
		}

		var txType uint64
		if txJson.Type == "" {
			txType = 0
		} else {
			txType = hexutil.MustDecodeUint64(txJson.Type)
		}

		value := hexutil.MustDecodeBig(txJson.Value)
		nonce := hexutil.MustDecodeUint64(txJson.Nonce)
		gasLimit := hexutil.MustDecodeUint64(txJson.Gas)
		gasPrice := hexutil.MustDecodeBig(txJson.GasPrice)

		// eip1559
		txBurntFees := big.NewInt(0)
		txMaxFeePerGas := big.NewInt(0)
		txMaxPriorityFeePerGas := big.NewInt(0)
		if txType == eth_types.DynamicFeeTxType {
			tmp := new(big.Int).SetUint64(receipt.GasUsed)
			txBurntFees = tmp.Mul(tmp, baseFee)
		}
		if txJson.MaxFeePerGas != "" {
			txMaxFeePerGas = hexutil.MustDecodeBig(txJson.MaxFeePerGas)
		}
		if txJson.MaxPriorityFeePerGas != "" {
			txMaxPriorityFeePerGas = hexutil.MustDecodeBig(txJson.MaxPriorityFeePerGas)
		}

		// tx
		tx := &data.Tx{
			TxType:               int(txType),
			TxHash:               txHash,
			From:                 fromAddr,
			To:                   toAddr,
			Nonce:                nonce,
			GasLimit:             gasLimit,
			GasPrice:             gasPrice.String(),
			GasUsed:              receipt.GasUsed,
			BaseFee:              baseFee.String(),
			BurntFees:            txBurntFees.String(),
			MaxFeePerGas:         txMaxFeePerGas.String(),
			MaxPriorityFeePerGas: txMaxPriorityFeePerGas.String(),
			Value:                value.String(),
			Input:                txJson.Input,
			ExecStatus:           receipt.Status,
			IsCallContract:       isCallContract,
			IsCreateContract:     isCreateContract,
		}

		// balance
		balanceNativeAddress[fromAddr] = struct{}{}
		if toAddr != "" {
			balanceNativeAddress[toAddr] = struct{}{}
		}

		fullEventList := make([]*data.FullEventLog, 0, len(receipt.Logs))

		for _, txLog := range receipt.Logs {
			// first one is event signature, left is indexed field, up to 3
			var topic0, topic1, topic2, topic3 string
			switch len(txLog.Topics) {
			case 1:
				topic0 = txLog.Topics[0].Hex()
			case 2:
				topic0 = txLog.Topics[0].Hex()
				topic1 = txLog.Topics[1].Hex()
			case 3:
				topic0 = txLog.Topics[0].Hex()
				topic1 = txLog.Topics[1].Hex()
				topic2 = txLog.Topics[2].Hex()
			case 4:
				topic0 = txLog.Topics[0].Hex()
				topic1 = txLog.Topics[1].Hex()
				topic2 = txLog.Topics[2].Hex()
				topic3 = txLog.Topics[3].Hex()
			}
			contractAddr := strings.ToLower(txLog.Address.Hex())

			balanceNativeAddress[contractAddr] = struct{}{}

			topicCount := len(txLog.Topics)

			// event log
			eventLog := &data.EventLog{
				IndexInBlock: uint(txLog.Index),
				ContractAddr: contractAddr,
				TopicCount:   uint(topicCount),
				Topic0:       topic0,
				Topic1:       topic1,
				Topic2:       topic2,
				Topic3:       topic3,
				Data:         txLog.Data,
			}

			fullEventLog := &data.FullEventLog{
				EventLog: eventLog,
			}

			fullEventList = append(fullEventList, fullEventLog)

			// erc20 transfer
			eventErc20Transfer := filter.FilterErc20TransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc20Transfer != nil {
				fullEventLog.EventErc20Transfer = eventErc20Transfer

				sender := eventErc20Transfer.From
				receiver := eventErc20Transfer.To

				if eventErc20Transfer.Amount != "0" {
					if _, ok := balanceErc20Address[sender]; !ok {
						balanceErc20Address[sender] = make(map[string]struct{}, 0)
					}
					if _, ok := balanceErc20Address[receiver]; !ok {
						balanceErc20Address[receiver] = make(map[string]struct{}, 0)
					}

					balanceErc20Address[sender][contractAddr] = struct{}{}
					balanceErc20Address[receiver][contractAddr] = struct{}{}
				}

				if contractAddr != util.ZeroAddress {
					erc20ContractAddrs[contractAddr] = struct{}{}
				}

				balanceNativeAddress[sender] = struct{}{}
				balanceNativeAddress[receiver] = struct{}{}
				balanceNativeAddress[contractAddr] = struct{}{}

				continue
			}

			// erc721 transfer
			eventErc721Transfer := filter.FilterErc721TransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc721Transfer != nil {
				fullEventLog.EventErc721Transfer = eventErc721Transfer

				tokenErc721KeyValue := TokenErc721KeyValue{
					ContractAddr: contractAddr,
					TokenId:      eventErc721Transfer.TokenId,
				}
				tokenErc721Set[tokenErc721KeyValue] = tokenErc721KeyValue

				if contractAddr != util.ZeroAddress {
					erc721ContractAddrs[contractAddr] = struct{}{}
				}

				balanceNativeAddress[eventErc721Transfer.From] = struct{}{}
				balanceNativeAddress[eventErc721Transfer.To] = struct{}{}
				balanceNativeAddress[contractAddr] = struct{}{}

				continue
			}

			// erc1155 single transfer
			eventErc1155Transfer := filter.FilterErc1155SingleTransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc1155Transfer != nil {
				eventErc1155TransferList := make([]*data.EventErc1155Transfer, 0)
				eventErc1155TransferList = append(eventErc1155TransferList, eventErc1155Transfer)
				fullEventLog.EventErc1155Transfers = eventErc1155TransferList

				balanceNativeAddress[eventErc1155Transfer.Operator] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.From] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.To] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.ContractAddr] = struct{}{}

				if _, ok := balanceErc1155Address[eventErc1155Transfer.ContractAddr]; !ok {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr] = make(map[string]string, 0)
				}
				if eventErc1155Transfer.From != util.ZeroAddress {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr][eventErc1155Transfer.TokenId] = eventErc1155Transfer.From
				}
				if eventErc1155Transfer.To != util.ZeroAddress {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr][eventErc1155Transfer.TokenId] = eventErc1155Transfer.To
				}

				continue
			}

			// erc1155 batch transfer
			eventErc1155Transfers := filter.FilterErc1155BatchTransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if len(eventErc1155Transfers) > 0 {
				fullEventLog.EventErc1155Transfers = eventErc1155Transfers

				for _, transfer := range eventErc1155Transfers {
					balanceNativeAddress[transfer.Operator] = struct{}{}
					balanceNativeAddress[transfer.From] = struct{}{}
					balanceNativeAddress[transfer.To] = struct{}{}
					balanceNativeAddress[transfer.ContractAddr] = struct{}{}

					if _, ok := balanceErc1155Address[transfer.ContractAddr]; !ok {
						balanceErc1155Address[transfer.ContractAddr] = make(map[string]string, 0)
					}
					if transfer.From != util.ZeroAddress {
						balanceErc1155Address[transfer.ContractAddr][transfer.TokenId] = transfer.From
					}
					if transfer.To != util.ZeroAddress {
						balanceErc1155Address[transfer.ContractAddr][transfer.TokenId] = transfer.To
					}
				}

				continue
			}
		}

		fullTx := data.FullTx{
			Tx:               tx,
			FullEventLogList: fullEventList,
			TxInternalList:   nil,
		}

		fullTxList = append(fullTxList, &fullTx)
	}

	txParseResult := TxParseResult{
		FullTxList:            fullTxList,
		ContractList:          contractList,
		BalanceNativeAddress:  balanceNativeAddress,
		BalanceErc20Address:   balanceErc20Address,
		BalanceErc1155Address: balanceErc1155Address,
		Erc20ContractAddrs:    erc20ContractAddrs,
		Erc721ContractAddrs:   erc721ContractAddrs,
		TokenErc721Set:        tokenErc721Set,
	}

	return &txParseResult
}

func normalizeTraceAddress(addr string) string {
	if addr == "" || addr == "0x" {
		return ""
	}
	return strings.ToLower(common.HexToAddress(addr).Hex())
}

func parseTraceBigInt(value *hexutil.Big) *big.Int {
	if value == nil {
		return big.NewInt(0)
	}
	return (*big.Int)(value)
}

func walkTxInternalTrace(txHash string, trace *TxInternalJson, traceAddress []uint64, depth int, nextIndex *int, txInternalList *[]*data.TxInternal, contractList *[]*data.Contract, balanceNativeAddress map[string]struct{}) {
	if trace == nil {
		return
	}

	opCode := strings.ToUpper(trace.Type)
	fromAddr := normalizeTraceAddress(trace.From)
	toAddr := normalizeTraceAddress(trace.To)
	value := parseTraceBigInt(trace.Value)
	success := trace.Error == ""

	*txInternalList = append(*txInternalList, &data.TxInternal{
		TxHash:       txHash,
		Index:        *nextIndex,
		From:         fromAddr,
		To:           toAddr,
		OpCode:       opCode,
		Value:        value.String(),
		Success:      success,
		Depth:        depth,
		Gas:          uint64(trace.Gas),
		GasUsed:      uint64(trace.GasUsed),
		Input:        trace.Input,
		Output:       trace.Output,
		TraceAddress: transTraceAddressToString(opCode, traceAddress),
	})
	(*nextIndex)++

	if success && (opCode == "CREATE" || opCode == "CREATE2") && toAddr != "" && toAddr != util.ZeroAddress {
		*contractList = append(*contractList, &data.Contract{
			TxHash:       txHash,
			ContractAddr: toAddr,
			CreatorAddr:  fromAddr,
			ExecStatus:   1,
		})
	}

	if success && value.Sign() > 0 {
		if fromAddr != "" {
			balanceNativeAddress[fromAddr] = struct{}{}
		}
		if toAddr != "" {
			balanceNativeAddress[toAddr] = struct{}{}
		}
	}

	for idx, call := range trace.Calls {
		childTraceAddress := append(append([]uint64(nil), traceAddress...), uint64(idx))
		walkTxInternalTrace(txHash, call, childTraceAddress, depth+1, nextIndex, txInternalList, contractList, balanceNativeAddress)
	}
}

func parseTxInternal(jsonTxInternalList []*TxInternalTraceResultJson, height uint64) *InternalTxParseResult {
	txInternalList := make([]*data.TxInternal, 0)
	contractList := make([]*data.Contract, 0)

	balanceNativeAddress := make(map[string]struct{}, 0)
	_ = height

	for _, traceResult := range jsonTxInternalList {
		if traceResult == nil {
			continue
		}
		if traceResult.Error != "" {
			logrus.Warnf("trace tx failed. tx_hash:%v err:%v", traceResult.TxHash, traceResult.Error)
			continue
		}
		if traceResult.Result == nil {
			continue
		}

		nextIndex := 0
		walkTxInternalTrace(traceResult.TxHash, traceResult.Result, nil, 0, &nextIndex, &txInternalList, &contractList, balanceNativeAddress)
	}

	internalTxParseResult := &InternalTxParseResult{
		InternalTxList:               txInternalList,
		InternalContractList:         contractList,
		InternalBalanceNativeAddress: balanceNativeAddress,
	}

	return internalTxParseResult
}
