package fetcher

import (
	"context"
	"fmt"
	"maps"
	"math/big"
	"scanner_eth/data"
	nodepkg "scanner_eth/fetch/node"
	"scanner_eth/filter"
	"scanner_eth/util"
	"strconv"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	ethTypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
)

const (
	fullBlockRPCRetries = 3
)

var fullBlockRPCRetryInterval = time.Second

// Fetcher captures the fetch capabilities consumed by higher-level
// runtime/orchestration code. Keep this contract in `fetcher` so callers reuse a
// shared boundary instead of redefining local fetch abstractions.
type Fetcher interface {
	FetchBlockHeaderByHeight(context.Context, nodepkg.NodeOperator, int, uint64) *BlockHeaderJson
	FetchBlockHeaderByHash(context.Context, nodepkg.NodeOperator, int, string) *BlockHeaderJson
	FetchFullBlockWithAttempts(context.Context, []nodepkg.NodeOperator, int, *BlockHeaderJson) *FullBlockFetchResult
}

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

type FullBlockRPCAttempt struct {
	OpName     string
	NodeID     int
	CostMicros int64
	Success    bool
	Error      string
}

type FullBlockFetchResult struct {
	FullBlock *data.FullBlock
	Attempts  []FullBlockRPCAttempt
}

type fullBlockRPCResult struct {
	NodeID   int
	OK       bool
	Attempts []FullBlockRPCAttempt
}

// FetcherImpl binds DB-backed full-block assembly helpers into a concrete fetcher.
type FetcherImpl struct {
	db *gorm.DB
}

// NewFetcherImpl creates the production fetcher implementation bound to `db`.
func NewFetcherImpl(db *gorm.DB) *FetcherImpl {
	return &FetcherImpl{db: db}
}

func (bf *FetcherImpl) FetchBlockHeaderByHeight(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, height uint64) *BlockHeaderJson {
	return FetchBlockHeaderByHeight(ctx, nodeOp, taskId, height)
}

func (bf *FetcherImpl) FetchBlockHeaderByHash(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, hash string) *BlockHeaderJson {
	return FetchBlockHeaderByHash(ctx, nodeOp, taskId, hash)
}

func (bf *FetcherImpl) FetchFullBlockWithAttempts(ctx context.Context, nodeOps []nodepkg.NodeOperator, taskId int, header *BlockHeaderJson) *FullBlockFetchResult {
	return FetchFullBlockWithAttempts(ctx, nodeOps, taskId, bf.db, header)
}

func transTraceAddressToString(opcode string, traceAddress []uint64) string {
	var b strings.Builder
	b.Grow(len(opcode) + len(traceAddress)*12)
	b.WriteString(strings.ToLower(opcode))
	for _, addr := range traceAddress {
		b.WriteByte('_')
		b.WriteString(strconv.FormatUint(addr, 10))
	}
	return b.String()
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

func FetchBlockHeaderByHeight(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, height uint64) *BlockHeaderJson {
	if nodeOp == nil {
		return nil
	}
	return nodeOp.FetchBlockHeaderByHeight(ctx, taskId, height)
}

func FetchBlockHeaderByHash(ctx context.Context, nodeOp nodepkg.NodeOperator, taskId int, hash string) *BlockHeaderJson {
	if nodeOp == nil {
		return nil
	}
	return nodeOp.FetchBlockHeaderByHash(ctx, taskId, hash)
}

func withFullBlockRPCRetry(
	ctx context.Context,
	nodeOps []nodepkg.NodeOperator,
	height uint64,
	taskId int,
	opName string,
	call func(nodepkg.NodeOperator) error,
) fullBlockRPCResult {
	if ctx == nil {
		ctx = context.Background()
	}
	nodeOps = compactNodeOperators(nodeOps)
	if len(nodeOps) == 0 || call == nil {
		return fullBlockRPCResult{NodeID: -1}
	}
	lastNodeID := -1
	var lastErr error
	startedAt := time.Now()
	selectedNodeIDs := make([]string, 0, len(nodeOps))
	attempts := make([]FullBlockRPCAttempt, 0, len(nodeOps))

	maxAttempts := fullBlockRPCRetries + 1
	if len(nodeOps) < maxAttempts {
		maxAttempts = len(nodeOps)
	}
	for attempt := 0; attempt < maxAttempts; attempt++ {
		nodeOp := nodeOps[attempt]
		nodeID := nodeOp.ID()
		selectedNodeIDs = append(selectedNodeIDs, strconv.Itoa(nodeID))
		attemptStartedAt := time.Now()
		err := call(nodeOp)
		costMicros := time.Since(attemptStartedAt).Microseconds()
		lastNodeID = nodeID
		if err == nil {
			attempts = append(attempts, FullBlockRPCAttempt{
				OpName:     opName,
				NodeID:     nodeID,
				CostMicros: costMicros,
				Success:    true,
			})
			logrus.Infof("fetch full block rpc success. op:%v nodeId:%v taskId:%v height:%v attempts:%v retries:%v selected_node_ids:%v cost_us:%v",
				opName, nodeID, taskId, height, attempt+1, fullBlockRPCRetries, strings.Join(selectedNodeIDs, ","), time.Since(startedAt).Microseconds())
			return fullBlockRPCResult{NodeID: nodeID, OK: true, Attempts: attempts}
		}

		lastErr = err
		attempts = append(attempts, FullBlockRPCAttempt{
			OpName:     opName,
			NodeID:     nodeID,
			CostMicros: costMicros,
			Success:    false,
			Error:      err.Error(),
		})
		logrus.Warnf("fetch full block rpc failed. op:%v nodeId:%v taskId:%v height:%v attempt:%v retries:%v err:%v",
			opName, nodeID, taskId, height, attempt+1, fullBlockRPCRetries, err)

		if attempt+1 < maxAttempts {
			select {
			case <-ctx.Done():
				lastErr = ctx.Err()
				attempt = maxAttempts
			case <-time.After(fullBlockRPCRetryInterval):
			}
		}
	}

	if lastErr != nil {
		logrus.Warnf("fetch full block rpc exhausted retries. op:%v taskId:%v height:%v attempts:%v retries:%v selected_node_ids:%v cost_us:%v err:%v",
			opName, taskId, height, len(selectedNodeIDs), fullBlockRPCRetries, strings.Join(selectedNodeIDs, ","), time.Since(startedAt).Microseconds(), lastErr)
	}
	return fullBlockRPCResult{NodeID: lastNodeID, Attempts: attempts}
}

func compactNodeOperators(nodeOps []nodepkg.NodeOperator) []nodepkg.NodeOperator {
	compact := make([]nodepkg.NodeOperator, 0, len(nodeOps))
	seen := make(map[int]struct{}, len(nodeOps))
	for _, nodeOp := range nodeOps {
		if nodeOp == nil {
			continue
		}
		nodeID := nodeOp.ID()
		if _, ok := seen[nodeID]; ok {
			continue
		}
		seen[nodeID] = struct{}{}
		compact = append(compact, nodeOp)
	}
	return compact
}

// FetchFullBlockWithAttempts loads txs, receipts, internal traces (if enabled), and token/balance state from RPC using the given header.
func FetchFullBlockWithAttempts(ctx context.Context, nodeOps []nodepkg.NodeOperator, taskId int, db *gorm.DB, header *BlockHeaderJson) *FullBlockFetchResult {
	if ctx == nil {
		ctx = context.Background()
	}
	nodeOps = compactNodeOperators(nodeOps)
	if len(nodeOps) == 0 || header == nil {
		return &FullBlockFetchResult{}
	}
	height := hexutil.MustDecodeUint64(header.Number)
	attempts := make([]FullBlockRPCAttempt, 0)
	fullBlock := fetchFullBlock(ctx, func(opName string, call func(nodepkg.NodeOperator) error) fullBlockRPCResult {
		result := withFullBlockRPCRetry(ctx, nodeOps, height, taskId, opName, call)
		attempts = append(attempts, result.Attempts...)
		return result
	}, taskId, db, header)
	return &FullBlockFetchResult{
		FullBlock: fullBlock,
		Attempts:  attempts,
	}
}

func fetchFullBlock(
	ctx context.Context,
	runRPC func(string, func(nodepkg.NodeOperator) error) fullBlockRPCResult,
	taskId int,
	db *gorm.DB,
	header *BlockHeaderJson,
) *data.FullBlock {
	if ctx == nil {
		ctx = context.Background()
	}
	if runRPC == nil || header == nil {
		return nil
	}
	height := hexutil.MustDecodeUint64(header.Number)
	gasUsed := hexutil.MustDecodeUint64(header.GasUsed)

	var baseFee *big.Int
	if header.BaseFeePerGas != "" {
		baseFee = hexutil.MustDecodeBig(header.BaseFeePerGas)
	}

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
		Height:          hexutil.MustDecodeUint64(header.Number),
		Hash:            header.Hash,
		ParentHash:      header.ParentHash,
		Timestamp:       int64(hexutil.MustDecodeUint64(header.TimeStamp)),
		TxCount:         len(header.Transactions),
		Miner:           header.Miner,
		Size:            int(hexutil.MustDecodeUint64(header.Size)),
		Nonce:           header.Nonce,
		BaseFee:         baseFee.String(),
		BurntFees:       burntFees.String(),
		GasLimit:        hexutil.MustDecodeUint64(header.GasLimit),
		GasUsed:         gasUsed,
		UnclesCount:     len(header.Uncles),
		Difficulty:      difficulty.String(),
		TotalDifficulty: totalDifficulty.String(),
		StateRoot:       header.StateRoot,
		TransactionRoot: header.TransactionRoot,
		ReceiptRoot:     header.ReceiptsRoot,
		ExtraData:       header.ExtraData,
	}

	txList := make([]*TxJson, 0, len(header.Transactions))
	if len(header.Transactions) > 0 {
		startTime := time.Now()
		var txSlots []*TxJson
		rpcResult := runRPC("FetchTransactionsByHashBatch", func(nodeOp nodepkg.NodeOperator) error {
			localSlots := make([]*TxJson, len(header.Transactions))
			for i := range header.Transactions {
				localSlots[i] = &TxJson{}
			}
			if err := nodeOp.FetchTransactionsByHashBatch(ctx, header.Transactions, localSlots); err != nil {
				return err
			}
			for idx, tx := range localSlots {
				if tx == nil || tx.Hash == "" {
					return fmt.Errorf("tx elem invalid at index %d", idx)
				}
			}
			txSlots = localSlots
			return nil
		})
		if !rpcResult.OK {
			logrus.Warnf("fetch tx by header hash failed. nodeId:%v taskId:%v height:%v", rpcResult.NodeID, taskId, height)
			return nil
		}
		for _, tx := range txSlots {
			txList = append(txList, tx)
		}
		logrus.Debugf("fetch tx by header hash success. nodeId:%v taskId:%v height:%v txs:%v cost:%v", rpcResult.NodeID, taskId, height, len(txList), time.Since(startTime).String())
	}

	receipts := make(map[string]*ethTypes.Receipt)
	{
		startTime := time.Now()
		nodeId := -1
		if len(header.Transactions) > 0 {
			rpcResult := runRPC("FetchReceiptsBatch", func(nodeOp nodepkg.NodeOperator) error {
				receiptList := make([]*ethTypes.Receipt, len(header.Transactions))
				localReceipts := make(map[string]*ethTypes.Receipt, len(header.Transactions))
				for i, txHash := range header.Transactions {
					r := &ethTypes.Receipt{}
					receiptList[i] = r
					localReceipts[txHash] = r
				}
				if err := nodeOp.FetchReceiptsBatch(ctx, header.Transactions, receiptList); err != nil {
					return err
				}
				receipts = localReceipts
				return nil
			})
			nodeId = rpcResult.NodeID
			if !rpcResult.OK {
				logrus.Warnf("fetch receipts failed. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
				return nil
			}
		}
		logrus.Debugf("fetch receipts success. nodeId:%v taskId:%v txs:%v height:%v cost:%v", nodeId, taskId, len(txList), height, time.Since(startTime).String())
	}

	txInternalJsonList := make([]*TxInternalTraceResultJson, 0)
	if enableInternalTx {
		rpcResult := runRPC("FetchInternalTxTracesByBlockHash", func(nodeOp nodepkg.NodeOperator) error {
			localList, err := nodeOp.FetchInternalTxTracesByBlockHash(ctx, taskId, header.Hash, height)
			if err != nil {
				return err
			}
			txInternalJsonList = localList
			return nil
		})
		if !rpcResult.OK {
			logrus.Warnf("fetch internal tx failed. nodeId:%v taskId:%v height:%v", rpcResult.NodeID, taskId, height)
			return nil
		}
	}

	txParseResult := parseTx(txList, receipts, height, baseFee)
	internalTxParseResult := parseTxInternal(txInternalJsonList, height)

	balanceNativeAddress := maps.Clone(txParseResult.BalanceNativeAddress)
	for k := range internalTxParseResult.InternalBalanceNativeAddress {
		balanceNativeAddress[k] = struct{}{}
	}

	balanceErc20Address := make(map[string]map[string]struct{})
	for k, v := range txParseResult.BalanceErc20Address {
		for c := range v {
			if _, ok := balanceErc20Address[k]; !ok {
				balanceErc20Address[k] = make(map[string]struct{})
			}
			balanceErc20Address[k][c] = struct{}{}
		}
	}

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

	balanceNativeList := make([]*data.BalanceNative, 0, len(balanceNativeAddress))
	for addr := range balanceNativeAddress {
		balanceNativeList = append(balanceNativeList, &data.BalanceNative{Addr: addr})
	}
	rpcResult := runRPC("FetchBalanceNative", func(nodeOp nodepkg.NodeOperator) error {
		return nodeOp.FetchBalanceNative(ctx, balanceNativeList, height)
	})
	nodeId := rpcResult.NodeID
	if !rpcResult.OK {
		logrus.Warnf("fetch balance failed. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
		return nil
	}

	balanceErc20List := make([]*data.BalanceErc20, 0)
	for addr, v := range balanceErc20Address {
		for contractAddr := range v {
			balanceErc20List = append(balanceErc20List, &data.BalanceErc20{Addr: addr, ContractAddr: contractAddr})
		}
	}
	rpcResult = runRPC("FetchErc20BalancesBatch", func(nodeOp nodepkg.NodeOperator) error {
		return nodeOp.FetchErc20BalancesBatch(ctx, balanceErc20List, height)
	})
	nodeId = rpcResult.NodeID
	if !rpcResult.OK {
		logrus.Warnf("fetch erc20balance failed. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
		return nil
	}

	balanceErc1155List := make([]*data.BalanceErc1155, 0)
	for contractAddr, v := range txParseResult.BalanceErc1155Address {
		for tokenId, addr := range v {
			balanceErc1155List = append(balanceErc1155List, &data.BalanceErc1155{
				Addr:         addr,
				ContractAddr: contractAddr,
				TokenId:      tokenId,
			})
		}
	}
	rpcResult = runRPC("FetchErc1155BalancesBatch", func(nodeOp nodepkg.NodeOperator) error {
		return nodeOp.FetchErc1155BalancesBatch(ctx, balanceErc1155List, height)
	})
	nodeId = rpcResult.NodeID
	if !rpcResult.OK {
		logrus.Warnf("fetch erc1155balance failed. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
		return nil
	}

	contractErc20List := make([]*data.ContractErc20, 0, len(txParseResult.Erc20ContractAddrs))
	for k := range txParseResult.Erc20ContractAddrs {
		contractErc20, ok := tokenCacheInst.Get(k, db)
		if !ok {
			addr := common.HexToAddress(k)
			rpcResult = runRPC("FetchContractErc20", func(nodeOp nodepkg.NodeOperator) error {
				var err error
				contractErc20, err = nodeOp.FetchContractErc20(ctx, &addr, height)
				return err
			})
			nodeId = rpcResult.NodeID
			if !rpcResult.OK {
				logrus.Warnf("fetch erc20 contract failed. nodeId:%v taskId:%v height:%v contract:%v", nodeId, taskId, height, k)
				return nil
			}
		}
		contractErc20List = append(contractErc20List, contractErc20)
	}

	contractErc721List := make([]*data.ContractErc721, 0, len(txParseResult.Erc721ContractAddrs))
	for k := range txParseResult.Erc721ContractAddrs {
		contractErc721, ok := erc721ContractCacheInst.Get(k, db)
		if !ok {
			addr := common.HexToAddress(k)
			rpcResult = runRPC("FetchContractErc721", func(nodeOp nodepkg.NodeOperator) error {
				var err error
				contractErc721, err = nodeOp.FetchContractErc721(ctx, &addr)
				return err
			})
			nodeId = rpcResult.NodeID
			if !rpcResult.OK {
				logrus.Warnf("fetch erc721 contract failed. nodeId:%v taskId:%v height:%v contract:%v", nodeId, taskId, height, k)
				return nil
			}
		}
		contractErc721List = append(contractErc721List, contractErc721)
	}

	tokenErc721List := make([]*data.TokenErc721, 0, len(txParseResult.TokenErc721Set))
	for k := range txParseResult.TokenErc721Set {
		contractAddr := common.HexToAddress(k.ContractAddr)
		tokenId, ok := new(big.Int).SetString(k.TokenId, 10)
		if !ok {
			logrus.Warnf("set string failed for contract:%v tokenId: %v", k.ContractAddr, k.TokenId)
			continue
		}
		var tokenErc721 *data.TokenErc721
		rpcResult = runRPC("FetchTokenErc721", func(nodeOp nodepkg.NodeOperator) error {
			var err error
			tokenErc721, err = nodeOp.FetchTokenErc721(ctx, &contractAddr, tokenId)
			return err
		})
		nodeId = rpcResult.NodeID
		if !rpcResult.OK {
			logrus.Warnf("fetch erc721 token failed. nodeId:%v taskId:%v height:%v contract:%v token_id:%v", nodeId, taskId, height, k.ContractAddr, k.TokenId)
			return nil
		}
		tokenErc721List = append(tokenErc721List, tokenErc721)
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

	return &data.FullBlock{
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
}

func parseTx(jsonTxList []*TxJson, receipts map[string]*ethTypes.Receipt, height uint64, baseFee *big.Int) *TxParseResult {
	fullTxList := make([]*data.FullTx, 0, len(jsonTxList))
	contractList := make([]*data.Contract, 0)
	balanceNativeAddress := make(map[string]struct{})
	balanceErc20Address := make(map[string]map[string]struct{})
	balanceErc1155Address := make(map[string]map[string]string)
	erc20ContractAddrs := make(map[string]struct{})
	erc721ContractAddrs := make(map[string]struct{})
	tokenErc721Set := make(map[TokenErc721KeyValue]TokenErc721KeyValue)

	for _, txJson := range jsonTxList {
		txHash := txJson.Hash
		receipt := receipts[txHash]
		toHex := txJson.To
		isCreateContract := false

		fromAddr := strings.ToLower(txJson.From)
		toAddr := strings.ToLower(txJson.To)
		if toHex == "" || toHex == "0x" || receipt.ContractAddress.Hex() != util.ZeroAddress {
			if receipt.Status == 1 {
				isCreateContract = true
				contractList = append(contractList, &data.Contract{
					TxHash:       txHash,
					ContractAddr: strings.ToLower(receipt.ContractAddress.Hex()),
					CreatorAddr:  fromAddr,
					ExecStatus:   receipt.Status,
				})
			}
		}

		isCallContract := len(txJson.Input) != 0 && txJson.Input != "0x"
		var txType uint64
		if txJson.Type != "" {
			txType = hexutil.MustDecodeUint64(txJson.Type)
		}

		value := hexutil.MustDecodeBig(txJson.Value)
		nonce := hexutil.MustDecodeUint64(txJson.Nonce)
		gasLimit := hexutil.MustDecodeUint64(txJson.Gas)
		gasPrice := hexutil.MustDecodeBig(txJson.GasPrice)

		txBurntFees := big.NewInt(0)
		txMaxFeePerGas := big.NewInt(0)
		txMaxPriorityFeePerGas := big.NewInt(0)
		if txType == ethTypes.DynamicFeeTxType {
			tmp := new(big.Int).SetUint64(receipt.GasUsed)
			txBurntFees = tmp.Mul(tmp, baseFee)
		}
		if txJson.MaxFeePerGas != "" {
			txMaxFeePerGas = hexutil.MustDecodeBig(txJson.MaxFeePerGas)
		}
		if txJson.MaxPriorityFeePerGas != "" {
			txMaxPriorityFeePerGas = hexutil.MustDecodeBig(txJson.MaxPriorityFeePerGas)
		}

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

		balanceNativeAddress[fromAddr] = struct{}{}
		if toAddr != "" {
			balanceNativeAddress[toAddr] = struct{}{}
		}

		fullEventList := make([]*data.FullEventLog, 0, len(receipt.Logs))
		for _, txLog := range receipt.Logs {
			var topicHex [4]string
			for i := 0; i < len(txLog.Topics) && i < 4; i++ {
				topicHex[i] = txLog.Topics[i].Hex()
			}
			topic0, topic1, topic2, topic3 := topicHex[0], topicHex[1], topicHex[2], topicHex[3]
			contractAddr := strings.ToLower(txLog.Address.Hex())
			balanceNativeAddress[contractAddr] = struct{}{}
			topicCount := len(txLog.Topics)
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
			fullEventLog := &data.FullEventLog{EventLog: eventLog}
			fullEventList = append(fullEventList, fullEventLog)

			eventErc20Transfer := filter.FilterErc20TransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc20Transfer != nil {
				fullEventLog.EventErc20Transfer = eventErc20Transfer
				sender := eventErc20Transfer.From
				receiver := eventErc20Transfer.To
				if eventErc20Transfer.Amount != "0" {
					if _, ok := balanceErc20Address[sender]; !ok {
						balanceErc20Address[sender] = make(map[string]struct{})
					}
					if _, ok := balanceErc20Address[receiver]; !ok {
						balanceErc20Address[receiver] = make(map[string]struct{})
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

			eventErc721Transfer := filter.FilterErc721TransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc721Transfer != nil {
				fullEventLog.EventErc721Transfer = eventErc721Transfer
				tokenKey := TokenErc721KeyValue{ContractAddr: contractAddr, TokenId: eventErc721Transfer.TokenId}
				tokenErc721Set[tokenKey] = tokenKey
				if contractAddr != util.ZeroAddress {
					erc721ContractAddrs[contractAddr] = struct{}{}
				}
				balanceNativeAddress[eventErc721Transfer.From] = struct{}{}
				balanceNativeAddress[eventErc721Transfer.To] = struct{}{}
				balanceNativeAddress[contractAddr] = struct{}{}
				continue
			}

			eventErc1155Transfer := filter.FilterErc1155SingleTransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if eventErc1155Transfer != nil {
				fullEventLog.EventErc1155Transfers = []*data.EventErc1155Transfer{eventErc1155Transfer}
				balanceNativeAddress[eventErc1155Transfer.Operator] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.From] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.To] = struct{}{}
				balanceNativeAddress[eventErc1155Transfer.ContractAddr] = struct{}{}
				if _, ok := balanceErc1155Address[eventErc1155Transfer.ContractAddr]; !ok {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr] = make(map[string]string)
				}
				if eventErc1155Transfer.From != util.ZeroAddress {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr][eventErc1155Transfer.TokenId] = eventErc1155Transfer.From
				}
				if eventErc1155Transfer.To != util.ZeroAddress {
					balanceErc1155Address[eventErc1155Transfer.ContractAddr][eventErc1155Transfer.TokenId] = eventErc1155Transfer.To
				}
				continue
			}

			eventErc1155Transfers := filter.FilterErc1155BatchTransferEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3)
			if len(eventErc1155Transfers) > 0 {
				fullEventLog.EventErc1155Transfers = eventErc1155Transfers
				for _, transfer := range eventErc1155Transfers {
					balanceNativeAddress[transfer.Operator] = struct{}{}
					balanceNativeAddress[transfer.From] = struct{}{}
					balanceNativeAddress[transfer.To] = struct{}{}
					balanceNativeAddress[transfer.ContractAddr] = struct{}{}
					if _, ok := balanceErc1155Address[transfer.ContractAddr]; !ok {
						balanceErc1155Address[transfer.ContractAddr] = make(map[string]string)
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

	return &TxParseResult{
		FullTxList:            fullTxList,
		ContractList:          contractList,
		BalanceNativeAddress:  balanceNativeAddress,
		BalanceErc20Address:   balanceErc20Address,
		BalanceErc1155Address: balanceErc1155Address,
		Erc20ContractAddrs:    erc20ContractAddrs,
		Erc721ContractAddrs:   erc721ContractAddrs,
		TokenErc721Set:        tokenErc721Set,
	}
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
	*nextIndex++

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
	balanceNativeAddress := make(map[string]struct{})
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

	return &InternalTxParseResult{
		InternalTxList:               txInternalList,
		InternalContractList:         contractList,
		InternalBalanceNativeAddress: balanceNativeAddress,
	}
}
