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
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
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

type FetchWorker struct {
	nodeId                   int
	taskId                   int
	client                   *ethclient.Client
	db                       *gorm.DB
	height                   uint64
	forkVersion              uint64
	fetchResultNotifyChannel chan<- *FetchResult
}

func NewFetchWorker(nodeId int, taskId int, client *ethclient.Client, db *gorm.DB, height uint64, forkVersion uint64, fetchResultNotifyChannel chan<- *FetchResult) *FetchWorker {
	return &FetchWorker{
		nodeId:                   nodeId,
		taskId:                   taskId,
		client:                   client,
		db:                       db,
		height:                   height,
		forkVersion:              forkVersion,
		fetchResultNotifyChannel: fetchResultNotifyChannel,
	}
}

func (fw *FetchWorker) Run() {
	logrus.Infof("new fetch task. nodeId:%v taskId:%v height:%v fork_version:%v", fw.nodeId, fw.taskId, fw.height, fw.forkVersion)

	go func() {
		height := fw.height
		forkVersion := fw.forkVersion
		tryCount := 0

		for {
			tryCount++
			logrus.Debugf("start fetch. nodeId:%v taskId:%v height:%v fork_version:%v try_count:%v", fw.nodeId, fw.taskId, height, forkVersion, tryCount)
			startTime := time.Now()
			fullblock := fw.fetch(height)
			costTime := time.Since(startTime)

			if tryCount >= 3 || fullblock != nil {

				if fullblock != nil {
					logrus.Infof("fetch task success. nodeId:%v taskId:%v height:%v fork_version:%v hash:%v parent_hash:%v cost:%v",
						fw.nodeId, fw.taskId, height, forkVersion, fullblock.Block.Hash, fullblock.Block.ParentHash, costTime.String())
				} else {
					logrus.Warnf("fetch task failed. nodeId:%v taskId:%v height:%v fork_version:%v try_count:%v", fw.nodeId, fw.taskId, height, forkVersion, tryCount)
				}

				fetchResult := &FetchResult{
					NodeId:      fw.nodeId,
					TaskId:      fw.taskId,
					ForkVersion: forkVersion,
					Height:      height,
					FullBlock:   fullblock,
					CostTime:    costTime,
				}
				fw.fetchResultNotifyChannel <- fetchResult
				break
			}
		}
	}()
}

func (fw *FetchWorker) fetch(height uint64) *data.FullBlock {
	return FetchFullBlock(fw.nodeId, fw.taskId, fw.client, fw.db, height)
}

func transTraceAddressToString(opcode string, traceAddress []uint64) string {
	var res = strings.ToLower(opcode)
	for _, addr := range traceAddress {
		res = fmt.Sprintf("%s_%d", res, addr)
	}
	return res
}

type TxParseResult struct {
	FullTxList                 []*data.FullTx
	ContractList               []*data.Contract
	BalanceNativeAddress       map[string]struct{}
	BalanceErc20Address        map[string]map[string]struct{}
	BalanceErc1155Address      map[string]map[string]string
	Erc20ContractAddrs         map[string]struct{}
	Erc721ContractAddrs        map[string]struct{}
	TokenErc721Set             map[TokenErc721KeyValue]TokenErc721KeyValue
	UniswapV2PairContractAddrs map[string]struct{}
}

type InternalTxParseResult struct {
	InternalTxList               []*data.TxInternal
	InternalContractList         []*data.Contract
	InternalBalanceNativeAddress map[string]struct{}
}

// parse tx and log
// fetch receipts and internal tx
// fetch latest state, like erc20/erc721/erc1155 token info
// the reason why not fetch the state of one specific height is fast node always lack historical state, only archive node has it.
func FetchFullBlock(nodeId int, taskId int, client *ethclient.Client, db *gorm.DB, height uint64) *data.FullBlock {
	blkJson := &BlockJson{}
	// fetch block with txs
	{
		startTime := time.Now()
		h := new(big.Int).SetUint64(height)
		err := client.Client().Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(h), true)
		if err != nil {
			logrus.Warnf("fetch header failed. nodeId:%v taskId:%v error:%v height:%v", nodeId, taskId, err, height)
			return nil
		}

		logrus.Debugf("fetch header success. nodeId:%v taskId:%v txs:%v height:%v cost:%v", nodeId, taskId, len(blkJson.Txs), height, time.Since(startTime).String())
	}

	gasUsed := hexutil.MustDecodeUint64(blkJson.GasUsed)

	var baseFee *big.Int
	if blkJson.BaseFeePerGas != "" {
		baseFee = hexutil.MustDecodeBig(blkJson.BaseFeePerGas)
	}

	// eip1559 set burnt fees
	burntFees := new(big.Int)
	if baseFee != nil {
		burntFees = burntFees.Mul(new(big.Int).SetUint64(gasUsed), baseFee)
	}

	if baseFee == nil {
		baseFee = big.NewInt(0)
	}

	//difficulty := hexutil.MustDecodeBig(blkJson.Difficulty)
	//totalDifficulty := hexutil.MustDecodeBig(blkJson.TotalDifficulty)
	difficulty := big.NewInt(0)
	totalDifficulty := big.NewInt(0)

	blk := &data.Block{
		Height:     hexutil.MustDecodeUint64(blkJson.Number),
		Hash:       blkJson.Hash,
		ParentHash: blkJson.ParentHash,
		Timestamp:  int64(hexutil.MustDecodeUint64(blkJson.TimeStamp)),
		TxCount:    len(blkJson.Txs),
		Miner:      blkJson.Miner,
		Size:       int(hexutil.MustDecodeUint64(blkJson.Size)),
		Nonce:      blkJson.Nonce,
		BaseFee:    baseFee.String(),
		BurntFees:  burntFees.String(),
		GasLimit:   hexutil.MustDecodeUint64(blkJson.GasLimit),
		GasUsed:    gasUsed,

		UnclesCount: len(blkJson.Uncles),

		Difficulty:      difficulty.String(),
		TotalDifficulty: totalDifficulty.String(),
		StateRoot:       blkJson.StateRoot,
		TransactionRoot: blkJson.TransactionRoot,
		ReceiptRoot:     blkJson.ReceiptsRoot,
		ExtraData:       blkJson.ExtraData,
	}

	receipts := make(map[string]*eth_types.Receipt)

	// fetch receipts
	{
		startTime := time.Now()
		elemsReceipts := make([]rpc.BatchElem, len(blkJson.Txs))
		for i, tx := range blkJson.Txs {
			receipt := &eth_types.Receipt{}
			elemsReceipt := rpc.BatchElem{
				Method: "eth_getTransactionReceipt",
				Args:   []interface{}{tx.Hash},
				Result: receipt,
			}
			receipts[tx.Hash] = receipt
			elemsReceipts[i] = elemsReceipt
		}

		if len(elemsReceipts) > 0 {
			if err := client.Client().BatchCallContext(context.Background(), elemsReceipts); err != nil {
				logrus.Warnf("fetch receipts failed. nodeId:%v taskId:%v height:%v error:%v", nodeId, taskId, height, err)
				return nil
			}

			for idx, elem := range elemsReceipts {
				if elem.Error != nil {
					logrus.Warnf("fetch receipts elem failed. nodeId:%v taskId:%v height:%v elem(%v) error:%v", nodeId, taskId, height, idx, elem.Error)
					return nil
				}
			}
		}
		logrus.Debugf("fetch receipts success. nodeId:%v taskId:%v txs:%v height:%v cost:%v", nodeId, taskId, len(blkJson.Txs), height, time.Since(startTime).String())
	}

	// fetch internal tx
	txInternalJsonList := make([]*TxInternalTraceResultJson, 0)
	if enableInternalTx {
		arg := map[string]interface{}{
			"tracer": "callTracer",
		}
		method := "debug_traceBlockByHash"
		if err := client.Client().CallContext(context.Background(), &txInternalJsonList, method, blkJson.Hash, arg); err != nil {
			logrus.Warnf("fetch internal tx failed. nodeId:%v taskId:%v err:%v height:%v", nodeId, taskId, err, height)
			return nil
		}
		for _, traceResult := range txInternalJsonList {
			if traceResult == nil {
				logrus.Warnf("fetch internal tx result invalid. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
				return nil
			}
			if traceResult.Result == nil || traceResult.Error != "" {
				logrus.Warnf("fetch internal tx result invalid. nodeId:%v taskId:%v height:%v tx_hash:%v err:%v", nodeId, taskId, height, traceResult.TxHash, traceResult.Error)
				return nil
			}
		}
	}

	// parse txs
	txParseResult := parseTx(blkJson.Txs, receipts, height, baseFee)

	// parse internal txs
	internalTxParseResult := parseTxInternal(txInternalJsonList, height)

	balanceNativeAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)

	// 按合约地址去重合并 contract
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

		if err := fetchBalanceNative(client, balanceNativeList, height); err != nil {
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

		if err := fetchErc20BalancesBatch(client, balanceErc20List, height); err != nil {
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

		if err := fetchErc1155BalancesBatch(client, balanceErc1155List, height); err != nil {
			logrus.Warnf("fetch erc1155balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}
	}

	// 对 UniswapV2 pair 批量拉取 token0/token1，与 Erc20ContractAddrs 合并后统一拉取 ERC20 合约信息
	erc20AddrsMerged := make(map[string]struct{}, len(txParseResult.Erc20ContractAddrs))
	for k := range txParseResult.Erc20ContractAddrs {
		erc20AddrsMerged[k] = struct{}{}
	}
	if len(txParseResult.UniswapV2PairContractAddrs) > 0 {
		pairAddrs := make([]common.Address, 0, len(txParseResult.UniswapV2PairContractAddrs))
		for k := range txParseResult.UniswapV2PairContractAddrs {
			pairAddrs = append(pairAddrs, common.HexToAddress(k))
		}
		pairInfoMap, err := callPairGetInfoBatch(context.Background(), client, pairAddrs)
		if err != nil {
			logrus.Warnf("callPairGetInfoBatch failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}
		for _, info := range pairInfoMap {
			t0 := strings.ToLower(info.Token0.Hex())
			t1 := strings.ToLower(info.Token1.Hex())
			if t0 != util.ZeroAddress {
				erc20AddrsMerged[t0] = struct{}{}
			}
			if t1 != util.ZeroAddress {
				erc20AddrsMerged[t1] = struct{}{}
			}
		}
	}

	// get new erc20 contract info（先查缓存，不存在则向节点获取）
	contractErc20List := make([]*data.ContractErc20, 0, len(erc20AddrsMerged))
	{
		for k := range erc20AddrsMerged {
			contractErc20, ok := tokenCacheInst.Get(k, db)
			if !ok {
				addr := common.HexToAddress(k)
				var err error
				contractErc20, err = fetchContractErc20(client, &addr, height)
				if err != nil {
					return nil
				}
			}
			contractErc20List = append(contractErc20List, contractErc20)
		}
	}

	// get new erc721 contract info（先查缓存，不存在则向节点获取）
	contractErc721List := make([]*data.ContractErc721, 0, len(txParseResult.Erc721ContractAddrs))
	{
		for k := range txParseResult.Erc721ContractAddrs {
			contractErc721, ok := erc721ContractCacheInst.Get(k, db)
			if !ok {
				addr := common.HexToAddress(k)
				var err error
				contractErc721, err = fetchContractErc721(client, &addr)
				if err != nil {
					return nil
				}
			}
			contractErc721List = append(contractErc721List, contractErc721)
		}
	}

	// get new erc721 token info（不缓存，直接链上拉取）
	tokenErc721List := make([]*data.TokenErc721, 0, len(txParseResult.TokenErc721Set))
	{
		for k := range txParseResult.TokenErc721Set {
			contractAddr := common.HexToAddress(k.ContractAddr)
			tokenId, ok := new(big.Int).SetString(k.TokenId, 10)
			if !ok {
				logrus.Warnf("set string failed for contract:%v tokenId: %v", k.ContractAddr, k.TokenId)
				continue
			}
			tokenErc721, err := fetchTokenErc721(client, &contractAddr, tokenId)
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
	uniswapV2PairContractAddrs := make(map[string]struct{}, 0)

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

		// TODO need it?
		//txIndex := hexutil.MustDecodeUint64(txJson.TransactionIndex)
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

			// erc20 payment event
			erc20PaymentEvent := filter.FilterErc20PaymentEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount)
			if erc20PaymentEvent != nil {
				fullEventLog.Erc20PaymentEvent = erc20PaymentEvent
				continue
			}

			// meme event
			memeEvent := filter.FilterMemeEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount)
			if memeEvent != nil {
				fullEventLog.MemeEvent = memeEvent
				continue
			}

			// uniswapv2 event
			uniswapv2Event := filter.FilterUniswapV2Event(txHash, tx.To, txLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount)
			if uniswapv2Event != nil {
				fullEventLog.UniswapV2Event = uniswapv2Event
				// 从 UniswapV2 事件中获取 pair 地址放入 UniswapV2PairContractAddrs
				if swapEvent, ok := uniswapv2Event.(*data.UniswapV2Swap); ok && swapEvent.Pair != util.ZeroAddress {
					uniswapV2PairContractAddrs[swapEvent.Pair] = struct{}{}
				}
				continue
			}

			// hybrid nft event
			hybridNftEvent := filter.FilterHybridNftEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount)
			if hybridNftEvent != nil {
				fullEventLog.HybridNftEvent = hybridNftEvent
				/*
					if _, ok := hybridNftEvent.(*data.HybridPublicConfigChanged); ok {
					} else if _, ok := hybridNftEvent.(*data.HybridWhitelistConfigChanged); ok {
					} else {
						logrus.Panic("unknown hybrid nft event type")
					}
				*/
				continue
			}

			// nft marketplace event
			nftMarketplaceEvent := filter.FilterNftMarketplaceEvent(txHash, txLog, contractAddr, height, topic0, topic1, topic2, topic3, topicCount)
			if nftMarketplaceEvent != nil {
				fullEventLog.NftMarketplaceEvent = nftMarketplaceEvent
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
		FullTxList:                 fullTxList,
		ContractList:               contractList,
		BalanceNativeAddress:       balanceNativeAddress,
		BalanceErc20Address:        balanceErc20Address,
		BalanceErc1155Address:      balanceErc1155Address,
		Erc20ContractAddrs:         erc20ContractAddrs,
		Erc721ContractAddrs:        erc721ContractAddrs,
		TokenErc721Set:             tokenErc721Set,
		UniswapV2PairContractAddrs: uniswapV2PairContractAddrs,
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
