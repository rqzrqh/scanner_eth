package fetch

import (
	"context"
	"fmt"
	"math/big"
	"os"
	"strings"
	"sync_eth/model"
	"sync_eth/types"
	"sync_eth/util"
	"time"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/shopspring/decimal"
	"github.com/sirupsen/logrus"
)

const erc20Transfer = `0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef`
const erc1155SingleTransfer = `0xc3d58168c5ae7397731d063d5bbf3d657854427343f4c083240f7aacaa2d0f62`
const erc1155BatchTransfer = `0x4a39dc06d4c0dbc64b70af90fd698a233a518aa5d07e595d983b8c0526c8f7fb`
const erc721Transfer = erc20Transfer

const jsonStrErc20ABI = `[{"constant":true,"inputs":[],"name":"name","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"guy","type":"address"},{"name":"wad","type":"uint256"}],"name":"approve","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"totalSupply","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"src","type":"address"},{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transferFrom","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"name":"wad","type":"uint256"}],"name":"withdraw","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":true,"inputs":[],"name":"decimals","outputs":[{"name":"","type":"uint8"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"}],"name":"balanceOf","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[],"name":"symbol","outputs":[{"name":"","type":"string"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":false,"inputs":[{"name":"dst","type":"address"},{"name":"wad","type":"uint256"}],"name":"transfer","outputs":[{"name":"","type":"bool"}],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[],"name":"deposit","outputs":[],"payable":true,"stateMutability":"payable","type":"function"},{"constant":true,"inputs":[{"name":"","type":"address"},{"name":"","type":"address"}],"name":"allowance","outputs":[{"name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"payable":true,"stateMutability":"payable","type":"fallback"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"guy","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"dst","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Deposit","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"name":"src","type":"address"},{"indexed":false,"name":"wad","type":"uint256"}],"name":"Withdrawal","type":"event"}]`
const jsonStrErc721ABI = `[{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"DODONFTBurn","type":"event"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"creator","type":"address"},{"indexed":false,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"DODONFTMint","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferPrepared","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"previousOwner","type":"address"},{"indexed":true,"internalType":"address","name":"newOwner","type":"address"}],"name":"OwnershipTransferred","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"inputs":[],"name":"_CUR_TOKENID_","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_NEW_OWNER_","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"_OWNER_","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"burn","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"claimOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"string","name":"name","type":"string"},{"internalType":"string","name":"symbol","type":"string"}],"name":"init","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"initOwner","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"string","name":"uri","type":"string"}],"name":"mint","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"name","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"operator","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"symbol","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"uint256","name":"index","type":"uint256"}],"name":"tokenOfOwnerByIndex","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"tokenURI","outputs":[{"internalType":"string","name":"","type":"string"}],"stateMutability":"view","type":"function"},{"inputs":[],"name":"totalSupply","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"stateMutability":"view","type":"function"},{"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newOwner","type":"address"}],"name":"transferOwnership","outputs":[],"stateMutability":"nonpayable","type":"function"}]`
const jsonStrErc1155ABI = `[{"inputs":[{"internalType":"address","name":"_logic","type":"address"},{"internalType":"address","name":"admin_","type":"address"},{"internalType":"bytes","name":"_data","type":"bytes"}],"stateMutability":"payable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":false,"internalType":"address","name":"previousAdmin","type":"address"},{"indexed":false,"internalType":"address","name":"newAdmin","type":"address"}],"name":"AdminChanged","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"beacon","type":"address"}],"name":"BeaconUpgraded","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"implementation","type":"address"}],"name":"Upgraded","type":"event"},{"stateMutability":"payable","type":"fallback"},{"inputs":[],"name":"admin","outputs":[{"internalType":"address","name":"admin_","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newAdmin","type":"address"}],"name":"changeAdmin","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[],"name":"implementation","outputs":[{"internalType":"address","name":"implementation_","type":"address"}],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"}],"name":"upgradeTo","outputs":[],"stateMutability":"nonpayable","type":"function"},{"inputs":[{"internalType":"address","name":"newImplementation","type":"address"},{"internalType":"bytes","name":"data","type":"bytes"}],"name":"upgradeToAndCall","outputs":[],"stateMutability":"payable","type":"function"},{"stateMutability":"payable","type":"receive"}]`

var (
	erc20ABI, erc721ABI, erc1155ABI abi.ABI
)

func InitAbi() {
	var r *strings.Reader
	var err error

	r = strings.NewReader(jsonStrErc20ABI)
	erc20ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}

	r = strings.NewReader(jsonStrErc721ABI)
	erc721ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}

	r = strings.NewReader(jsonStrErc1155ABI)
	erc1155ABI, err = abi.JSON(r)
	if err != nil {
		os.Exit(0)
	}
}

type FetchWorker struct {
	nodeId                   int
	taskId                   int
	client                   *rpc.Client
	height                   uint64
	forkVersion              uint64
	fetchResultNotifyChannel chan<- *types.FetchResult
}

func NewFetchWorker(nodeId int, taskId int, client *rpc.Client, height uint64, forkVersion uint64, fetchResultNotifyChannel chan<- *types.FetchResult) *FetchWorker {
	return &FetchWorker{
		nodeId:                   nodeId,
		taskId:                   taskId,
		client:                   client,
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
						fw.nodeId, fw.taskId, height, forkVersion, fullblock.Block.BlockHash, fullblock.Block.ParentHash, costTime.String())
				} else {
					logrus.Warnf("fetch task failed. nodeId:%v taskId:%v height:%v fork_version:%v try_count:%v", fw.nodeId, fw.taskId, height, forkVersion, tryCount)
				}

				fetchResult := &types.FetchResult{
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

func (fw *FetchWorker) fetch(height uint64) *types.FullBlock {
	return FetchFullBlock(fw.nodeId, fw.taskId, fw.client, height)
}

func isErc20TransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc20Transfer && topic1 != "" && topic2 != "" && topic3 == ""
}

func isErc721TransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc721Transfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func isErc1155SingleTransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc1155SingleTransfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func isErc1155BatchTransferEvent(topic0, topic1, topic2, topic3 string) bool {
	return topic0 == erc1155BatchTransfer && topic1 != "" && topic2 != "" && topic3 != ""
}

func transTraceAddressToString(opcode string, traceAddress []uint64) string {
	var res = strings.ToLower(opcode)
	for _, addr := range traceAddress {
		res = fmt.Sprintf("%s_%d", res, addr)
	}
	return res
}

type BlockHeaderJson struct {
	BaseFeePerGas   string        `json:"baseFeePerGas"`
	Difficulty      string        `json:"difficulty"`
	ExtraData       string        `json:"extraData"`
	GasLimit        string        `json:"gasLimit"`
	GasUsed         string        `json:"gasUsed"`
	Hash            string        `json:"hash"`
	Miner           string        `json:"miner"`
	Nonce           string        `json:"nonce"`
	Number          string        `json:"number"`
	ParentHash      string        `json:"parentHash"`
	ReceiptsRoot    string        `json:"receiptsRoot"`
	Sha3Uncles      string        `json:"sha3Uncles"`
	Size            string        `json:"size"`
	StateRoot       string        `json:"stateRoot"`
	TimeStamp       string        `json:"timestamp"`
	TotalDifficulty string        `json:"totalDifficulty"`
	TransactionRoot string        `json:"transactionsRoot"`
	Uncles          []interface{} `json:"uncles"`
}

type BlockJson struct {
	BlockHeaderJson
	Txs []*TxJson `json:"transactions"`
}

type TxJson struct {
	Hash                 string `json:"hash"`
	From                 string `json:"from"`
	To                   string `json:"to"`
	Gas                  string `json:"gas"`
	GasPrice             string `json:"gasPrice"`
	Input                string `json:"input"`
	MaxFeePerGas         string `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string `json:"maxPriorityFeePerGas"`
	Nonce                string `json:"nonce"`
	R                    string `json:"r"`
	S                    string `json:"s"`
	V                    string `json:"v"`
	TransactionIndex     string `json:"transactionIndex"`
	Type                 string `json:"type"`
	Value                string `json:"value"`
}

type TxInternalJson struct {
	TxHash      string               `json:"transactionHash"`
	BlockHash   string               `json:"blockHash,omitempty"`
	BlockNumber uint64               `json:"blockNumber,omitempty"`
	Logs        []*TxInternalLogJson `json:"logs"`
}

type TxInternalLogJson struct {
	From         string   `json:"From"`
	To           string   `json:"to,omitempty"`
	Value        *big.Int `json:"value,omitempty"`
	Success      bool     `json:"success"`
	OpCode       string   `json:"opcode"`
	Depth        int      `json:"depth"`
	Gas          uint64   `json:"gas"`
	GasUsed      uint64   `json:"gas_used"`
	Input        string   `json:"input"`
	Output       string   `json:"output,omitempty"`
	TraceAddress []uint64 `json:"trace_address"`
}

func FetchFullBlock(nodeId int, taskId int, client *rpc.Client, height uint64) *types.FullBlock {
	blkJson := &BlockJson{}
	// fetch block with txs
	{
		startTime := time.Now()
		h := new(big.Int).SetUint64(height)
		err := client.Call(blkJson, "eth_getBlockByNumber", util.ToBlockNumArg(h), true)
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

	modelBlock := &model.Block{
		Height:         hexutil.MustDecodeUint64(blkJson.Number),
		BlockHash:      blkJson.Hash,
		ParentHash:     blkJson.ParentHash,
		BlockTimestamp: int64(hexutil.MustDecodeUint64(blkJson.TimeStamp)),
		TxsCount:       len(blkJson.Txs),
		Miner:          blkJson.Miner,
		Size:           int(hexutil.MustDecodeUint64(blkJson.Size)),
		Nonce:          blkJson.Nonce,
		BaseFee:        decimal.NewFromBigInt(baseFee, 0),
		BurntFees:      decimal.NewFromBigInt(burntFees, 0),
		GasLimit:       hexutil.MustDecodeUint64(blkJson.GasLimit),
		GasUsed:        gasUsed,

		UnclesCount: len(blkJson.Uncles),

		Difficulty:      decimal.NewFromBigInt(difficulty, 0),
		TotalDifficulty: decimal.NewFromBigInt(totalDifficulty, 0),
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
			if err := client.BatchCallContext(context.Background(), elemsReceipts); err != nil {
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
	txInternalJsonList := make([]*TxInternalJson, 0)
	/* need to change evm code
	{
		arg := map[string]interface{}{}
		method := "debug_traceActionByBlockNumber"
		if err := client.CallContext(context.Background(), &txInternalJsonList, method, util.ToBlockNumArg(new(big.Int).SetUint64(height)), arg); err != nil {
			logrus.Warnf("fetch internal tx failed. nodeId:%v taskId:%v err:%v height:%v", nodeId, taskId, err, height)
			return nil
		}
	}
	*/

	// parse txs
	txSet, txBalanceNativeAddress, txBalanceErc20Address, txBalanceErc1155Address, erc20ContractAddrs, erc721ContractAddrs, modelTxList, modelEventLogList, modelEventErc20TransferList, modelEventErc721TransferList, modelTokenErc721List, modelEventErc1155TransferList, modelTxContractList := parseTx(blkJson.Txs, receipts, height, baseFee)

	// parse internal txs
	modelTxInternalList, modelTxInternalContractList, txInternalBalanceAddress, txInternalBalanceErc20Address := parseTxInternal(txInternalJsonList, height)

	balanceNativeAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)

	modelContractList := make([]*model.Contract, 0)
	modelContractList = append(modelContractList, modelTxContractList...)
	modelContractList = append(modelContractList, modelTxInternalContractList...)

	// get all native token balance changed accounts
	for k := range txBalanceNativeAddress {
		balanceNativeAddress[k] = struct{}{}
	}
	for k := range txInternalBalanceAddress {
		balanceNativeAddress[k] = struct{}{}
	}

	// get all erc20 token balance changed accounts
	for k, v := range txBalanceErc20Address {
		for c := range v {
			if _, ok := balanceErc20Address[k]; !ok {
				balanceErc20Address[k] = make(map[string]struct{}, 0)
			}
			balanceErc20Address[k][c] = struct{}{}
		}
	}
	for k, v := range txInternalBalanceErc20Address {
		for c := range v {
			if _, ok := balanceErc20Address[k]; !ok {
				balanceErc20Address[k] = make(map[string]struct{}, 0)
			}
			balanceErc20Address[k][c] = struct{}{}
		}
	}

	// fetch native token balance changed account's balance
	modelBalanceNativeList := make([]*model.BalanceNative, 0)
	{
		balances := make([]*types.BalanceNative, 0)
		for addr := range balanceNativeAddress {
			balances = append(balances, &types.BalanceNative{
				Addr: common.HexToAddress(addr),
			})
		}

		if err := fetchBalanceNative(client, balances, height); err != nil {
			logrus.Warnf("fetch balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}

		for _, v := range balances {
			modelBalanceNative := &model.BalanceNative{
				Addr:         v.Addr.Hex(),
				Balance:      decimal.NewFromBigInt(v.ValueHexBig.ToInt(), 0),
				UpdateHeight: v.Height.Uint64(),
			}
			modelBalanceNativeList = append(modelBalanceNativeList, modelBalanceNative)
		}
	}

	// fetch erc20 token balance changed account's erc20 balance
	modelBalanceErc20List := make([]*model.BalanceErc20, 0)
	{
		balancesErc20 := make([]*types.BalanceErc20, 0)
		for addr, v := range balanceErc20Address {
			for contractAddr := range v {
				balancesErc20 = append(balancesErc20, &types.BalanceErc20{
					Addr:         common.HexToAddress(addr),
					ContractAddr: common.HexToAddress(contractAddr),
				})
			}
		}

		if err := fetchErc20BalancesBatch(client, balancesErc20, height); err != nil {
			logrus.Warnf("fetch erc20balance failed. nodeId:%v taskId:%v height:%v err:%v", nodeId, taskId, height, err)
			return nil
		}

		for _, v := range balancesErc20 {
			modelBalanceErc20 := &model.BalanceErc20{
				Addr:         v.Addr.Hex(),
				ContractAddr: v.ContractAddr.Hex(),
				Balance:      decimal.NewFromBigInt(v.Value, 0),
				UpdateHeight: v.Height.Uint64(),
			}
			modelBalanceErc20List = append(modelBalanceErc20List, modelBalanceErc20)
		}
	}

	// get new erc20 contract info
	modelContractErc20List := make([]*model.ContractErc20, 0)
	{
		// TODO check if in database
		for k, txHash := range erc20ContractAddrs {
			addr := common.HexToAddress(k)
			modelContractErc20, err := fetchContractErc20(client, &addr, height)
			if err != nil {
				return nil
			}
			if txHash != "" {
				modelContractErc20.Height = height
				modelContractErc20.TxHash = txHash
				modelContractErc20.CreatorAddr = strings.ToLower(txSet[txHash].From)
			}

			modelContractErc20List = append(modelContractErc20List, modelContractErc20)
		}
	}

	// get new erc721 contract info
	modelContractErc721List := make([]*model.ContractErc721, 0)
	{
		// TODO check if in database

		for k, txHash := range erc721ContractAddrs {
			addr := common.HexToAddress(k)
			modelContractErc721, err := fetchContractErc721(client, &addr)
			if err != nil {
				return nil
			}
			if txHash != "" {
				modelContractErc721.Height = height
				modelContractErc721.TxHash = txHash
				modelContractErc721.CreatorAddr = strings.ToLower(txSet[txHash].From)
			}

			modelContractErc721List = append(modelContractErc721List, modelContractErc721)
		}
	}

	fmt.Println("erc1155 contract count:", len(txBalanceErc1155Address))

	modelBalanceErc1155List := make([]*model.BalanceErc1155, 0)

	fullblock := &types.FullBlock{
		Block:                   modelBlock,
		TxList:                  modelTxList,
		TxInternalList:          modelTxInternalList,
		EventLogList:            modelEventLogList,
		EventErc20TransferList:  modelEventErc20TransferList,
		EventErc721TransferList: modelEventErc721TransferList,
		TokenErc721List:         modelTokenErc721List,

		EventErc1155TransferList: modelEventErc1155TransferList,

		ContractList:       modelContractList,
		ContractErc20List:  modelContractErc20List,
		ContractErc721List: modelContractErc721List,

		BalanceNativeList:  modelBalanceNativeList,
		BalanceErc20List:   modelBalanceErc20List,
		BalanceErc1155List: modelBalanceErc1155List,
	}

	return fullblock
}

func parseTx(jsonTxList []*TxJson, receipts map[string]*eth_types.Receipt, height uint64, baseFee *big.Int) (
	map[string]*TxJson, map[string]struct{}, map[string]map[string]struct{}, map[string]map[string]map[string]struct{}, map[string]string, map[string]string,
	[]*model.Tx, []*model.EventLog, []*model.EventErc20Transfer, []*model.EventErc721Transfer, []*model.TokenErc721, []*model.EventErc1155Transfer, []*model.Contract,
) {

	txSet := make(map[string]*TxJson, 0)
	balanceNativeAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)
	balanceErc1155Address := make(map[string]map[string]map[string]struct{}, 0)

	erc20ContractAddrs := make(map[string]string, 0)
	erc721ContractAddrs := make(map[string]string, 0)

	modelTxList := make([]*model.Tx, 0)
	modelEventLogList := make([]*model.EventLog, 0)

	modelEventErc20TransferList := make([]*model.EventErc20Transfer, 0)
	modelEventErc721TransferList := make([]*model.EventErc721Transfer, 0)
	modelTokenErc721List := make([]*model.TokenErc721, 0)
	modelEventErc1155TransferList := make([]*model.EventErc1155Transfer, 0)

	modelContractList := make([]*model.Contract, 0)

	for _, txJson := range jsonTxList {
		txHash := txJson.Hash
		receipt := receipts[txHash]
		var (
			toHex            string = txJson.To
			isCreateContract bool
		)

		txSet[txJson.Hash] = txJson

		fromAddr := strings.ToLower(txJson.From)
		toAddr := strings.ToLower(txJson.To)

		// get new create contract
		if toHex == "" || toHex == "0x" || receipt.ContractAddress.Hex() != util.ZeroAddress {
			if receipt.Status == 1 {
				isCreateContract = true
				modelContract := &model.Contract{
					Height:       height,
					TxHash:       txHash,
					ContractAddr: strings.ToLower(receipt.ContractAddress.Hex()),
					CreatorAddr:  fromAddr,
					ExecStatus:   receipt.Status,
				}
				modelContractList = append(modelContractList, modelContract)
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

		txIndex := hexutil.MustDecodeUint64(txJson.TransactionIndex)
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
		modelTx := &model.Tx{
			Height:               height,
			TxType:               int(txType),
			TxHash:               txHash,
			TxIndex:              int(txIndex),
			From:                 fromAddr,
			To:                   toAddr,
			Nonce:                nonce,
			GasLimit:             gasLimit,
			GasPrice:             decimal.NewFromBigInt(gasPrice, 0),
			GasUsed:              receipt.GasUsed,
			BaseFee:              decimal.NewFromBigInt(baseFee, 0),
			BurntFees:            decimal.NewFromBigInt(txBurntFees, 0),
			MaxFeePerGas:         decimal.NewFromBigInt(txMaxFeePerGas, 0),
			MaxPriorityFeePerGas: decimal.NewFromBigInt(txMaxPriorityFeePerGas, 0),
			Value:                decimal.NewFromBigInt(value, 0),
			Input:                txJson.Input,
			ExecStatus:           receipt.Status,
			IsCallContract:       isCallContract,
			IsCreateContract:     isCreateContract,
		}
		modelTxList = append(modelTxList, modelTx)

		// balance
		balanceNativeAddress[fromAddr] = struct{}{}
		balanceNativeAddress[toAddr] = struct{}{}

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

			// event log
			modelEventLog := &model.EventLog{
				Height:       height,
				TxHash:       txHash,
				TopicCount:   len(txLog.Topics),
				Topic0:       topic0,
				Topic1:       topic1,
				Topic2:       topic2,
				Topic3:       topic3,
				Data:         hexutil.Encode(txLog.Data),
				Index:        int(txLog.Index),
				ContractAddr: contractAddr,
			}
			modelEventLogList = append(modelEventLogList, modelEventLog)

			if isErc20TransferEvent(topic0, topic1, topic2, topic3) {
				sender := topic1
				receiver := topic2
				tokenAmount := new(big.Int)
				tokenAmount.SetBytes(txLog.Data)

				// erc20 balance
				if tokenAmount.Uint64() != 0 {
					if _, ok := balanceErc20Address[sender]; !ok {
						balanceErc20Address[sender] = make(map[string]struct{}, 0)
					}
					if _, ok := balanceErc20Address[receiver]; !ok {
						balanceErc20Address[receiver] = make(map[string]struct{}, 0)
					}

					balanceErc20Address[sender][contractAddr] = struct{}{}
					balanceErc20Address[receiver][contractAddr] = struct{}{}
				}

				// tx erc20
				tokenCnt := tokenAmount.String()
				var tokenCntOrigin string
				if len(tokenCnt) > 65 {
					tokenCntOrigin = tokenCnt
					tokenCnt = tokenCnt[:65]
				}
				amount, _ := decimal.NewFromString(tokenCnt)

				modelEventErc20Transfer := &model.EventErc20Transfer{
					Height:       height,
					TxHash:       txHash,
					ContractAddr: contractAddr,
					From:         sender,
					To:           receiver,
					Amount:       amount,
					AmountOrigin: tokenCntOrigin,
					Index:        int(txLog.Index),
				}
				modelEventErc20TransferList = append(modelEventErc20TransferList, modelEventErc20Transfer)

				// consider success
				if contractAddr != util.ZeroAddress {
					if isCreateContract {
						erc20ContractAddrs[contractAddr] = txHash
					} else {
						erc20ContractAddrs[contractAddr] = ""
					}
				}

			} else if isErc721TransferEvent(topic0, topic1, topic2, topic3) {
				sender := strings.ToLower(common.HexToAddress(topic1).Hex())
				receiver := strings.ToLower(common.HexToAddress(topic2).Hex())
				tokenId := common.HexToHash(topic3).Big().String()

				// tx erc721
				modelEventErc721Transfer := &model.EventErc721Transfer{
					Height:       height,
					TxHash:       txHash,
					ContractAddr: contractAddr,
					From:         sender,
					To:           receiver,
					TokenId:      tokenId,
					Index:        int(txLog.Index),
				}
				modelEventErc721TransferList = append(modelEventErc721TransferList, modelEventErc721Transfer)
				// token erc721
				modelTokenErc721 := &model.TokenErc721{
					ContractAddr:  contractAddr,
					TokenId:       tokenId,
					OwnerAddr:     receiver,
					TokenUri:      "",
					TokenMetaData: []byte(""),
					UpdateHeight:  height,
				}
				modelTokenErc721List = append(modelTokenErc721List, modelTokenErc721)

				// consider success
				if contractAddr != util.ZeroAddress {
					if isCreateContract {
						erc721ContractAddrs[contractAddr] = txHash
					} else {
						erc721ContractAddrs[contractAddr] = ""
					}
				}
			} else if isErc1155SingleTransferEvent(topic0, topic1, topic2, topic3) {
				var transferSingleData struct {
					Id    *big.Int
					Value *big.Int
				}

				if err := erc1155ABI.UnpackIntoInterface(&transferSingleData, "TransferSingle", txLog.Data); err != nil {
					logrus.Errorf("erc1155 single err:%v height:%v", err, height)
					continue
				}

				operator := strings.ToLower(common.HexToAddress(topic1).Hex())
				sender := strings.ToLower(common.HexToAddress(topic2).Hex())
				receiver := strings.ToLower(common.HexToAddress(topic3).Hex())
				tokenId := transferSingleData.Id.String()

				// balance erc1155
				if sender != util.ZeroAddress {
					balanceErc1155Address[sender][contractAddr][tokenId] = struct{}{}
				}
				if receiver != util.ZeroAddress {
					balanceErc1155Address[receiver][contractAddr][tokenId] = struct{}{}
				}

				// tx erc1155
				tokens := transferSingleData.Value
				tokenCnt := tokens.String()
				if len(tokenCnt) > 65 {
					tokenCnt = tokenCnt[:65]
				}
				amount, _ := decimal.NewFromString(tokenCnt)

				modelEventErc1155Transfer := &model.EventErc1155Transfer{
					Height:       height,
					TxHash:       txHash,
					ContractAddr: contractAddr,
					Operator:     operator,
					From:         sender,
					To:           receiver,
					TokenId:      tokenId,
					Amount:       amount,
					Index:        int(txLog.Index),
					IndexInBatch: -1,
				}
				modelEventErc1155TransferList = append(modelEventErc1155TransferList, modelEventErc1155Transfer)
			} else if isErc1155BatchTransferEvent(topic0, topic1, topic2, topic3) {
				var transferBatchData struct {
					Ids    []*big.Int
					Values []*big.Int
				}

				if err := erc1155ABI.UnpackIntoInterface(&transferBatchData, "TransferBatch", txLog.Data); err != nil {
					logrus.Errorf("erc1155 batch UnpackIntoInterface err:%v height:%v", err, height)
					continue
				}

				ids := transferBatchData.Ids
				values := transferBatchData.Values
				if len(values) != len(ids) {
					logrus.Warnf("erc1155 batch nonstandard tx_hash:%v index:%v height:%v", txHash, txLog.Index, height)
					continue
				}

				operator := strings.ToLower(common.HexToAddress(topic1).Hex())
				sender := strings.ToLower(common.HexToAddress(topic2).Hex())
				receiver := strings.ToLower(common.HexToAddress(topic3).Hex())

				for index, id := range ids {
					tokenId := id.String()

					if sender != util.ZeroAddress {
						balanceErc1155Address[sender][contractAddr][tokenId] = struct{}{}
					}
					if receiver != util.ZeroAddress {
						balanceErc1155Address[receiver][contractAddr][tokenId] = struct{}{}
					}

					// tx erc1155
					tokenCnt := values[index].String()
					if len(tokenCnt) > 65 {
						tokenCnt = tokenCnt[:65]
					}
					amount, _ := decimal.NewFromString(tokenCnt)

					modelEventErc1155Transfer := &model.EventErc1155Transfer{
						Height:       height,
						TxHash:       txHash,
						ContractAddr: contractAddr,
						Operator:     operator,
						From:         sender,
						To:           receiver,
						TokenId:      tokenId,
						Amount:       amount,
						Index:        int(txLog.Index),
						IndexInBatch: index,
					}
					modelEventErc1155TransferList = append(modelEventErc1155TransferList, modelEventErc1155Transfer)
				}
			}
		}
	}

	return txSet, balanceNativeAddress, balanceErc20Address, balanceErc1155Address, erc20ContractAddrs, erc721ContractAddrs, modelTxList, modelEventLogList, modelEventErc20TransferList, modelEventErc721TransferList, modelTokenErc721List, modelEventErc1155TransferList, modelContractList
}

func parseTxInternal(jsonTxInternalList []*TxInternalJson, height uint64) ([]*model.TxInternal, []*model.Contract, map[string]struct{}, map[string]map[string]struct{}) {
	modelTxInternalList := make([]*model.TxInternal, 0)
	modelContractList := make([]*model.Contract, 0)

	balanceAddress := make(map[string]struct{}, 0)
	balanceErc20Address := make(map[string]map[string]struct{}, 0)

	for _, v := range jsonTxInternalList {
		txHash := v.TxHash
		for tiIdx, tiLog := range v.Logs {
			fromAddr := strings.ToLower(tiLog.From)
			toAddr := strings.ToLower(tiLog.To)

			if tiLog.OpCode == "CREATE" || tiLog.OpCode == "CREATE2" {
				if tiLog.Success {
					var status uint64 = 1
					if tiLog.To == util.ZeroAddress {
						logrus.Fatal("internal tx empty txhash:%v from:%v to:%v", txHash, fromAddr, toAddr)
					}
					modelContract := &model.Contract{
						Height:       height,
						TxHash:       txHash,
						ContractAddr: toAddr,
						CreatorAddr:  fromAddr,
						ExecStatus:   status,
					}
					modelContractList = append(modelContractList, modelContract)
				}
			}

			modelTxInternal := &model.TxInternal{
				Height:       height,
				TxHash:       txHash,
				Index:        tiIdx,
				From:         fromAddr,
				To:           toAddr,
				OpCode:       tiLog.OpCode,
				Value:        decimal.NewFromBigInt(tiLog.Value, 0),
				Success:      tiLog.Success,
				Depth:        tiLog.Depth,
				Gas:          tiLog.Gas,
				GasUsed:      tiLog.GasUsed,
				Input:        tiLog.Input,
				Output:       tiLog.Output,
				TraceAddress: transTraceAddressToString(tiLog.OpCode, tiLog.TraceAddress),
			}
			modelTxInternalList = append(modelTxInternalList, modelTxInternal)

			if tiLog.Success && tiLog.Value.Cmp(big.NewInt(0)) > 0 {
				balanceAddress[fromAddr] = struct{}{}
				balanceAddress[toAddr] = struct{}{}
			}
		}
	}

	return modelTxInternalList, modelContractList, balanceAddress, balanceErc20Address
}

func fetchBalanceNative(client *rpc.Client, balancesNative []*types.BalanceNative, height uint64) error {
	elems := make([]rpc.BatchElem, 0)
	for _, ba := range balancesNative {
		elem := rpc.BatchElem{
			Method: "eth_getBalance",
			Args:   []interface{}{ba.Addr, "latest"},
			Result: &ba.ValueHexBig,
		}
		elems = append(elems, elem)
	}

	var blockNumber hexutil.Uint64
	heightReq := rpc.BatchElem{
		Method: "eth_blockNumber",
		Args:   []interface{}{},
		Result: &blockNumber,
	}
	elems = append(elems, heightReq)

	err := util.HandleErrorWithRetry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := client.BatchCallContext(ctx, elems)
		if err != nil {
			return err
		}

		for _, e := range elems {
			if e.Error != nil {
				return e.Error
			}
		}

		if uint64(blockNumber) < height {
			return fmt.Errorf("latest height:%v got cur chain height:%v", height, uint64(blockNumber))
		}

		for _, ba := range balancesNative {
			ba.Height = big.NewInt(int64(blockNumber))
		}

		return nil
	}, 1, time.Second)

	if err != nil {
		logrus.Warnf("fetch balance failed. err:%v", err)
	}

	return err
}

func fetchErc20BalancesBatch(client *rpc.Client, bs []*types.BalanceErc20, height uint64) error {
	elems := make([]rpc.BatchElem, 0, len(bs))
	for _, v := range bs {
		b := v
		input, err := erc20ABI.Pack("balanceOf", b.Addr)
		if err != nil {
			return fmt.Errorf("panic erc20 balanceOf input err:%v", err)
		}
		arg := map[string]interface{}{
			"from": b.Addr,
			"to":   &b.ContractAddr,
			"data": hexutil.Bytes(input),
		}

		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{arg, "latest"},
			Result: &b.ValueBytes,
		}
		elems = append(elems, elem)
	}

	var blockNumber hexutil.Uint64
	heightReq := rpc.BatchElem{
		Method: "eth_blockNumber",
		Args:   []interface{}{},
		Result: &blockNumber,
	}
	elems = append(elems, heightReq)

	err := client.BatchCallContext(context.Background(), elems)
	if err != nil {
		return fmt.Errorf("rpc erc20 balances err:%v", err)
	}

	for _, elem := range elems {
		if elem.Error != nil && !util.HitNoMoreRetryErrors(elem.Error) {
			return fmt.Errorf("erc20 balances elem err:%v elem:%v", elem.Error, elem)
		}
	}

	if uint64(blockNumber) < height {
		return fmt.Errorf("latest height:%v got cur chain height:%v", height, uint64(blockNumber))
	}

	for _, b := range bs {
		b.Height = new(big.Int).SetUint64(uint64(blockNumber))
		if len(b.ValueBytes) == 0 {
			continue
		}

		rets, err := erc20ABI.Unpack("balanceOf", b.ValueBytes)
		if err != nil {
			logrus.Warnf("unpack erc20 balanceOf err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}
		if len(rets) == 0 {
			logrus.Warnf("erc20 balanceOf ret size err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}

		if v, ok := rets[0].(*big.Int); ok {
			b.Value = v
		} else {
			logrus.Warnf("erc20 balanceOf ret type error err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
		}
	}

	return nil
}

func toCallArg(msg ethereum.CallMsg) interface{} {
	arg := map[string]interface{}{
		"from": msg.From,
		"to":   msg.To,
	}
	if len(msg.Data) > 0 {
		arg["data"] = hexutil.Bytes(msg.Data)
	}
	if msg.Value != nil {
		arg["value"] = (*hexutil.Big)(msg.Value)
	}
	if msg.Gas != 0 {
		arg["gas"] = hexutil.Uint64(msg.Gas)
	}
	if msg.GasPrice != nil {
		arg["gasPrice"] = (*hexutil.Big)(msg.GasPrice)
	}
	return arg
}

func fetchContractErc20(client *rpc.Client, addr *common.Address, height uint64) (*model.ContractErc20, error) {
	methods := []string{"name", "symbol", "decimals", "totalSupply"}
	elems := make([]rpc.BatchElem, 0)
	for _, method := range methods {
		input, _ := erc20ABI.Pack(method)
		var ret hexutil.Bytes
		msg := ethereum.CallMsg{
			To:   addr,
			Data: input,
		}
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{toCallArg(msg), "latest"},
			Result: &ret,
		}
		elems = append(elems, elem)
	}

	var blockNumber hexutil.Uint64
	heightReq := rpc.BatchElem{
		Method: "eth_blockNumber",
		Args:   []interface{}{},
		Result: &blockNumber,
	}
	elems = append(elems, heightReq)

	err := client.BatchCall(elems)
	if err != nil {
		return nil, fmt.Errorf("batch call get erc20 info failed. err:%v addr:%v", err, addr.Hex())
	}

	if uint64(blockNumber) < height {
		return nil, fmt.Errorf("get erc20 info height too low")
	}

	modelContractErc20 := &model.ContractErc20{}
	modelContractErc20.ContractAddr = strings.ToLower(addr.Hex())

	for i, elem := range elems {
		if elem.Method == "eth_blockNumber" {
			continue
		}

		if elem.Error != nil {
			logrus.Infof("erc20 info elem err:%v elem:%v method:%v", elem.Error, elem, methods[i])
			continue
		}

		ret := elem.Result.(*hexutil.Bytes)
		if ret == nil || len(*ret) == 0 {
			logrus.Infof("erc20 info ret empty addr:%v elem:%v method:%v", addr.Hex(), elem, methods[i])
			continue
		}

		rets, err := erc20ABI.Unpack(methods[i], *ret)
		if err != nil {
			logrus.Infof("erc20 info unpack failed. err:%v addr:%v method:%v ret:%v", err, addr.Hex(), methods[i], *ret)
			continue
		}

		if len(rets) <= 0 {
			logrus.Infof("elem rets empty addr:%v", addr.Hex())
			continue
		}

		switch i {
		case 0:
			if name, ok := rets[0].(string); ok {
				modelContractErc20.Name = []byte(name)
			} else {
				logrus.Infof("erc20 info name not string addr:%v", addr.Hex())
			}
		case 1:
			if symbol, ok := rets[0].(string); ok {
				modelContractErc20.Symbol = []byte(symbol)
			} else {
				logrus.Infof("erc20 info symbol not string addr:%v", addr.Hex())
			}
		case 2:
			if decimals, ok := rets[0].(uint8); ok {
				modelContractErc20.Decimals = int(decimals)
			} else {
				logrus.Infof("erc20 info decimals not uint8 addr:%v", addr.Hex())
			}
		case 3:
			if totalSupply, ok := rets[0].(*big.Int); ok {
				modelContractErc20.TotalSupplyOrigin = totalSupply.String()
				modelContractErc20.TotalSupply = decimal.NewFromBigInt(totalSupply, 0)
			} else {
				logrus.Infof("erc20 info totalSupply not *big.Int addr:%v", addr.Hex())
			}
		}
	}

	return modelContractErc20, nil
}

func toCallArg2(msg ethereum.CallMsg) interface{} {
	arg := map[string]interface{}{
		"from": msg.From,
		"to":   msg.To,
	}
	if len(msg.Data) > 0 {
		arg["data"] = hexutil.Bytes(msg.Data)
	}
	if msg.Value != nil {
		arg["value"] = (*hexutil.Big)(msg.Value)
	}
	if msg.Gas != 0 {
		arg["gas"] = hexutil.Uint64(msg.Gas)
	}
	if msg.GasPrice != nil {
		arg["gasPrice"] = (*hexutil.Big)(msg.GasPrice)
	}

	return arg
}

func fetchContractErc721(client *rpc.Client, addr *common.Address) (*model.ContractErc721, error) {
	methods := []string{"name", "symbol"}
	elems := make([]rpc.BatchElem, 0)
	for _, method := range methods {
		input, _ := erc721ABI.Pack(method)
		var ret hexutil.Bytes
		msg := ethereum.CallMsg{
			To:   addr,
			Data: input,
		}
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{toCallArg2(msg), "latest"},
			Result: &ret,
		}
		elems = append(elems, elem)
	}

	err := client.BatchCall(elems)
	if err != nil {
		return nil, fmt.Errorf("batch call get erc721 info failed. err:%v addr:%v", err, addr.Hex())
	}

	modelContractErc721 := &model.ContractErc721{}
	modelContractErc721.ContractAddr = strings.ToLower(addr.Hex())

	for i, elem := range elems {
		if elem.Error != nil {
			logrus.Warnf("erc721 info elem err:%v elem:%v method:%v", elem.Error, elem, methods[i])
			continue
		}
		ret := elem.Result.(*hexutil.Bytes)
		if ret == nil || len(*ret) == 0 {
			logrus.Warnf("erc721 info ret empty addr:%v elem:%v method:%v", addr.Hex(), elem, methods[i])
			continue
		}
		rets, err := erc721ABI.Unpack(methods[i], *ret)
		if err != nil {
			logrus.Warnf("erc721 info unpack err:%v addr:%v method:%v", err, addr.Hex(), methods[i])
			continue
		}
		if len(rets) <= 0 {
			logrus.Warnf("elem rets empty addr:%v", addr.Hex())
			continue
		}

		switch i {
		case 0:
			if name, ok := rets[0].(string); ok {
				modelContractErc721.Name = []byte(name)
			} else {
				logrus.Warnf("erc721 info name not string addr:%v", addr.Hex())
			}
		case 1:
			if symbol, ok := rets[0].(string); ok {
				modelContractErc721.Symbol = []byte(symbol)
			} else {
				logrus.Warnf("erc721 info symbol not string addr:%v", addr.Hex())
			}
		}
	}

	return modelContractErc721, nil
}
