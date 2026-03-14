package fetch

import (
	"context"
	"fmt"
	"math/big"
	"strings"
	"time"

	"scanner_eth/data"
	"scanner_eth/filter"
	"scanner_eth/util"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	eth_types "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

// NodeOperatorInterface abstracts batched on-chain reads (balances, contract metadata, etc.).
type NodeOperatorInterface interface {
	ID() int
	FetchBalanceNative(ctx context.Context, balancesNative []*data.BalanceNative, height uint64) error
	FetchErc20BalancesBatch(ctx context.Context, bs []*data.BalanceErc20, height uint64) error
	FetchErc1155BalancesBatch(ctx context.Context, bs []*data.BalanceErc1155, height uint64) error
	ToCallArg(msg ethereum.CallMsg) interface{}
	FetchContractErc20(ctx context.Context, addr *common.Address, height uint64) (*data.ContractErc20, error)
	FetchContractErc721(ctx context.Context, addr *common.Address) (*data.ContractErc721, error)
	FetchTokenErc721(ctx context.Context, contractAddr *common.Address, tokenId *big.Int) (*data.TokenErc721, error)
}

// NodeOperator implements NodeOperatorInterface via ethclient.
type NodeOperator struct {
	id         int
	client     *ethclient.Client
	rpcTimeout time.Duration
}

// NewNodeOperator builds a NodeOperator; id is the index in NodeManager; rpcTimeout is per-RPC deadline (config fetch.timeout; ≤0 uses package default).
func NewNodeOperator(id int, client *ethclient.Client, rpcTimeout time.Duration) *NodeOperator {
	return &NodeOperator{id: id, client: client, rpcTimeout: rpcTimeout}
}

func (n *NodeOperator) ID() int {
	return n.id
}

// EthClient returns the underlying RPC client (for code that still calls ethclient directly, e.g. headers).
func (n *NodeOperator) EthClient() *ethclient.Client {
	return n.client
}

// FetchBlockHeaderByHeight fetches a block header by height (logs include taskId for BlockFetcher, etc.).
func (n *NodeOperator) FetchBlockHeaderByHeight(ctx context.Context, taskId int, height uint64) *BlockHeaderJson {
	if n == nil || n.client == nil {
		return nil
	}
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	nodeId := n.ID()
	blkHeaderJson := &BlockHeaderJson{}
	startTime := time.Now()
	h := new(big.Int).SetUint64(height)
	n.recordRPC("FetchBlockHeaderByHeight", "eth_getBlockByNumber", 1)
	err := n.client.Client().CallContext(rpcCtx, blkHeaderJson, "eth_getBlockByNumber", util.ToBlockNumArg(h), false)
	if err != nil {
		logrus.Warnf("fetch header failed. nodeId:%v taskId:%v error:%v height:%v", nodeId, taskId, err, height)
		return nil
	}
	logrus.Debugf("fetch header success. nodeId:%v taskId:%v height:%v cost:%v", nodeId, taskId, height, time.Since(startTime).String())
	return blkHeaderJson
}

// FetchBlockHeaderByHash fetches a block header by hash.
func (n *NodeOperator) FetchBlockHeaderByHash(ctx context.Context, taskId int, hash string) *BlockHeaderJson {
	if n == nil || n.client == nil {
		return nil
	}
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	nodeId := n.ID()
	blkHeaderJson := &BlockHeaderJson{}
	startTime := time.Now()
	n.recordRPC("FetchBlockHeaderByHash", "eth_getBlockByHash", 1)
	err := n.client.Client().CallContext(rpcCtx, blkHeaderJson, "eth_getBlockByHash", hash, false)
	if err != nil {
		logrus.Warnf("fetch header by hash failed. nodeId:%v taskId:%v error:%v hash:%v", nodeId, taskId, err, hash)
		return nil
	}
	if blkHeaderJson.Hash == "" {
		logrus.Warnf("fetch header by hash empty. nodeId:%v taskId:%v hash:%v", nodeId, taskId, hash)
		return nil
	}
	logrus.Debugf("fetch header by hash success. nodeId:%v taskId:%v hash:%v cost:%v", nodeId, taskId, hash, time.Since(startTime).String())
	return blkHeaderJson
}

// FetchInternalTxTracesByBlockHash uses debug_traceBlockByHash with callTracer for per-tx internal call traces.
func (n *NodeOperator) FetchInternalTxTracesByBlockHash(ctx context.Context, taskId int, blockHash string, height uint64) ([]*TxInternalTraceResultJson, error) {
	if n == nil || n.client == nil {
		return nil, fmt.Errorf("nil NodeOperator or client")
	}
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	nodeId := n.ID()
	arg := map[string]interface{}{
		"tracer": "callTracer",
	}
	const method = "debug_traceBlockByHash"
	n.recordRPC("FetchInternalTxTracesByBlockHash", method, 1)
	var txInternalJsonList []*TxInternalTraceResultJson
	if err := n.client.Client().CallContext(rpcCtx, &txInternalJsonList, method, blockHash, arg); err != nil {
		logrus.Warnf("fetch internal tx failed. nodeId:%v taskId:%v err:%v height:%v", nodeId, taskId, err, height)
		return nil, err
	}
	for _, traceResult := range txInternalJsonList {
		if traceResult == nil {
			logrus.Warnf("fetch internal tx result invalid. nodeId:%v taskId:%v height:%v", nodeId, taskId, height)
			return nil, fmt.Errorf("nil internal trace result")
		}
		if traceResult.Result == nil || traceResult.Error != "" {
			logrus.Warnf("fetch internal tx result invalid. nodeId:%v taskId:%v height:%v tx_hash:%v err:%v", nodeId, taskId, height, traceResult.TxHash, traceResult.Error)
			return nil, fmt.Errorf("invalid internal trace for tx %s", traceResult.TxHash)
		}
	}
	return txInternalJsonList, nil
}

// FetchTransactionsByHashBatch runs batched eth_getTransactionByHash; txHashes and txs align 1:1; results go into txs[i].
func (n *NodeOperator) FetchTransactionsByHashBatch(ctx context.Context, txHashes []string, txs []*TxJson) error {
	if n == nil || n.client == nil {
		return fmt.Errorf("nil NodeOperator or client")
	}
	if len(txHashes) != len(txs) {
		return fmt.Errorf("txHashes len %d != txs len %d", len(txHashes), len(txs))
	}
	if len(txHashes) == 0 {
		return nil
	}
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	elems := make([]rpc.BatchElem, len(txHashes))
	for i := range txHashes {
		if txs[i] == nil {
			return fmt.Errorf("nil tx at index %d", i)
		}
		elems[i] = rpc.BatchElem{
			Method: "eth_getTransactionByHash",
			Args:   []interface{}{txHashes[i]},
			Result: txs[i],
		}
	}
	recordRPCBatchElems(n, "FetchTransactionsByHashBatch", elems)
	if err := n.client.Client().BatchCallContext(rpcCtx, elems); err != nil {
		return err
	}
	for i, elem := range elems {
		if elem.Error != nil {
			return fmt.Errorf("elem(%v): %w", i, elem.Error)
		}
	}
	return nil
}

// FetchReceiptsBatch runs batched eth_getTransactionReceipt; txHashes and receipts align 1:1; results go into receipts[i].
func (n *NodeOperator) FetchReceiptsBatch(ctx context.Context, txHashes []string, receipts []*eth_types.Receipt) error {
	if n == nil || n.client == nil {
		return fmt.Errorf("nil NodeOperator or client")
	}
	if len(txHashes) != len(receipts) {
		return fmt.Errorf("txHashes len %d != receipts len %d", len(txHashes), len(receipts))
	}
	if len(txHashes) == 0 {
		return nil
	}
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	elems := make([]rpc.BatchElem, len(txHashes))
	for i := range txHashes {
		if receipts[i] == nil {
			return fmt.Errorf("nil receipt at index %d", i)
		}
		elems[i] = rpc.BatchElem{
			Method: "eth_getTransactionReceipt",
			Args:   []interface{}{txHashes[i]},
			Result: receipts[i],
		}
	}
	recordRPCBatchElems(n, "FetchReceiptsBatch", elems)
	if err := n.client.Client().BatchCallContext(rpcCtx, elems); err != nil {
		return err
	}
	for i, elem := range elems {
		if elem.Error != nil {
			return fmt.Errorf("elem(%v): %w", i, elem.Error)
		}
	}
	return nil
}

func (n *NodeOperator) FetchBalanceNative(ctx context.Context, balancesNative []*data.BalanceNative, height uint64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	hexBalances := make([]hexutil.Big, len(balancesNative))

	elems := make([]rpc.BatchElem, 0, len(balancesNative)+1)
	for i, ba := range balancesNative {
		elem := rpc.BatchElem{
			Method: "eth_getBalance",
			Args:   []interface{}{common.HexToAddress(ba.Addr), "latest"},
			Result: &hexBalances[i],
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
		rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
		defer cancel()

		recordRPCBatchElems(n, "FetchBalanceNative", elems)
		err := n.client.Client().BatchCallContext(rpcCtx, elems)
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

		for i, ba := range balancesNative {
			ba.Balance = hexBalances[i].ToInt().String()
			ba.UpdateHeight = uint64(blockNumber)
		}

		return nil
	}, 1, time.Second)

	if err != nil {
		logrus.Warnf("fetch balance failed. err:%v", err)
	}

	return err
}

func (n *NodeOperator) FetchErc20BalancesBatch(ctx context.Context, bs []*data.BalanceErc20, height uint64) error {
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	hexBalances := make([]hexutil.Bytes, len(bs))
	elems := make([]rpc.BatchElem, 0, len(bs))
	for i, v := range bs {
		b := v
		input, err := filter.Erc20ABI.Pack("balanceOf", common.HexToAddress(b.Addr))
		if err != nil {
			return fmt.Errorf("panic erc20 balanceOf input err:%v", err)
		}
		arg := map[string]interface{}{
			"from": common.HexToAddress(b.Addr),
			"to":   common.HexToAddress(b.ContractAddr),
			"data": hexutil.Bytes(input),
		}

		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{arg, "latest"},
			Result: &hexBalances[i],
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

	recordRPCBatchElems(n, "FetchErc20BalancesBatch", elems)
	err := n.client.Client().BatchCallContext(rpcCtx, elems)
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

	for i, b := range bs {
		b.UpdateHeight = uint64(blockNumber)
		if len(hexBalances[i]) == 0 {
			b.Balance = "0"
			continue
		}

		rets, err := filter.Erc20ABI.Unpack("balanceOf", hexBalances[i])
		if err != nil {
			logrus.Warnf("unpack erc20 balanceOf err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}
		if len(rets) == 0 {
			logrus.Warnf("erc20 balanceOf ret size err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}

		if v, ok := rets[0].(*big.Int); ok {
			b.Balance = v.String()
		} else {
			logrus.Warnf("erc20 balanceOf ret type error err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
		}
	}

	return nil
}

func (n *NodeOperator) FetchErc1155BalancesBatch(ctx context.Context, bs []*data.BalanceErc1155, height uint64) error {
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	hexBalances := make([]hexutil.Bytes, len(bs))
	elems := make([]rpc.BatchElem, 0, len(bs))
	for i, v := range bs {
		b := v

		tkn := new(big.Int)
		tkn.SetString(b.TokenId, 10)

		input, err := filter.Erc1155ABI.Pack("balanceOf", common.HexToAddress(b.Addr), tkn)
		if err != nil {
			return fmt.Errorf("panic erc1155 balanceOf input err:%v", err)
		}
		arg := map[string]interface{}{
			"from": common.HexToAddress(b.Addr),
			"to":   common.HexToAddress(b.ContractAddr),
			"data": hexutil.Bytes(input),
		}

		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{arg, "latest"},
			Result: &hexBalances[i],
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

	recordRPCBatchElems(n, "FetchErc1155BalancesBatch", elems)
	err := n.client.Client().BatchCallContext(rpcCtx, elems)
	if err != nil {
		return fmt.Errorf("rpc erc1155 balances err:%v", err)
	}

	for _, elem := range elems {
		if elem.Error != nil && !util.HitNoMoreRetryErrors(elem.Error) {
			return fmt.Errorf("erc1155 balances elem err:%v elem:%v", elem.Error, elem)
		}
	}

	if uint64(blockNumber) < height {
		return fmt.Errorf("latest height:%v got cur chain height:%v", height, uint64(blockNumber))
	}

	for i, b := range bs {
		b.UpdateHeight = uint64(blockNumber)
		if len(hexBalances[i]) == 0 {
			b.Balance = "0"
			continue
		}

		rets, err := filter.Erc1155ABI.Unpack("balanceOf", hexBalances[i])
		if err != nil {
			logrus.Warnf("unpack erc1155 balanceOf err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}
		if len(rets) == 0 {
			logrus.Warnf("erc1155 balanceOf ret size err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
			continue
		}

		if v, ok := rets[0].(*big.Int); ok {
			b.Balance = v.String()
		} else {
			logrus.Warnf("erc1155 balanceOf ret type error err:%v contract:%v addr:%v", err, b.ContractAddr, b.Addr)
		}
	}

	return nil
}

func (n *NodeOperator) ToCallArg(msg ethereum.CallMsg) interface{} {
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

func (n *NodeOperator) FetchContractErc20(ctx context.Context, addr *common.Address, height uint64) (*data.ContractErc20, error) {
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	methods := []string{"name", "symbol", "decimals", "totalSupply"}
	elems := make([]rpc.BatchElem, 0)
	for _, method := range methods {
		input, _ := filter.Erc20ABI.Pack(method)
		var ret hexutil.Bytes
		msg := ethereum.CallMsg{
			To:   addr,
			Data: input,
		}
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{n.ToCallArg(msg), "latest"},
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

	recordRPCBatchElems(n, "FetchContractErc20", elems)
	err := n.client.Client().BatchCallContext(rpcCtx, elems)
	if err != nil {
		return nil, fmt.Errorf("batch call get erc20 info failed. err:%v addr:%v", err, addr.Hex())
	}

	if uint64(blockNumber) < height {
		return nil, fmt.Errorf("get erc20 info height too low")
	}

	contractErc20 := &data.ContractErc20{}
	contractErc20.ContractAddr = strings.ToLower(addr.Hex())

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

		rets, err := filter.Erc20ABI.Unpack(methods[i], *ret)
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
				contractErc20.Name = name
			} else {
				logrus.Infof("erc20 info name not string addr:%v", addr.Hex())
			}
		case 1:
			if symbol, ok := rets[0].(string); ok {
				contractErc20.Symbol = symbol
			} else {
				logrus.Infof("erc20 info symbol not string addr:%v", addr.Hex())
			}
		case 2:
			if decimals, ok := rets[0].(uint8); ok {
				contractErc20.Decimals = int(decimals)
			} else {
				logrus.Infof("erc20 info decimals not uint8 addr:%v", addr.Hex())
			}
		case 3:
			if totalSupply, ok := rets[0].(*big.Int); ok {
				contractErc20.TotalSupply = totalSupply.String()
			} else {
				logrus.Infof("erc20 info totalSupply not *big.Int addr:%v", addr.Hex())
			}
		}
	}

	return contractErc20, nil
}

func (n *NodeOperator) FetchContractErc721(ctx context.Context, addr *common.Address) (*data.ContractErc721, error) {
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	methods := []string{"name", "symbol"}
	elems := make([]rpc.BatchElem, 0)
	for _, method := range methods {
		input, _ := filter.Erc721ABI.Pack(method)
		var ret hexutil.Bytes
		msg := ethereum.CallMsg{
			To:   addr,
			Data: input,
		}
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{n.ToCallArg(msg), "latest"},
			Result: &ret,
		}
		elems = append(elems, elem)
	}

	recordRPCBatchElems(n, "FetchContractErc721", elems)
	err := n.client.Client().BatchCallContext(rpcCtx, elems)
	if err != nil {
		return nil, fmt.Errorf("batch call get erc721 info failed. err:%v addr:%v", err, addr.Hex())
	}

	contractErc721 := &data.ContractErc721{}
	contractErc721.ContractAddr = strings.ToLower(addr.Hex())

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
		rets, err := filter.Erc721ABI.Unpack(methods[i], *ret)
		if err != nil {
			logrus.Warnf("erc721 info unpack err:%v addr:%v method:%v", err, addr.Hex(), methods[i])
			continue
		}
		if len(rets) <= 0 {
			logrus.Warnf("erc721 elem rets empty addr:%v", addr.Hex())
			continue
		}

		switch i {
		case 0:
			if name, ok := rets[0].(string); ok {
				contractErc721.Name = name
			} else {
				logrus.Warnf("erc721 info name not string addr:%v", addr.Hex())
			}
		case 1:
			if symbol, ok := rets[0].(string); ok {
				contractErc721.Symbol = symbol
			} else {
				logrus.Warnf("erc721 info symbol not string addr:%v", addr.Hex())
			}
		}
	}

	return contractErc721, nil
}

func (n *NodeOperator) FetchTokenErc721(ctx context.Context, contractAddr *common.Address, tokenId *big.Int) (*data.TokenErc721, error) {
	rpcCtx, cancel := n.withNodeRPCTimeout(ctx)
	defer cancel()
	methods := []string{"ownerOf", "tokenURI"}
	elems := make([]rpc.BatchElem, 0)
	for _, method := range methods {
		input, _ := filter.Erc721ABI.Pack(method, tokenId)
		var ret hexutil.Bytes
		msg := ethereum.CallMsg{
			To:   contractAddr,
			Data: input,
		}
		elem := rpc.BatchElem{
			Method: "eth_call",
			Args:   []interface{}{n.ToCallArg(msg), "latest"},
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

	recordRPCBatchElems(n, "FetchTokenErc721", elems)
	err := n.client.Client().BatchCallContext(rpcCtx, elems)
	if err != nil {
		return nil, fmt.Errorf("batch call get token erc721 info failed. err:%v addr:%v token_id:%v", err, contractAddr.Hex(), tokenId.String())
	}

	tokenErc721 := &data.TokenErc721{}
	tokenErc721.ContractAddr = strings.ToLower(contractAddr.Hex())
	tokenErc721.TokenId = tokenId.String()
	tokenErc721.UpdateHeight = uint64(blockNumber)

	for i, elem := range elems {
		if elem.Method == "eth_blockNumber" {
			continue
		}

		if elem.Error != nil {
			logrus.Warnf("token erc721 info elem err:%v elem:%v method:%v", elem.Error, elem, methods[i])
			continue
		}
		ret := elem.Result.(*hexutil.Bytes)
		if ret == nil || len(*ret) == 0 {
			logrus.Warnf("token erc721 info ret empty addr:%v elem:%v method:%v", contractAddr.Hex(), elem, methods[i])
			continue
		}
		rets, err := filter.Erc721ABI.Unpack(methods[i], *ret)
		if err != nil {
			logrus.Warnf("token erc721 info unpack err:%v addr:%v method:%v", err, contractAddr.Hex(), methods[i])
			continue
		}
		if len(rets) <= 0 {
			logrus.Warnf("token erc721 elem rets empty addr:%v", contractAddr.Hex())
			continue
		}

		switch i {
		case 0:
			if owner, ok := rets[0].(common.Address); ok {
				tokenErc721.OwnerAddr = owner.String()
			} else {
				logrus.Warnf("token erc721 info owner not string addr:%v token_id:%v", contractAddr.Hex(), tokenId.String())
			}
		case 1:
			if tokenURI, ok := rets[0].(string); ok {
				tokenErc721.TokenUri = tokenURI
			} else {
				logrus.Warnf("token erc721 info tokenURI not string addr:%v token_id:%v", contractAddr.Hex(), tokenId.String())
			}
		}
	}

	return tokenErc721, nil
}
