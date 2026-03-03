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
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/sirupsen/logrus"
)

// PairInfo 单个 pair 合约的 factory、token0、token1
type PairInfo struct {
	Factory common.Address
	Token0  common.Address
	Token1  common.Address
}

// callPairGetInfoBatch 支持一批 pair 地址，向节点批量请求且只调用一次 RPC，返回 map[pairAddr]PairInfo
func callPairGetInfoBatch(ctx context.Context, ethClient *ethclient.Client, pairs []common.Address) (map[string]PairInfo, error) {
	if len(pairs) == 0 {
		return make(map[string]PairInfo), nil
	}

	methods := []string{"factory", "token0", "token1"}
	numCalls := len(pairs) * len(methods)
	results := make([]hexutil.Bytes, numCalls)
	batch := make([]rpc.BatchElem, 0, numCalls)

	// 为每个 pair 依次打包 factory、token0、token1 的 calldata，并加入 batch
	for _, contract := range pairs {
		for _, m := range methods {
			in, err := filter.UniswapV2PairABI.Pack(m)
			if err != nil {
				return nil, err
			}
			idx := len(batch)
			batch = append(batch, rpc.BatchElem{
				Method: "eth_call",
				Args: []interface{}{
					map[string]interface{}{
						"to":   contract.Hex(),
						"data": hexutil.Encode(in),
					},
					"latest",
				},
				Result: &results[idx],
			})
		}
	}

	if err := ethClient.Client().BatchCallContext(ctx, batch); err != nil {
		return nil, err
	}

	out := make(map[string]PairInfo, len(pairs))
	for i, contract := range pairs {
		key := strings.ToLower(contract.Hex())
		var info PairInfo
		for j, m := range methods {
			idx := i*len(methods) + j
			if batch[idx].Error != nil {
				return nil, batch[idx].Error
			}
			unpacked, err := filter.UniswapV2PairABI.Unpack(m, results[idx])
			if err != nil {
				logrus.Errorf("pair decode failed for method:%s pair:%s. %v", m, key, err)
				return nil, err
			}
			if len(unpacked) == 0 {
				return nil, fmt.Errorf("empty output for %s pair:%s", m, key)
			}
			addr, ok := unpacked[0].(common.Address)
			if !ok {
				return nil, fmt.Errorf("invalid address output for %s pair:%s", m, key)
			}
			switch m {
			case "factory":
				info.Factory = addr
			case "token0":
				info.Token0 = addr
			case "token1":
				info.Token1 = addr
			}
		}
		out[key] = info
	}
	return out, nil
}

func fetchBalanceNative(client *ethclient.Client, balancesNative []*data.BalanceNative, height uint64) error {
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		err := client.Client().BatchCallContext(ctx, elems)
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

func fetchErc20BalancesBatch(client *ethclient.Client, bs []*data.BalanceErc20, height uint64) error {
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

	err := client.Client().BatchCallContext(context.Background(), elems)
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

func fetchErc1155BalancesBatch(client *ethclient.Client, bs []*data.BalanceErc1155, height uint64) error {
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

	err := client.Client().BatchCallContext(context.Background(), elems)
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

func fetchContractErc20(client *ethclient.Client, addr *common.Address, height uint64) (*data.ContractErc20, error) {
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

	err := client.Client().BatchCall(elems)
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

func fetchContractErc721(client *ethclient.Client, addr *common.Address) (*data.ContractErc721, error) {
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
			Args:   []interface{}{toCallArg2(msg), "latest"},
			Result: &ret,
		}
		elems = append(elems, elem)
	}

	err := client.Client().BatchCall(elems)
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

func fetchTokenErc721(client *ethclient.Client, contractAddr *common.Address, tokenId *big.Int) (*data.TokenErc721, error) {
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
			Args:   []interface{}{toCallArg2(msg), "latest"},
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

	err := client.Client().BatchCall(elems)
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
