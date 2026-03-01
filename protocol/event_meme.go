package protocol

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type MemeTokenLaunched struct {
	Token         common.Address
	Name          string
	Symbol        string
	Creator       common.Address
	VReserveEth   *big.Int
	VReserveToken *big.Int
	TotalSupply   *big.Int
	AutoBuyAmount *big.Int
	Pair          common.Address
}

type MemeTrade struct {
	Token         common.Address
	EthAmount     *big.Int
	EthFeeAmount  *big.Int
	TokenAmount   *big.Int
	IsBuy         bool
	User          common.Address
	VReserveEth   *big.Int
	VReserveToken *big.Int
}

type MemeLiquiditySwapped struct {
	Token         common.Address
	EthAmount     *big.Int
	TokenAmount   *big.Int
	VReserveEth   *big.Int
	VReserveToken *big.Int
}
