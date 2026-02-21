package types

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type UniswapV2Swap struct {
	Pair       common.Address
	Sender     common.Address
	Amount0In  *big.Int
	Amount1In  *big.Int
	Amount0Out *big.Int
	Amount1Out *big.Int
	To         common.Address
}
