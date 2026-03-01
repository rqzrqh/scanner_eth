package protocol

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
)

type Erc20PaymentTransferEvent struct {
	Token  common.Address
	From   common.Address
	To     common.Address
	Amount *big.Int
	Memo   []byte
}
