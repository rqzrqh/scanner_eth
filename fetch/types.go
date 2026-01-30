package fetch

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

/*
type Tx struct {
	//TxType
	From             string
	To               string
	Hash             string
	Index            int
	Value            *big.Int
	Input            string
	Nonce            uint64
	GasPrice         *big.Int
	GasLimit         uint64
	GasUsed          uint64
	IsContract       bool
	IsContractCreate bool
	BlockTime        int64
	BlockNum         uint64
	BlockHash        string
	ExecStatus       uint64

	EventLogs []*EventLog

	BaseFee              *big.Int
	MaxFeePerGas         *big.Int
	MaxPriorityFeePerGas *big.Int
	BurntFees            *big.Int
}

type EventLog struct {
	TxHash   string
	TopicCnt int
	Topic0   string
	Topic1   string
	Topic2   string
	Topic3   string

	Data  string
	Index uint
	Addr  string
}

type Erc20Transfer struct {
	TxHash          string
	ContractAddr    string
	Sender          string
	Receiver        string
	Tokens          *big.Int
	LogIndex        int
	SenderBalance   *big.Int
	ReceiverBalance *big.Int
}

type Erc20Info struct {
	Addr        string
	Name        string
	Symbol      string
	Decimals    uint8
	TotalSupply string
}

type Erc721Info struct {
	Addr        string
	Name        string
	Symbol      string
	TotalSupply string
}

type Erc721Token struct {
	ContractAddr  string
	TokenId       *big.Int
	OwnerAddr     string
	TokenUri      string
	TokenMetaData string
	Height        uint64
}

type Balance struct {
	Addr         common.Address
	ContractAddr common.Address
	Height       *big.Int
	Value        *big.Int
	ValueBytes   hexutil.Bytes
	ValueHexBig  hexutil.Big
}

type TxInternal struct {
	TxHash       string
	From         string
	To           string
	Value        *big.Int
	Success      bool
	OpCode       string
	Depth        int
	Gas          uint64
	GasUsed      uint64
	Input        string
	Output       string
	TraceAddress []uint64
}

type Contract struct {
	TxHash      string
	Addr        string
	CreatorAddr string
	ExecStatus  uint64
}

type TokenPair struct {
	Addr          common.Address
	PairName      string
	PairNameBytes hexutil.Bytes
	Token0Name    string
	Token1Name    string
	Token0        common.Address
	Token0Bytes   hexutil.Bytes
	Token1        common.Address
	Token1Bytes   hexutil.Bytes
	Reserve0      string
	Reserve1      string
	Updated       time.Time
	BlockNum      uint64
}
*/

type BalanceNative struct {
	Addr        common.Address
	ValueHexBig hexutil.Big
	Height      *big.Int
}

type BalanceErc20 struct {
	Addr         common.Address
	ContractAddr common.Address
	ValueBytes   hexutil.Bytes
	Value        *big.Int
	Height       *big.Int
}
