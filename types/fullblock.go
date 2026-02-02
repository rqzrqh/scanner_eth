package types

import "github.com/shopspring/decimal"

type FullBlock struct {
	Block                    *Block
	TxList                   []*Tx
	TxInternalList           []*TxInternal
	EventLogList             []*EventLog
	EventErc20TransferList   []*EventErc20Transfer
	EventErc721TransferList  []*EventErc721Transfer
	EventErc1155TransferList []*EventErc1155Transfer
	TokenErc721List          []*TokenErc721
	ContractList             []*Contract
	ContractErc20List        []*ContractErc20
	ContractErc721List       []*ContractErc721
	BalanceNativeList        []*BalanceNative
	BalanceErc20List         []*BalanceErc20
	BalanceErc1155List       []*BalanceErc1155
}

type Block struct {
	Height          uint64
	BlockHash       string
	ParentHash      string
	BlockTimestamp  int64
	TxsCount        int
	Miner           string
	Size            int
	Nonce           string
	BaseFee         decimal.Decimal
	BurntFees       decimal.Decimal
	GasLimit        uint64
	GasUsed         uint64
	UnclesCount     int
	Difficulty      decimal.Decimal
	TotalDifficulty decimal.Decimal
	StateRoot       string
	TransactionRoot string
	ReceiptRoot     string
	ExtraData       string
}

type Tx struct {
	TxHash               string
	TxIndex              int
	TxType               int
	From                 string
	To                   string
	Nonce                uint64
	GasLimit             uint64
	GasPrice             decimal.Decimal
	GasUsed              uint64
	BaseFee              decimal.Decimal
	BurntFees            decimal.Decimal
	MaxFeePerGas         decimal.Decimal
	MaxPriorityFeePerGas decimal.Decimal
	Value                decimal.Decimal
	Input                string
	ExecStatus           uint64
	IsCallContract       bool
	IsCreateContract     bool
}

type TxInternal struct {
	TxHash       string
	Index        int
	From         string
	To           string
	OpCode       string
	Value        decimal.Decimal
	Success      bool
	Depth        int
	Gas          uint64
	GasUsed      uint64
	Input        string
	Output       string
	TraceAddress string
}

type EventLog struct {
	TxHash       string
	ContractAddr string
	TopicCount   int
	Topic0       string
	Topic1       string
	Topic2       string
	Topic3       string
	Data         string
	Index        int
}

type EventErc20Transfer struct {
	TxHash       string
	ContractAddr string
	From         string
	To           string
	Amount       decimal.Decimal
	AmountOrigin string
	Index        int
}

type EventErc721Transfer struct {
	TxHash       string
	ContractAddr string
	From         string
	To           string
	TokenId      string
	Index        int
}

type EventErc1155Transfer struct {
	TxHash       string
	ContractAddr string
	Operator     string
	From         string
	To           string
	TokenId      string
	Amount       decimal.Decimal
	Index        int
	IndexInBatch int
}

type TokenErc721 struct {
	ContractAddr  string
	TokenId       string
	OwnerAddr     string
	TokenUri      string
	TokenMetaData []byte
}

type Contract struct {
	TxHash       string
	ContractAddr string
	CreatorAddr  string
	ExecStatus   uint64
}

type ContractErc20 struct {
	TxHash            string
	ContractAddr      string
	CreatorAddr       string
	Name              []byte
	Symbol            []byte
	Decimals          int
	TotalSupply       decimal.Decimal
	TotalSupplyOrigin string
}

type ContractErc721 struct {
	TxHash       string
	ContractAddr string
	CreatorAddr  string
	Name         []byte
	Symbol       []byte
}

type BalanceNative struct {
	Addr    string
	Balance decimal.Decimal
}

type BalanceErc20 struct {
	Addr         string
	ContractAddr string
	Balance      decimal.Decimal
}

type BalanceErc1155 struct {
	Addr         string
	ContractAddr string
	TokenId      string
	Balance      decimal.Decimal
}
