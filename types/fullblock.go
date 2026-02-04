package types

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
	BaseFee         string
	BurntFees       string
	GasLimit        uint64
	GasUsed         uint64
	UnclesCount     int
	Difficulty      string
	TotalDifficulty string
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
	GasPrice             string
	GasUsed              uint64
	BaseFee              string
	BurntFees            string
	MaxFeePerGas         string
	MaxPriorityFeePerGas string
	Value                string
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
	Value        string
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
	IndexInTx    uint
	IndexInBlock uint
	ContractAddr string
	TopicCount   uint
	Topic0       string
	Topic1       string
	Topic2       string
	Topic3       string
	Data         []byte
}

type EventErc20Transfer struct {
	TxHash       string
	IndexInBlock uint
	ContractAddr string
	From         string
	To           string
	Amount       string
}

type EventErc721Transfer struct {
	TxHash       string
	IndexInBlock uint
	ContractAddr string
	From         string
	To           string
	TokenId      string
}

type EventErc1155Transfer struct {
	TxHash       string
	IndexInBlock uint
	IndexInBatch int
	ContractAddr string
	Operator     string
	From         string
	To           string
	TokenId      string
	Amount       string
}

type Contract struct {
	TxHash       string
	ContractAddr string
	CreatorAddr  string
	ExecStatus   uint64
}

type ContractErc20 struct {
	ContractAddr string
	Name         string
	Symbol       string
	Decimals     int
	TotalSupply  string
}

type ContractErc721 struct {
	ContractAddr string
	Name         string
	Symbol       string
}

type BalanceNative struct {
	Addr         string
	Balance      string
	UpdateHeight uint64
}

type BalanceErc20 struct {
	Addr         string
	ContractAddr string
	Balance      string
	UpdateHeight uint64
}

type BalanceErc1155 struct {
	Addr         string
	ContractAddr string
	TokenId      string
	Balance      string
	UpdateHeight uint64
}

type TokenErc721 struct {
	ContractAddr  string
	TokenId       string
	OwnerAddr     string
	TokenUri      string
	TokenMetaData []byte
	UpdateHeight  uint64
}
