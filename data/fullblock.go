package data

type FullBlock struct {
	Block      *Block
	FullTxList []*FullTx
	StateSet   *StateSet
}

type Block struct {
	Height          uint64
	Hash            string
	ParentHash      string
	Timestamp       int64
	TxCount         int
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

type FullTx struct {
	Tx               *Tx
	FullEventLogList []*FullEventLog
	TxInternalList   []*TxInternal
}

type Tx struct {
	TxHash               string
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

type FullEventLog struct {
	EventLog              *EventLog
	EventErc20Transfer    *EventErc20Transfer
	EventErc721Transfer   *EventErc721Transfer
	EventErc1155Transfers []*EventErc1155Transfer

	MemeEvent           interface{}
	Erc20PaymentEvent   interface{}
	HybridNftEvent      interface{}
	NftMarketplaceEvent interface{}
	UniswapV2Event      interface{}
}

type EventLog struct {
	IndexInBlock uint
	ContractAddr string
	TopicCount   uint
	Topic0       string
	Topic1       string
	Topic2       string
	Topic3       string
	Data         []byte
}

type Contract struct {
	TxHash       string
	ContractAddr string
	CreatorAddr  string
	ExecStatus   uint64
}

type StateSet struct {
	ContractList       []*Contract
	ContractErc20List  []*ContractErc20
	ContractErc721List []*ContractErc721
	BalanceNativeList  []*BalanceNative
	BalanceErc20List   []*BalanceErc20
	BalanceErc1155List []*BalanceErc1155
	TokenErc721List    []*TokenErc721
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
