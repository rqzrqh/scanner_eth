package protocol

type ChainActionType byte

const (
	ChainActionApply ChainActionType = iota
	ChainActionRollback
)

type ScannerData struct {
	MessageId  uint64          `json:"message_id"`
	ActionType ChainActionType `json:"action_type"`
	Height     uint64          `json:"height"`
	FullBlock  []byte          `json:"full_block"`
}

type FullBlock struct {
	Height          uint64 `json:"height"`
	BlockHash       string `json:"block_hash"`
	ParentHash      string `json:"parent_block_hash"`
	BlockTimestamp  int64  `json:"block_timestamp"`
	TxsCount        int    `json:"block_tx_count"`
	Miner           string `json:"miner"`
	Size            int    `json:"block_size"`
	Nonce           string `json:"nonce"`
	BaseFee         string `json:"base_fee"`
	BurntFees       string `json:"burnt_fees"`
	GasLimit        uint64 `json:"gas_limit"`
	GasUsed         uint64 `json:"gas_used"`
	UnclesCount     int    `json:"uncles_count"`
	Difficulty      string `json:"difficulty"`
	TotalDifficulty string `json:"total_difficulty"`
	StateRoot       string `json:"state_root"`
	TransactionRoot string `json:"transaction_root"`
	ReceiptRoot     string `json:"receipt_root"`
	ExtraData       string `json:"extra_data"`

	FullTxList []*FullTx `json:"full_tx_list"`
	StateSet   *StateSet `json:"state_set"`
}

type FullTx struct {
	Tx               *Tx             `json:"tx"`
	FullEventLogList []*FullEventLog `json:"full_event_log_list"`
	TxInternalList   []*TxInternal   `json:"tx_internal_list"`
	ContractList     []*Contract     `json:"contract_list"`
}

type Tx struct {
	TxHash               string `json:"tx_hash"`
	TxIndex              int    `json:"tx_index"`
	TxType               int    `json:"tx_type"`
	From                 string `json:"from"`
	To                   string `json:"to"`
	Nonce                uint64 `json:"nonce"`
	GasLimit             uint64 `json:"gas_limit"`
	GasPrice             string `json:"gas_price"`
	GasUsed              uint64 `json:"gas_used"`
	BaseFee              string `json:"base_fee"`
	BurntFees            string `json:"burnt_fees"`
	MaxFeePerGas         string `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas string `json:"max_priority_fee_per_gas"`
	Value                string `json:"value"`
	Input                string `json:"input"`
	ExecStatus           uint64 `json:"exec_status"`
	IsCallContract       bool   `json:"is_call_contract"`
	IsCreateContract     bool   `json:"is_create_contract"`
}

type TxInternal struct {
	Index        int    `json:"index"`
	From         string `json:"from"`
	To           string `json:"to"`
	OpCode       string `json:"op_code"`
	Value        string `json:"value"`
	Success      bool   `json:"success"`
	Depth        int    `json:"call_depth"`
	Gas          uint64 `json:"gas"`
	GasUsed      uint64 `json:"gas_used"`
	Input        string `json:"input"`
	Output       string `json:"output"`
	TraceAddress string `json:"trace_address"`
}

type FullEventLog struct {
	EventLog             *EventLog             `json:"event_log"`
	EventErc20Transfer   *EventErc20Transfer   `json:"event_erc20_transfer"`
	EventErc721Transfer  *EventErc721Transfer  `json:"event_erc721_transfer"`
	EventErc1155Transfer *EventErc1155Transfer `json:"event_erc1155_transfer"`
}

type EventLog struct {
	IndexInBlock uint   `json:"index_in_block"`
	ContractAddr string `json:"contract_addr"`
	TopicCount   uint   `json:"topic_count"`
	Topic0       string `json:"topic0"`
	Topic1       string `json:"topic1"`
	Topic2       string `json:"topic2"`
	Topic3       string `json:"topic3"`
	Data         []byte `json:"data"`
}

type EventErc20Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	Amount       string `json:"amount"`
}

type EventErc721Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
}

type EventErc1155Transfer struct {
	IndexInBlock uint   `json:"index_in_block"`
	IndexInBatch int    `json:"index_in_batch"`
	ContractAddr string `json:"contract_addr"`
	Operator     string `json:"operator"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
	Amount       string `json:"amount"`
}

type Contract struct {
	ContractAddr string `json:"contract_addr"`
	CreatorAddr  string `json:"creator_addr"`
	ExecStatus   uint64 `json:"exec_status"`
}

type StateSet struct {
	ContractErc20List  []*ContractErc20  `json:"contract_erc20_list"`
	ContractErc721List []*ContractErc721 `json:"contract_erc721_list"`
	BalanceNativeList  []*BalanceNative  `json:"balance_native_list"`
	BalanceErc20List   []*BalanceErc20   `json:"balance_erc20_list"`
	BalanceErc1155List []*BalanceErc1155 `json:"balance_erc1155_list"`
	TokenErc721List    []*TokenErc721    `json:"token_erc721_list"`
}

type ContractErc20 struct {
	ContractAddr string `json:"contract_addr"`
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
	Decimals     int    `json:"decimals"`
	TotalSupply  string `json:"total_supply"`
}

type ContractErc721 struct {
	ContractAddr string `json:"contract_addr"`
	Name         string `json:"name"`
	Symbol       string `json:"symbol"`
}

type BalanceNative struct {
	Addr         string `json:"addr"`
	Balance      string `json:"balance"`
	UpdateHeight uint64 `json:"update_height"`
}

type BalanceErc20 struct {
	Addr         string `json:"addr"`
	ContractAddr string `json:"contract_addr"`
	Balance      string `json:"balance"`
	UpdateHeight uint64 `json:"update_height"`
}

type BalanceErc1155 struct {
	Addr         string `json:"addr"`
	ContractAddr string `json:"contract_addr"`
	TokenId      string `json:"token_id"`
	Balance      string `json:"balance"`
	UpdateHeight uint64 `json:"update_height"`
}

type TokenErc721 struct {
	ContractAddr  string `json:"contract_addr"`
	TokenId       string `json:"token_id"`
	OwnerAddr     string `json:"owner_addr"`
	TokenUri      string `json:"token_uri"`
	TokenMetaData []byte `json:"token_meta_data"`
	UpdateHeight  uint64 `json:"update_height"`
}
