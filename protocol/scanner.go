package protocol

type ChainActionType byte

const (
	ChainActionApply ChainActionType = iota
	ChainActionRollback
)

type ChainBinlog struct {
	ChainId    int64           `json:"chain_id"`
	MessageId  uint64          `json:"message_id"`
	ActionType ChainActionType `json:"action_type"`
	Height     uint64          `json:"height"`
	Data       []byte          `json:"full_block"`
}

type FullBlock struct {
	Block               *Block    `json:"block"`
	StateSet            *StateSet `json:"state_set"`
	MemeEvent           []interface{} `json:"meme_event"`
	Erc20PaymentEvent   []interface{} `json:"erc20_payment_event"`
	HybridNftEvent      []interface{} `json:"hybrid_nft_event"`
	NftMarketplaceEvent []interface{} `json:"nft_marketplace_event"`
	UniswapV2Event      []interface{} `json:"uniswap_v2_event"`
}

type Block struct {
	Height          uint64 `json:"height"`
	Hash            string `json:"hash"`
	ParentHash      string `json:"parent_hash"`
	Timestamp       int64  `json:"timestamp"`
	TxCount         int    `json:"tx_count"`
	Miner           string `json:"miner"`
	Size            int    `json:"size"`
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
