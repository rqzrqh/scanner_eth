package protocol

import (
	"github.com/shopspring/decimal"
)

type ProtocolFullBlock struct {
	Block                    *Block                  `json:"block"`
	TxList                   []*Tx                   `json:"tx_list"`
	EventLogList             []*EventLog             `json:"event_log_list"`
	EventErc20TransferList   []*EventErc20Transfer   `json:"event_erc20_transfer_list"`
	EventErc721TransferList  []*EventErc721Transfer  `json:"event_erc721_transfer_list"`
	EventErc1155TransferList []*EventErc1155Transfer `json:"event_erc1155_transfer_list"`
	TokenErc721List          []*TokenErc721          `json:"token_erc721_list"`
	ContractList             []*Contract             `json:"contract_list"`
	ContractErc20List        []*ContractErc20        `json:"contract_erc20_list"`
	ContractErc721List       []*ContractErc721       `json:"contract_erc721_list"`
	BalanceNativeList        []*BalanceNative        `json:"balance_native_list"`
	BalanceErc20List         []*BalanceErc20         `json:"balance_erc20_list"`
	BalanceErc1155List       []*BalanceErc1155       `json:"balance_erc1155_list"`
}

type Block struct {
	Height         uint64          `json:"height"`
	BlockHash      string          `json:"block_hash"`
	ParentHash     string          `json:"parent_block_hash"`
	BlockTimestamp int64           `json:"block_timestamp"`
	TxsCount       int             `json:"block_tx_count"`
	Miner          string          `json:"miner"`
	Size           int             `json:"block_size"`
	Nonce          string          `json:"nonce"`
	BaseFee        decimal.Decimal `json:"base_fee"`
	BurntFees      decimal.Decimal `json:"burnt_fees"`
	GasLimit       uint64          `json:"gas_limit"`
	GasUsed        uint64          `json:"gas_used"`
	UnclesCount    int             `json:"uncles_count"`
}

type Tx struct {
	TxHash               string          `json:"tx_hash"`
	TxIndex              int             `json:"tx_index"`
	From                 string          `json:"from_addr"`
	To                   string          `json:"to_addr"`
	Nonce                uint64          `json:"nonce"`
	GasLimit             uint64          `json:"gas_limit"`
	GasPrice             decimal.Decimal `json:"gas_price"`
	GasUsed              uint64          `json:"gas_used"`
	BaseFee              decimal.Decimal `json:"base_fee"`
	BurntFees            decimal.Decimal `json:"burnt_fees"`
	MaxFeePerGas         decimal.Decimal `json:"max_fee_per_gas"`
	MaxPriorityFeePerGas decimal.Decimal `json:"max_priority_fee_per_gas"`
	Value                decimal.Decimal `json:"value"`
	Input                string          `json:"input"`
	ExecStatus           uint64          `json:"exec_status"`
	IsCallContract       bool            `json:"is_call_contract"`
	IsCreateContract     bool            `json:"is_create_contract"`
}

type EventLog struct {
	ContractAddr string `json:"contract_addr"`
	TopicCount   int    `json:"topic_count"`
	Topic0       string `json:"topic0"`
	Topic1       string `json:"topic1"`
	Topic2       string `json:"topic2"`
	Topic3       string `json:"topic3"`
	Data         string `json:"data"`
	Index        int    `json:"index"`
}

type EventErc20Transfer struct {
	ContractAddr string          `json:"contract_addr"`
	From         string          `json:"from_addr"`
	To           string          `json:"to_addr"`
	Amount       decimal.Decimal `json:"amount"`
	Index        int             `json:"index"`
}

type EventErc721Transfer struct {
	ContractAddr string `json:"contract_addr"`
	From         string `json:"from"`
	To           string `json:"to"`
	TokenId      string `json:"token_id"`
	Index        int    `json:"index"`
}

type EventErc1155Transfer struct {
	ContractAddr string          `json:"contract_addr"`
	Operator     string          `json:"operator"`
	From         string          `json:"from"`
	To           string          `json:"to"`
	TokenId      string          `json:"token_id"`
	Amount       decimal.Decimal `json:"amount"`
	Index        int             `json:"index"`
	IndexInBatch int             `json:"index_in_batch"`
}

type TokenErc721 struct {
	ContractAddr  string `json:"contract_addr"`
	TokenId       string `json:"token_id"`
	OwnerAddr     string `json:"owner_addr"`
	TokenUri      string `json:"tokenUri"`
	TokenMetaData []byte `json:"token_meta_data"`
}

type Contract struct {
	ContractAddr string `json:"contract_addr"`
	CreatorAddr  string `json:"creator_addr"`
	ExecStatus   uint64 `json:"exec_status"`
}

type ContractErc20 struct {
	ContractAddr string          `json:"contract_addr"`
	CreatorAddr  string          `json:"creator_addr"`
	Name         []byte          `json:"name"`
	Symbol       []byte          `json:"symbol"`
	Decimals     int             `json:"decimals"`
	TotalSupply  decimal.Decimal `json:"total_supply"`
}

type ContractErc721 struct {
	ContractAddr string `json:"contract_addr"`
	CreatorAddr  string `json:"creator_addr"`
	Name         []byte `json:"name"`
	Symbol       []byte `json:"symbol"`
}

type BalanceNative struct {
	Addr    string          `json:"addr"`
	Balance decimal.Decimal `json:"balance"`
}

type BalanceErc20 struct {
	Addr         string          `json:"addr"`
	ContractAddr string          `json:"contract_addr"`
	Balance      decimal.Decimal `json:"balance"`
}

type BalanceErc1155 struct {
	Addr         string          `json:"addr"`
	ContractAddr string          `json:"contract_addr"`
	TokenId      string          `json:"token_id"`
	Balance      decimal.Decimal `json:"balance"`
}
