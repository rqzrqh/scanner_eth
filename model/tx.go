package model

type Tx struct {
	Id        uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	BlockId   uint64 `gorm:"index;type:bigint unsigned;not null;comment:block_id"`
	Height    uint64 `gorm:"index;type:bigint unsigned;comment:height"`
	TxHash    string `gorm:"index:uniq_txhash,unique;type:varchar(255);comment:tx_hash"`
	TxIndex   int    `gorm:"type:int;comment:tx_index"`
	TxType    int    `gorm:"type:tinyint;comment:tx_type"`
	From      string `gorm:"index;type:varchar(255);comment:from"`
	To        string `gorm:"index;type:varchar(255);comment:to"`
	Nonce     uint64 `gorm:"type:bigint unsigned;comment:nonce"`
	GasLimit  uint64 `gorm:"type:bigint unsigned;comment:gas_limit"`
	GasPrice  string `gorm:"type:varchar(255);comment:gas_price"`
	GasUsed   uint64 `gorm:"type:bigint unsigned;comment:gas_used"`
	BaseFee   string `gorm:"type:varchar(255);comment:base_fee"`
	BurntFees string `gorm:"type:varchar(255);comment:burnt_fees"`

	MaxFeePerGas         string `gorm:"type:varchar(255);comment:max_fee_per_gas"`
	MaxPriorityFeePerGas string `gorm:"type:varchar(255);comment:max_priority_fee_per_gas"`
	Value                string `gorm:"type:varchar(255);comment:value"`
	Input                string `gorm:"type:longtext;comment:input"`
	ExecStatus           uint64 `gorm:"type:bigint unsigned;comment:exec_status"`
	IsCallContract       bool   `gorm:"type:tinyint;comment:is_call_contract"`
	IsCreateContract     bool   `gorm:"type:tinyint;comment:is_create_contract"`
}

func (Tx) TableName() string {
	return "tx"
}
