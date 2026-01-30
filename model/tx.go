package model

import "github.com/shopspring/decimal"

type Tx struct {
	Id        uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height    uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash    string          `gorm:"index:uniq_txhash,unique;type:varchar(255);comment:tx_hash"`
	TxIndex   int             `gorm:"type:int;comment:tx_index"`
	TxType    int             `gorm:"type:tinyint;comment:tx_type"`
	From      string          `gorm:"index;type:varchar(255);comment:from_addr"`
	To        string          `gorm:"index;type:varchar(255);comment:to_addr"`
	Nonce     uint64          `gorm:"type:bigint unsigned;comment:nonce"`
	GasLimit  uint64          `gorm:"type:bigint unsigned;comment:gas_limit"`
	GasPrice  decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:gas_price"`
	GasUsed   uint64          `gorm:"type:bigint unsigned;comment:gas_used"`
	BaseFee   decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:base_fee"`
	BurntFees decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:burnt_fees"`

	MaxFeePerGas         decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:max_fee_per_gas"`
	MaxPriorityFeePerGas decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:max_priority_fee_per_gas"`
	Value                decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:value"`
	Input                string          `gorm:"type:longtext;comment:input"`
	ExecStatus           uint64          `gorm:"type:bigint unsigned;comment:exec_status"`

	IsCallContract   bool `gorm:"type:tinyint;comment:is_call_contract"`
	IsCreateContract bool `gorm:"type:tinyint;comment:is_create_contract"`
}

func (Tx) TableName() string {
	return "tx"
}
