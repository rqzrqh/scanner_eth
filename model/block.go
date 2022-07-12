package model

import "github.com/shopspring/decimal"

type Block struct {
	Id             uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height         uint64 `gorm:"index;type:bigint unsigned;comment:block_height"`
	BlockHash      string `gorm:"index:uniq_blockhash,unique;type:varchar(255);comment:block_hash"`
	ParentHash     string `gorm:"index;type:varchar(255);comment:parent_block_hash"`
	BlockTimestamp int64  `gorm:"index;type:bigint;comment:block_timestamp"`
	TxsCount       int    `gorm:"type:int;comment:block_tx_count"`
	Miner          string `gorm:"index;type:varchar(255);comment:miner"`
	Size           int    `gorm:"type:int;comment:block_size"`

	Nonce     string          `gorm:"type:varchar(255);comment:nonce"`
	BaseFee   decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:base_fee"`
	BurntFees decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:burnt_fees"`

	GasLimit uint64 `gorm:"type:bigint unsigned;comment:gas_limit"`
	GasUsed  uint64 `gorm:"type:bigint unsigned;comment:gas_used"`

	UnclesCount int `gorm:"type:int;comment:uncles_count"`

	Difficulty      decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:difficulty"`
	TotalDifficulty decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:total_difficulty"`

	StateRoot       string `gorm:"index;type:varchar(255);comment:state_root"`
	TransactionRoot string `gorm:"index;type:varchar(255);comment:transaction_root"`
	ReceiptRoot     string `gorm:"index;type:varchar(255);comment:receipt_root"`

	ExtraData string `gorm:"type:text;comment:extra_data"`
}

func (Block) TableName() string {
	return "block"
}
