package model

type Block struct {
	Id         uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height     uint64 `gorm:"index:uniq_height,unique;type:bigint unsigned;comment:height"`
	Hash       string `gorm:"index:uniq_hash,unique;type:varchar(255);comment:hash"`
	ParentHash string `gorm:"index;type:varchar(255);comment:parent_hash"`
	IrreversibleHeight uint64 `gorm:"type:bigint unsigned;index;comment:irreversible_height"`
	IrreversibleHash   string `gorm:"type:varchar(255);index;comment:irreversible_hash"`
	Timestamp  int64  `gorm:"index;type:bigint;comment:timestamp"`
	TxCount    int    `gorm:"type:int;comment:tx_count"`
	Miner      string `gorm:"index;type:varchar(255);comment:miner"`
	Size       int    `gorm:"type:int;comment:size"`

	Nonce     string `gorm:"type:varchar(255);comment:nonce"`
	BaseFee   string `gorm:"type:varchar(255);comment:base_fee"`
	BurntFees string `gorm:"type:varchar(255);comment:burnt_fees"`

	GasLimit uint64 `gorm:"type:bigint unsigned;comment:gas_limit"`
	GasUsed  uint64 `gorm:"type:bigint unsigned;comment:gas_used"`

	UnclesCount int `gorm:"type:int;comment:uncles_count"`

	Difficulty      string `gorm:"type:varchar(255);comment:difficulty"`
	TotalDifficulty string `gorm:"type:varchar(255);comment:total_difficulty"`

	StateRoot       string `gorm:"index;type:varchar(255);comment:state_root"`
	TransactionRoot string `gorm:"index;type:varchar(255);comment:transaction_root"`
	ReceiptRoot     string `gorm:"index;type:varchar(255);comment:receipt_root"`

	ExtraData string `gorm:"type:text;comment:extra_data"`

	// Complete 为 true 表示该高度下交易、事件等子数据已写入完毕
	Complete bool `gorm:"type:tinyint;default:0;index;comment:block_store_complete"`
}

func (Block) TableName() string {
	return "block"
}
