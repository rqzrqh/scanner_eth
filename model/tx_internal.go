package model

type TxInternal struct {
	Id           uint64 `gorm:"primaryKey;autoIncrement;comment:auto_inc_id"`
	BlockId      uint64 `gorm:"index;uniqueIndex:uniq_blockid_txhash_index;type:bigint unsigned;not null;comment:block_id"`
	Height       uint64 `gorm:"index:idx_height;type:bigint unsigned;comment:height"`
	TxHash       string `gorm:"index:idx_txhash;uniqueIndex:uniq_blockid_txhash_index;type:varchar(255);not null;comment:tx_hash"`
	Index        int    `gorm:"uniqueIndex:uniq_blockid_txhash_index;type:int;not null;comment:index"`
	From         string `gorm:"index:idx_from;type:varchar(255);comment:from"`
	To           string `gorm:"index:idx_to;type:varchar(255);comment:to"`
	OpCode       string `gorm:"index:idx_opcode;type:varchar(255);comment:op_code"`
	Value        string `gorm:"type:varchar(255);comment:value"`
	Success      bool   `gorm:"type:boolean;comment:success"`
	Depth        int    `gorm:"type:int;comment:call_depth"`
	Gas          uint64 `gorm:"type:bigint unsigned;comment:gas"`
	GasUsed      uint64 `gorm:"type:bigint unsigned;comment:gas_used"`
	Input        string `gorm:"type:longtext;comment:input"`
	Output       string `gorm:"type:longtext;comment:output"`
	TraceAddress string `gorm:"type:varchar(255);comment:trace_address"`
}

func (TxInternal) TableName() string {
	return "tx_internal"
}
