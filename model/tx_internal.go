package model

import "github.com/shopspring/decimal"

type TxInternal struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string          `gorm:"index;type:varchar(255);comment:tx_hash"`
	Index        int             `gorm:"type:int;comment:index"`
	From         string          `gorm:"index;type:varchar(255);comment:from_addr"`
	To           string          `gorm:"index;type:varchar(255);comment:to_addr"`
	OpCode       string          `gorm:"index;type:varchar(255);comment:op_code"`
	Value        decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:value"`
	Success      bool            `gorm:"type:int;comment:success"`
	Depth        int             `gorm:"type:int;comment:call_depth"`
	Gas          uint64          `gorm:"type:bigint unsigned;comment:gas"`
	GasUsed      uint64          `gorm:"type:bigint unsigned;comment:gas_used"`
	Input        string          `gorm:"type:longtext;comment:input"`
	Output       string          `gorm:"type:longtext;comment:output"`
	TraceAddress string          `gorm:"type:varchar(255);comment:success"`
}

func (TxInternal) TableName() string {
	return "tx_internal"
}
