package model

import "github.com/shopspring/decimal"

type EventErc20Transfer struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string          `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string          `gorm:"index;type:varchar(255);comment:contract_addr"`
	From         string          `gorm:"index;type:varchar(255);comment:from_addr"`
	To           string          `gorm:"index;type:varchar(255);comment:to_addr"`
	Amount       decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:amount"`
	AmountOrigin string          `gorm:"type:varchar(255);comment:amuont_origin"`
	Index        int             `gorm:"type:int;comment:index"`
}

func (EventErc20Transfer) TableName() string {
	return "event_erc20_transfer"
}
