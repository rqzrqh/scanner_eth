package model

import "github.com/shopspring/decimal"

type EventErc1155Transfer struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string          `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string          `gorm:"index;type:varchar(255);comment:contract_addr"`
	Operator     string          `gorm:"index;type:varchar(255);comment:operator"`
	Sender       string          `gorm:"index;type:varchar(255);comment:sender"`
	Receiver     string          `gorm:"index;type:varchar(255);comment:receiver"`
	TokenId      string          `gorm:"index;type:varchar(255);comment:token_id"`
	Amount       decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:amount"`
	Index        int             `gorm:"type:int;comment:index"`
}

func (EventErc1155Transfer) TableName() string {
	return "event_erc1155_transfer"
}
