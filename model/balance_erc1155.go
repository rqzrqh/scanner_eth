package model

import "github.com/shopspring/decimal"

type BalanceErc1155 struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Addr         string          `gorm:"index;type:varchar(255);comment:addr"`
	ContractAddr string          `gorm:"index;type:varchar(255);comment:contract_addr"`
	TokenId      string          `gorm:"index;type:varchar(255);comment:token_id"`
	Balance      decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:balance"`
	UpdateHeight uint64          `gorm:"index;type:bigint unsigned;comment:update_height"`
}

func (BalanceErc1155) TableName() string {
	return "balance_erc1155"
}
