package model

import "github.com/shopspring/decimal"

type BalanceErc20 struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Addr         string          `gorm:"index;uniqueIndex:addr_contract;type:varchar(255);comment:addr"`
	ContractAddr string          `gorm:"index;uniqueIndex:addr_contract;type:varchar(255);comment:contract_addr"`
	Balance      decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:balance"`
	UpdateHeight uint64          `gorm:"index;type:bigint unsigned;comment:update_height"`
}

func (BalanceErc20) TableName() string {
	return "balance_erc20"
}
