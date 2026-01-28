package model

import "github.com/shopspring/decimal"

type BalanceNative struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Addr         string          `gorm:"index:uniq_addr,unique;type:varchar(255);comment:addr"`
	Balance      decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:balance"`
	UpdateHeight uint64          `gorm:"index;type:bigint unsigned;comment:update_height"`
}

func (BalanceNative) TableName() string {
	return "balance_native"
}
