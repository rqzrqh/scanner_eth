package model

import "github.com/shopspring/decimal"

type ContractErc721 struct {
	Id                uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height            uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash            string          `gorm:"index;type:varchar(255);comment:tx_hash"`
	Addr              string          `gorm:"index:uniq_addr,unique;type:varchar(255);comment:addr"`
	CreatorAddr       string          `gorm:"index;type:varchar(255);comment:creator_addr"`
	Name              []byte          `gorm:"type:blob;comment:name"`
	Symbol            []byte          `gorm:"type:blob;comment:symbol"`
	TotalSupply       decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:total_supply"`
	TotalSupplyOrigin string          `gorm:"type:varchar(255);comment:total_supply_origin"`
}

func (ContractErc721) TableName() string {
	return "contract_erc721"
}
