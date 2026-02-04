package model

// trigger by erc20 transfer event, so we don't know the creator info
type ContractErc20 struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	ContractAddr string `gorm:"index:uniq_contract_addr,unique;type:varchar(255);comment:contract_addr"`
	Name         string `gorm:"type:text;comment:name"`
	Symbol       string `gorm:"type:text;comment:symbol"`
	Decimals     int    `gorm:"type:int;comment:decimals"`
	TotalSupply  string `gorm:"type:varchar(255);comment:total_supply"`
}

func (ContractErc20) TableName() string {
	return "contract_erc20"
}
