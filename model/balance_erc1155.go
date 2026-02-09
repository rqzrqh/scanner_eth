package model

type BalanceErc1155 struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Addr         string `gorm:"index;type:varchar(255);comment:addr"`
	ContractAddr string `gorm:"index;uniqueIndex:contractaddr_tokenid;type:varchar(255);comment:contract_addr"`
	TokenId      string `gorm:"uniqueIndex:contractaddr_tokenid;type:varchar(255);comment:token_id"`
	Balance      string `gorm:"type:varchar(255);comment:balance"`
	UpdateHeight uint64 `gorm:"index;type:bigint unsigned;comment:update_height"`
}

func (BalanceErc1155) TableName() string {
	return "balance_erc1155"
}
