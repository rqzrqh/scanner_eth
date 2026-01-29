package model

type ContractErc721 struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string `gorm:"index:uniq_contract_addr,unique;type:varchar(255);comment:contract_addr"`
	CreatorAddr  string `gorm:"index;type:varchar(255);comment:creator_addr"`
	Name         []byte `gorm:"type:blob;comment:name"`
	Symbol       []byte `gorm:"type:blob;comment:symbol"`
}

func (ContractErc721) TableName() string {
	return "contract_erc721"
}
