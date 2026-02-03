package model

// trigger by erc721 transfer event, so we don't know the creator info
type ContractErc721 struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	ContractAddr string `gorm:"index:uniq_contract_addr,unique;type:varchar(255);comment:contract_addr"`
	Name         []byte `gorm:"type:blob;comment:name"`
	Symbol       []byte `gorm:"type:blob;comment:symbol"`
}

func (ContractErc721) TableName() string {
	return "contract_erc721"
}
