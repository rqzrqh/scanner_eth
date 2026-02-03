package model

type TokenErc721 struct {
	Id            uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	ContractAddr  string `gorm:"index;uniqueIndex:contractaddr_tokenid;type:varchar(255);comment:contract_addr"`
	TokenId       string `gorm:"uniqueIndex:contractaddr_tokenid;type:varchar(255);comment:token_id"`
	OwnerAddr     string `gorm:"index;type:varchar(255);comment:owner_addr"`
	TokenUri      string `gorm:"type:text;comment:tokenUri"`
	TokenMetaData []byte `gorm:"type:blob;comment:token_meta_data"`
	UpdateHeight  uint64 `gorm:"type:bigint unsigned;comment:update_height"`
}

func (TokenErc721) TableName() string {
	return "token_erc721"
}
