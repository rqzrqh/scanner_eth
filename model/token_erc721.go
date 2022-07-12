package model

type TokenErc721 struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`
	TokenId      string `gorm:"index;type:varchar(255);comment:token_id"`

	OwnerAddr string `gorm:"index;type:varchar(255);comment:owner_addr"`
	TokenUri  string `gorm:"type:text;comment:tokenUri"`

	TokenMetaData []byte `gorm:"type:blob;comment:token_meta_data"`
	UpdateHeight  uint64 `gorm:"type:blob;comment:symbol"`
}

func (TokenErc721) TableName() string {
	return "contract_erc721"
}
