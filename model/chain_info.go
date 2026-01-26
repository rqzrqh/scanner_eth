package model

type ChainInfo struct {
	ID        uint64 `gorm:"primaryKey;autoIncrement:true"`
	ChainId   uint64 `gorm:"index;type:bigint unsigned;comment:chain_id"`
	BlockHash string `gorm:"index:uniq_blockhash,unique;type:varchar(255);comment:block_hash"`
}

func (ChainInfo) TableName() string {
	return "chain_info"
}
