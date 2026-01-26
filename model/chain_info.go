package model

type ChainInfo struct {
	ID               uint64 `gorm:"primaryKey;autoIncrement:true"`
	ChainId          uint64 `gorm:"index;type:bigint unsigned;comment:chain_id"`
	GenesisBlockHash string `gorm:"index:uniq_blockhash,unique;type:varchar(255);comment:genesis_block_hash"`
}

func (ChainInfo) TableName() string {
	return "chain_info"
}
