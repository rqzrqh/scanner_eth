package model

type ScannerInfo struct {
	Id               uint64 `gorm:"primaryKey;autoIncrement:true"`
	ChainId          uint64 `gorm:"type:bigint unsigned;comment:chain_id"`
	GenesisBlockHash string `gorm:"index:uniq_blockhash,unique;type:varchar(255);comment:genesis_block_hash"`
}

func (ScannerInfo) TableName() string {
	return "scanner_info"
}
