package model

type ScannerInfo struct {
	Id                 uint64 `gorm:"primaryKey;autoIncrement:true"`
	ChainId            int64  `gorm:"index:uniq_chainid,unique;type:bigint;comment:chain_id"`
	GenesisBlockHash   string `gorm:"index:uniq_blockhash,unique;type:varchar(255);comment:genesis_block_hash"`
	MessageId          uint64 `gorm:"type:bigint unsigned;comment:message_id"`
	PublishedMessageId uint64 `gorm:"type:bigint unsigned;comment:published_message_id"`
}

func (ScannerInfo) TableName() string {
	return "scanner_info"
}
