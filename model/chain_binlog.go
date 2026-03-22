package model

type ChainBinlog struct {
	Id         uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	MessageId  uint64 `gorm:"type:bigint unsigned;comment:message_id"`
	Height     uint64 `gorm:"type:bigint unsigned;comment:height"`
	Hash       string `gorm:"type:varchar(255);comment:hash"`
	ParentHash string `gorm:"type:varchar(255);comment:parent_hash"`
	Data       []byte `gorm:"type:blob;comment:data"`
}

func (ChainBinlog) TableName() string {
	return "chain_binlog"
}
