package model

type ChainBinlog struct {
	Id         uint64 `gorm:"primaryKey;autoIncrement:true"`
	MessageId  uint64 `gorm:"type:bigint unsigned;comment:message_id"`
	ActionType int    `gorm:"type:int;comment:action_type"` // 0: apply block, 1: revert block
	Height     uint64 `gorm:"type:bigint unsigned;comment:block_height"`
	BinlogData []byte `gorm:"type:blob;comment:binlog_data"`
}

func (ChainBinlog) TableName() string {
	return "chain_binlog"
}
