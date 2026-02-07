package model

type ChainBinlog struct {
	Id         uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	MessageId  uint64 `gorm:"type:bigint unsigned;comment:message_id"`
	ActionType int    `gorm:"type:int;comment:action_type"` // 0: apply block, 1: revert block
	Height     uint64 `gorm:"type:bigint unsigned;comment:height"`
	BinlogData []byte `gorm:"type:blob;comment:binlog_data"`
}

func (ChainBinlog) TableName() string {
	return "chain_binlog"
}
