package model

type ChainBinlog struct {
	Id         uint64 `gorm:"primaryKey;autoIncrement:true"`
	ActionType int    `gorm:"type:int;comment:action_type"` // 0: apply block, 1: revert block
	Height     uint64 `gorm:"type:bigint unsigned;comment:block_height"`
	FullBlock  string `gorm:"type:text;comment:full_block"`
}

func (ChainBinlog) TableName() string {
	return "chain_binlog"
}
