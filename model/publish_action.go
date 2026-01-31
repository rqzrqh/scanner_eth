package model

type PublishAction struct {
	Id         uint64 `gorm:"primaryKey;autoIncrement:true"`
	ActionType int    `gorm:"type:int;comment:action_type"` // 0: publish block, 1: revert block
	Data       string `gorm:"type:text;comment:action_data"`
	Height     uint64 `gorm:"type:bigint unsigned;comment:block_height"`
}

func (PublishAction) TableName() string {
	return "publish_action"
}
