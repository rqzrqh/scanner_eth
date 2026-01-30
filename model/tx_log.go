package model

type EventLog struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:height"`
	TxHash       string `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`

	TopicCount int    `gorm:"type:int;comment:topic_count"`
	Topic0     string `gorm:"index;type:varchar(255);comment:topic0"`
	Topic1     string `gorm:"index;type:varchar(255);comment:topic1"`
	Topic2     string `gorm:"index;type:varchar(255);comment:topic2"`
	Topic3     string `gorm:"index;type:varchar(255);comment:topic3"`

	Data  string `gorm:"type:longtext;comment:data"`
	Index int    `gorm:"type:int;comment:index"`
}

func (EventLog) TableName() string {
	return "event_log"
}
