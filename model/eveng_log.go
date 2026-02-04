package model

type EventLog struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:height"`
	TxHash       string `gorm:"index;uniqueIndex:txhash_logindex;type:varchar(255);comment:tx_hash"`
	IndexInTx    uint   `gorm:"type:uint;comment:index_in_tx"`
	IndexInBlock uint   `gorm:"uniqueIndex:txhash_logindex;type:uint;comment:index_in_block"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`

	TopicCount uint   `gorm:"type:int;comment:topic_count"`
	Topic0     string `gorm:"index;type:varchar(255);comment:topic0"`
	Topic1     string `gorm:"index;type:varchar(255);comment:topic1"`
	Topic2     string `gorm:"index;type:varchar(255);comment:topic2"`
	Topic3     string `gorm:"index;type:varchar(255);comment:topic3"`

	Data []byte `gorm:"type:blob;comment:data"`
}

func (EventLog) TableName() string {
	return "event_log"
}
