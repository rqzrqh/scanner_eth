package model

// EventLog is insert-heavy: each secondary index slows INSERTs. We drop the redundant single-column tx_hash index
// (unique(tx_hash,index_in_block) already covers it) and topic1–3 indexes (keep topic0 for signature-style filters).
type EventLog struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	BlockId      uint64 `gorm:"index;type:bigint unsigned;not null;comment:block_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:height"`
	TxHash       string `gorm:"uniqueIndex:txhash_logindex;type:varchar(255);comment:tx_hash"`
	IndexInTx    uint   `gorm:"type:uint;comment:index_in_tx"`
	IndexInBlock uint   `gorm:"uniqueIndex:txhash_logindex;type:uint;comment:index_in_block"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`

	TopicCount uint   `gorm:"type:int;comment:topic_count"`
	Topic0     string `gorm:"index;type:varchar(255);comment:topic0"`
	Topic1     string `gorm:"type:varchar(255);comment:topic1"`
	Topic2     string `gorm:"type:varchar(255);comment:topic2"`
	Topic3     string `gorm:"type:varchar(255);comment:topic3"`

	Data []byte `gorm:"type:blob;comment:data"`
}

func (EventLog) TableName() string {
	return "event_log"
}
