package model

// ScannerMessage stores one chain message per finalized block; external MessageId uses this table's auto-increment Id.
type ScannerMessage struct {
	Id         uint64 `gorm:"primaryKey;autoIncrement;comment:auto_inc_id;index:idx_scanner_msg_pending,priority:2"`
	Height     uint64 `gorm:"not null;type:bigint unsigned;comment:height"`
	Hash       string `gorm:"not null;uniqueIndex:uniq_scanner_msg_hash;type:varchar(255);comment:hash"`
	ParentHash string `gorm:"type:varchar(255);comment:parent_hash"`
	// Composite (pushed, id) matches Redis push query WHERE pushed=0 ORDER BY id LIMIT N; avoids filesort on a single-column pushed index.
	Pushed bool `gorm:"type:tinyint(1);default:0;index:idx_scanner_msg_pending,priority:1;comment:redis_pushed"`
}

func (ScannerMessage) TableName() string {
	return "scanner_message"
}
