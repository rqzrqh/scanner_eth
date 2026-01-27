package model

type EventErc721Transfer struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`
	Sender       string `gorm:"index;type:varchar(255);comment:sender"`
	Receiver     string `gorm:"index;type:varchar(255);comment:receiver"`
	TokenId      string `gorm:"index;type:varchar(255);comment:token_id"`
	Index        int    `gorm:"type:int;comment:index"`
}

func (EventErc721Transfer) TableName() string {
	return "event_erc721_transfer"
}
