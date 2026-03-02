package model

type EventErc20Transfer struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:height"`
	TxHash       string `gorm:"index;uniqueIndex:txhash_logindex;type:varchar(255);comment:tx_hash"`
	IndexInTx    uint   `gorm:"uniqueIndex:txhash_logindex;type:uint;comment:index_in_tx"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`
	From         string `gorm:"index;type:varchar(255);comment:from"`
	To           string `gorm:"index;type:varchar(255);comment:to"`
	Amount       string `gorm:"type:varchar(255);comment:amount"`
}

func (EventErc20Transfer) TableName() string {
	return "event_erc20_transfer"
}
