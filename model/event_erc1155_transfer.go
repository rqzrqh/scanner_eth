package model

import "github.com/shopspring/decimal"

type EventErc1155Transfer struct {
	Id           uint64          `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64          `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string          `gorm:"index;uniqueIndex:txhash_logindex_batchindex;type:varchar(255);comment:tx_hash"`
	IndexInBlock uint            `gorm:"uniqueIndex:txhash_logindex_batchindex;type:uint;comment:index_in_block"`
	IndexInBatch int             `gorm:"uniqueIndex:txhash_logindex_batchindex;type:int;comment:index_in_batch"`
	ContractAddr string          `gorm:"index;type:varchar(255);comment:contract_addr"`
	Operator     string          `gorm:"index;type:varchar(255);comment:operator"`
	From         string          `gorm:"index;type:varchar(255);comment:from"`
	To           string          `gorm:"index;type:varchar(255);comment:to"`
	TokenId      string          `gorm:"index;type:varchar(255);comment:token_id"`
	Amount       decimal.Decimal `gorm:"type:DECIMAL(65,0);comment:amount"`
}

func (EventErc1155Transfer) TableName() string {
	return "event_erc1155_transfer"
}
