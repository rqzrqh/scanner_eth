package model

type Contract struct {
	Id           uint64 `gorm:"primaryKey:autoIncrement;comment:auto_inc_id"`
	Height       uint64 `gorm:"index;type:bigint unsigned;comment:block_height"`
	TxHash       string `gorm:"index;type:varchar(255);comment:tx_hash"`
	ContractAddr string `gorm:"index;type:varchar(255);comment:contract_addr"`
	CreatorAddr  string `gorm:"index;type:varchar(255);comment:creator_addr"`
	ExecStatus   uint64 `gorm:"type:bigint unsigned;comment:exec_status"`
}

func (Contract) TableName() string {
	return "contract"
}
