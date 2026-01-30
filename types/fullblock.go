package types

import (
	"sync_eth/model"
)

type FullBlock struct {
	Block                    *model.Block
	TxList                   []*model.Tx
	TxInternalList           []*model.TxInternal
	EventLogList             []*model.EventLog
	EventErc20TransferList   []*model.EventErc20Transfer
	EventErc721TransferList  []*model.EventErc721Transfer
	EventErc1155TransferList []*model.EventErc1155Transfer

	TokenErc721List []*model.TokenErc721

	ContractList       []*model.Contract
	ContractErc20List  []*model.ContractErc20
	ContractErc721List []*model.ContractErc721

	BalanceNativeList  []*model.BalanceNative
	BalanceErc20List   []*model.BalanceErc20
	BalanceErc1155List []*model.BalanceErc1155
}
