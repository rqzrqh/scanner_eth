package types

import (
	"sync_eth/protocol"
)

type FullBlock struct {
	Block                    *protocol.Block
	TxList                   []*protocol.Tx
	TxInternalList           []*protocol.TxInternal
	EventLogList             []*protocol.EventLog
	EventErc20TransferList   []*protocol.EventErc20Transfer
	EventErc721TransferList  []*protocol.EventErc721Transfer
	EventErc1155TransferList []*protocol.EventErc1155Transfer

	TokenErc721List []*protocol.TokenErc721

	ContractList       []*protocol.Contract
	ContractErc20List  []*protocol.ContractErc20
	ContractErc721List []*protocol.ContractErc721

	BalanceNativeList  []*protocol.BalanceNative
	BalanceErc20List   []*protocol.BalanceErc20
	BalanceErc1155List []*protocol.BalanceErc1155
}
