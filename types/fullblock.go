package types

import (
	"sync_eth/protocol"
)

type FullBlock struct {
	Block                    *Block
	TxList                   []*Tx
	TxInternalList           []*TxInternal
	EventLogList             []*EventLog
	EventErc20TransferList   []*EventErc20Transfer
	EventErc721TransferList  []*EventErc721Transfer
	EventErc1155TransferList []*EventErc1155Transfer
	TokenErc721List          []*TokenErc721
	ContractList             []*Contract
	ContractErc20List        []*ContractErc20
	ContractErc721List       []*ContractErc721
	BalanceNativeList        []*BalanceNative
	BalanceErc20List         []*BalanceErc20
	BalanceErc1155List       []*BalanceErc1155
}

type Block protocol.Block

type Tx protocol.Tx

type TxInternal protocol.TxInternal

type EventLog protocol.EventLog

type EventErc20Transfer protocol.EventErc20Transfer

type EventErc721Transfer protocol.EventErc721Transfer

type EventErc1155Transfer protocol.EventErc1155Transfer

type TokenErc721 protocol.TokenErc721

type Contract protocol.Contract

type ContractErc20 protocol.ContractErc20

type ContractErc721 protocol.ContractErc721

type BalanceNative protocol.BalanceNative

type BalanceErc20 protocol.BalanceErc20

type BalanceErc1155 protocol.BalanceErc1155
