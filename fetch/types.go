package fetch

import (
	"scanner_eth/model"
	"scanner_eth/protocol"
)

// EventBlockData carries converted block payloads for storage and publish flow.
type EventBlockData struct {
	StorageFullBlock  *StorageFullBlock
	ProtocolFullBlock *protocol.FullBlock
}

type StorageFullBlock struct {
	Block                    model.Block
	TxList                   []model.Tx
	TxInternalList           []model.TxInternal
	EventLogList             []model.EventLog
	EventErc20TransferList   []model.EventErc20Transfer
	EventErc721TransferList  []model.EventErc721Transfer
	EventErc1155TransferList []model.EventErc1155Transfer

	ContractList       []model.Contract
	ContractErc20List  []model.ContractErc20
	ContractErc721List []model.ContractErc721

	BalanceNativeList  []model.BalanceNative
	BalanceErc20List   []model.BalanceErc20
	BalanceErc1155List []model.BalanceErc1155
	TokenErc721List    []model.TokenErc721
}

type RemoteHeader struct {
	Hash       string
	ParentHash string
	Number     string
	Difficulty string
}

type RemoteChainUpdate struct {
	NodeId    int
	Height    uint64
	BlockHash string
	Weight    uint64
	Header    *RemoteHeader
}
