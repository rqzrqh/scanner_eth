package types

import "scanner_eth/data"

type StoreOperationType byte

const (
	StoreApply StoreOperationType = iota
	StoreRollback
)

type StoreOperation struct {
	Type      StoreOperationType
	Height    uint64
	FullBlock *data.FullBlock
}
