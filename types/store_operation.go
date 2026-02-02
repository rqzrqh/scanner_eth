package types

type StoreOperationType byte

const (
	StoreApply StoreOperationType = iota
	StoreRollback
)

type StoreOperation struct {
	Type      StoreOperationType
	Height    uint64
	FullBlock *FullBlock
}
