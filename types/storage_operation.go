package types

type StoreOperationType byte

const (
	StoreApply StoreOperationType = iota
	StoreRollback
)

type StoreOperation struct {
	Type StoreOperationType
	Data interface{}
}

type StoreApplyData struct {
	FullBlock *FullBlock
}

type StoreRollbackData struct {
	Height uint64
}
