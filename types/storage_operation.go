package types

type StoreOperationType byte

const (
	StoreApply StoreOperationType = iota
	StoreRollback
	StorePublishSuccess
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

type StorePublishSuccessData struct {
	Id     uint64
	Height uint64
}
