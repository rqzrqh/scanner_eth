package types

type PublishOperationType byte

const (
	PublishApply PublishOperationType = iota
	PublishRollback
)

type PublishOperation struct {
	Type PublishOperationType
	Data interface{}
}

type PublishApplyData struct {
	FullBlock *FullBlock
}

type PublishRollbackData struct {
	Height uint64
}
