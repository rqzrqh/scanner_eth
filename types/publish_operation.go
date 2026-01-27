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
	Events []EventItem
}

type PublishRollbackData struct {
	Height uint64
}
