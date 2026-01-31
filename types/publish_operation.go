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
	Id                uint64
	Height            uint64
	ProtocolFullBlock []byte
}

type PublishRollbackData struct {
	Height uint64
}
