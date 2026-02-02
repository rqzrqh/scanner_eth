package types

type PublishOperationType byte

const (
	PublishApply PublishOperationType = iota
	PublishRollback
)

type PublishOperation struct {
	Type              PublishOperationType
	Id                uint64
	Height            uint64
	ProtocolFullBlock []byte
}
