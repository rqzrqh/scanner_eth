package types

type ChainEventType byte

const (
	Apply ChainEventType = iota
	Revert
)

type ChainEvent struct {
	Type ChainEventType
	Data interface{}
}

type ApplyData struct {
	FullBlock   *FullBlock
	ForkVersion uint64
	EventID     uint64
}

type RevertData struct {
	Height      uint64
	ForkVersion uint64
	EventID     uint64
}
