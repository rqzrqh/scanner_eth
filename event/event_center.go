package event

import "sync_eth/types"

type ApplyData struct {
	FullBlock *types.FullBlock
}

type RevertData struct {
	Height uint64
}

type EventCenter struct {
	publicshOperationChannel <-chan *types.PublishOperation
}

func NewEventCenter(publicshOperationChannel <-chan *types.PublishOperation) *EventCenter {
	return &EventCenter{
		publicshOperationChannel: publicshOperationChannel,
	}
}

func (ec *EventCenter) Run() {
	go func() {
		for {
			select {
			// must async publish
			case ev := <-ec.publicshOperationChannel:
				if ev.Type == types.PublishApply {

				} else if ev.Type == types.PublishRollback {

				}
			}
		}
	}()
}
