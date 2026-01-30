package event

import "sync_eth/types"

type EventCenter struct {
	publishOperationChannel <-chan *types.PublishOperation
}

func NewEventCenter(publishOperationChannel <-chan *types.PublishOperation) *EventCenter {
	return &EventCenter{
		publishOperationChannel: publishOperationChannel,
	}
}

func (ec *EventCenter) Run() {
	go func() {
		for {
			for op := range ec.publishOperationChannel {
				// must async publish

				if op.Type == types.PublishApply {

				} else if op.Type == types.PublishRollback {

				}
			}
		}
	}()
}
