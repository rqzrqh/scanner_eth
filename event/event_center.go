package event

import "sync_eth/types"

type ApplyData struct {
	FullBlock *types.FullBlock
}

type RevertData struct {
	Height uint64
}

type EventCenter struct {
	eventChannel chan *types.ChainEvent
}

func NewEventCenter(eventChannel chan *types.ChainEvent) *EventCenter {
	return &EventCenter{
		eventChannel: eventChannel,
	}
}

func (ec *EventCenter) Apply(fullblock *types.FullBlock) {
	ec.eventChannel <- &types.ChainEvent{
		Type: types.Apply,
		Data: &ApplyData{
			FullBlock: fullblock,
		},
	}
}

func (ec *EventCenter) Revert(height uint64) {
	ec.eventChannel <- &types.ChainEvent{
		Type: types.Revert,
		Data: &RevertData{
			Height: height,
		},
	}
}

func (ec *EventCenter) Run() {
	go func() {
		for {
			select {
			// must async publish
			case ev := <-ec.eventChannel:
				if ev.Type == types.Apply {

				} else if ev.Type == types.Revert {

				}
			}
		}
	}()
}
