package event

import (
	"context"
	"os"
	"sync_eth/types"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
)

type EventCenter struct {
	w                       *kafka.Writer
	publishOperationChannel <-chan *types.PublishOperation
	storeOperationChannel   chan<- *types.StoreOperation
}

func NewEventCenter(w *kafka.Writer, publishOperationChannel <-chan *types.PublishOperation, storeOperationChannel chan<- *types.StoreOperation) *EventCenter {
	return &EventCenter{
		w:                       w,
		publishOperationChannel: publishOperationChannel,
		storeOperationChannel:   storeOperationChannel,
	}
}

func (ec *EventCenter) Run() {
	go func() {
		for {
			for op := range ec.publishOperationChannel {
				id := uint64(0)
				height := uint64(0)

				// must async publish
				switch op.Type {
				case types.PublishApply:

					protocolFullBlock := op.Data.(*types.PublishApplyData).ProtocolFullBlock
					id = op.Data.(*types.PublishApplyData).Id
					height = op.Data.(*types.PublishApplyData).Height

					tryCount := 0
					for {
						tryCount++
						startTime := time.Now()

						if err := ec.w.WriteMessages(context.Background(),
							kafka.Message{
								Value: protocolFullBlock,
							},
						); err != nil {
							logrus.Errorf("failed to write messages. wait retry. height:%v err:%v tryCount:%v", height, err, tryCount)
							time.Sleep(3 * time.Second)
							continue
						}

						logrus.Infof("publish fullblock success. height:%v id:%v cost:%v", height, id, time.Since(startTime).String())
						break
					}

				case types.PublishRollback:

				default:
					logrus.Errorf("unknown publish operation type. type:%v", op.Type)
					os.Exit(0)
				}

				storeOperation := &types.StoreOperation{
					Type: types.StorePublishSuccess,
					Data: &types.StorePublishSuccessData{
						Id:     id,
						Height: height,
					},
				}
				ec.storeOperationChannel <- storeOperation
			}
		}
	}()
}
